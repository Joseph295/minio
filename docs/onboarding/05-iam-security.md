# 05 · IAM 与安全：签名、授权、加密

本篇覆盖三件事：**你是谁**（Signature V4 验签）、**你能做什么**（IAM/策略授权）、
**数据怎么保密**（SSE 加密 + KMS）。这三者构成 MinIO 的安全闭环。

## 5.1 鉴权总流程：从分流到放行

第 2 篇说过，中间件只做"鉴权类型分流"，真正的验签 + 授权在 handler 内部。入口函数族在
`cmd/auth-handler.go`：

```
请求 → getRequestAuthType()          auth-handler.go:124   判断 V2/V4/Presigned/JWT/STS/匿名
     → checkRequestAuthType()         :338                 高层编排
        → authenticateRequest()       :357   "你是谁"——验签
        → authorizeRequest()          :418   "你能做什么"——查策略
```

```
                       ┌─────────────────────────────┐
   incoming request →  │ getRequestAuthType (分流)    │
                       └──────────────┬──────────────┘
            ┌──────────────┬──────────┴──────┬───────────────┐
            ▼              ▼                  ▼               ▼
       V4 Signed     Presigned URL     Streaming Signed   JWT/STS
       doesSignature doesPresigned     calculateSeed      getClaimsFromToken
       Match         SignatureMatch    Signature
            └──────────────┴──────────┬───────┴───────────────┘
                                      ▼
                       ┌─────────────────────────────┐
                       │ authorizeRequest (查策略)    │
                       │  匿名→bucket policy          │
                       │  认证→IAM policy (∪ bucket)  │
                       └─────────────────────────────┘
```

## 5.2 "你是谁"：AWS Signature V4

S3 不传明文密码，而是用 **HMAC 签名**：客户端用 SecretKey 对"规范化的请求"算签名，服务端用
同样的 SecretKey 重算，比对一致即认证通过。SecretKey 从不在网络上出现。

### 验签主流程 `doesSignatureMatch`（`cmd/signature-v4.go:347-408`）
```
1. 解析 Authorization 头 → credential / signedHeaders / clientSignature
2. 提取被签名的 header 子集 + 时间戳
3. 重建 canonicalRequest（method + path + query + headers + payloadHash）   :389
4. stringToSign = 算法 + 时间 + scope + hash(canonicalRequest)            :392
5. signingKey = 派生密钥（见下）                                          :395
6. serverSignature = HMAC(signingKey, stringToSign)                       :399
7. subtle.ConstantTimeCompare(serverSignature, clientSignature)           :402
```

### 派生签名密钥 `getSigningKey`（`:141-147`）——逐级 HMAC
```
k1 = HMAC("AWS4"+SecretKey, date)
k2 = HMAC(k1, region)
k3 = HMAC(k2, "s3")
signingKey = HMAC(k3, "aws4_request")
```

`★ 易错点/洞察 ─────────────────────────────`
- **必须用 `subtle.ConstantTimeCompare`** 比对签名，而不是 `==` 或 `bytes.Equal`。普通比较会
  在第一个不同字节就返回，攻击者能用计时差逐字节爆破签名。这是密码学工程的硬性要求，改这段
  代码时绝不能"优化"成普通比较。
- **签名覆盖的内容很微妙**：哪些 header 被签、path 是否编码、query 排序——任何一处规范化不一致
  都会导致 `SignatureDoesNotMatch`。客户端报这个错时，90% 是 canonical request 构造与 AWS 规范
  有出入（比如 path 编码、含特殊字符的对象名）。
`──────────────────────────────────────────`

### 其它签名变体
- **Presigned URL**（`doesPresignedSignatureMatch` `:211-341`）：签名放在 query 里，带过期时间
  `X-Amz-Expires`，服务端检查 `now - date > expires` 即过期。用于"给一个临时下载/上传链接"。
- **Streaming Signature**（`cmd/streaming-signature-v4.go`）：上传大文件时分块签名，先算
  `calculateSeedSignature`（`:101`），每个 chunk 的签名链式依赖上一个 chunk 的签名
  （`getChunkSignature` `:54`）。这样能边传边校验，不必先缓存整个 body。
- **STS 临时凭证**：带 `SessionToken`（一个 JWT），`getClaimsFromTokenWithSecret`（`:219`）
  验证并解出其中的会话策略。

## 5.3 "你能做什么"：IAM 体系

### 两层结构
- **`IAMSys`**（`cmd/iam.go:85`）：内存中的 IAM 门面，handler 调它的 `IsAllowed`。
- **`IAMStoreSys`**（`cmd/iam-store.go:740`）：持久化 + 缓存层，底层是 `IAMStorageAPI` 接口，
  两个实现：
  - `IAMObjectStore`（存在对象存储里，`.minio.sys/config/iam/...`）
  - `IAMEtcdStore`（存在 etcd 里，多用于跨集群共享身份）

### 数据存哪（`cmd/iam-store.go:46-71`）
所有身份数据其实是**存在 MinIO 自己的系统桶 `.minio.sys` 里的 JSON 文件**：
```
config/iam/users/{ak}/identity.json                用户
config/iam/service-accounts/{ak}/identity.json     服务账号
config/iam/groups/{name}/members.json              组
config/iam/policies/{name}/policy.json             策略文档
config/iam/sts/{ak}/identity.json                  STS 临时凭证
config/iam/policydb/users/{user}.json              用户→策略映射
config/iam/policydb/groups/{group}.json            组→策略映射
```

`★ 设计洞察："自举"存储 ───────────────────────`
- **IAM 把自己的数据存在 MinIO 自己里**（系统桶 `.minio.sys`）。这意味着身份系统天然继承了
  纠删码的可靠性、复制能力——你不需要为 IAM 单独搭数据库。代价是有"先有鸡还是先有蛋"的启动
  顺序约束（对象层必须先就绪，IAM 才能加载，见第 2 篇启动顺序步骤 9→11）。
`──────────────────────────────────────────`

### 加载与缓存
- 启动时全量加载进内存缓存（`iamUsersMap`、`iamPoliciesMap`、`iamUserPolicyMap` 等）。
- **变更通知**：某节点改了 IAM，通过通知机制让所有节点 reload 对应条目
  （`UserNotificationHandler` / `PolicyNotificationHandler` / `GroupNotificationHandler`，
  `cmd/iam-store.go:128-171`）。
- 用 `singleflight`（`cmd/iam-store.go:740` 的 `policy`/`group` group）**合并并发的相同加载请求**，
  避免缓存击穿时 N 个请求同时打到存储。

### 授权评估 `IsAllowed`（`cmd/iam.go:2518-2564`）
```
if OPA/外部 AuthZ 插件配置了:  交给插件
if 是 owner(root):            直接放行
if 临时凭证(STS):             IsAllowedSTS()      （会话策略 ∩ 父用户策略）
if 服务账号:                  IsAllowedServiceAccount()
else 普通用户:
    policies = PolicyDBGet(account, groups...)         查用户+组绑定的策略
    return GetCombinedPolicy(policies).IsAllowed(args) 合并后评估
```
- 策略对象来自外部库 `github.com/minio/pkg/v3/policy`，评估 `Action × Resource × Condition`。
- 条件值（`getConditionValues`，`cmd/bucket-policy.go`）包括 SourceIp、SecureTransport、
  CurrentTime、principaltype、versionid 等——这就是为什么策略能写"只允许某 IP 段在 TLS 下访问"。

### Bucket Policy vs IAM Policy
| | IAM Policy | Bucket Policy |
|---|---|---|
| 绑定对象 | 用户/组/服务账号 | 某个 bucket |
| 存储 | `config/iam/policies/...` | bucket 元数据 |
| 匿名访问 | 不适用 | **唯一**能授权匿名访问的途径 |
| 合并逻辑 | 见 `authorizeRequest` | 二者**任一允许即放行**（OR） |

`★ 易错点 ─────────────────────────────────`
- **匿名请求只看 bucket policy**（`authorizeRequest` `:431-464`）。所以"为什么我设了 IAM 策略
  匿名还是访问不了"——因为匿名根本不走 IAM，只认 bucket policy。反之"为什么 bucket 设成 public
  后谁都能访问"也是这个原因。
- **STS/服务账号的会话策略是"交集"语义**：会话策略只能**收窄**父用户的权限，不能扩权。临时凭证
  即使带了一个超大权限的 session policy，也越不过父用户的边界。
`──────────────────────────────────────────`

## 5.4 "数据怎么保密"：SSE 服务端加密

MinIO 支持三种服务端加密，统一抽象在 `internal/crypto/sse.go:44`（`Type` 接口）：

| 模式 | 密钥从哪来 | 谁管密钥 |
|------|-----------|---------|
| **SSE-C** | 客户端**每个请求**带 32 字节密钥 | 客户端自己（服务端不存密钥） |
| **SSE-S3** | 服务端用默认 KMS 主密钥生成 | 服务端/KMS |
| **SSE-KMS** | 客户端指定 KMS 主密钥 ID | KMS（如 KES） |

### 信封加密（envelope encryption）——三种模式的共同骨架
MinIO **不**用主密钥直接加密对象。而是：
```
1. 每个对象生成一把随机"对象密钥"(object key)
2. 用对象密钥（经 DARE 算法）加密对象数据         ← 真正加密 TB 级数据用的是它
3. 用"主密钥/客户端密钥"把对象密钥本身加密（Seal） ← 只加密 32 字节的小钥匙
4. 把"封好的对象密钥"存进 xl.meta 元数据
```
- 对象密钥派生：`internal/crypto/key.go:40` `GenerateKey`（HMAC 从外部密钥 + nonce 派生）。
- 封装：`ObjectKey.Seal`（`key.go:87`），用 `iv|domain|"DAREv2-HMAC-SHA256"|bucket/object` 当
  上下文做 HMAC，再 `sio.Encrypt`。
- 数据加密：`EncryptSinglePart`/`EncryptMultiPart`（`sse.go:103-117`），用 **DARE v2**
  （`sio` 库，FIPS 模式用 `fips.DARECiphers()`）。多 part 时每个 part 用
  `partKey = HMAC(objectKey, partID)` 派生独立子密钥。

`★ 设计洞察：为什么信封加密？─────────────────`
- **主密钥永不接触大数据，也永不离开 KMS**。加密 1TB 对象用的是临时对象密钥；KMS 只负责
  加解密那把 32 字节小钥匙。这让密钥轮换（rotate 主密钥）无需重新加密对象数据，只需重新封装
  对象密钥；也让 KMS 不会成为数据吞吐瓶颈。
- **上下文绑定（bucket/object 进 HMAC）**：封装对象密钥时把 bucket/object 名混进 HMAC 上下文，
  使得"把 A 对象的封装密钥挪到 B 对象"无法解开——防止密文搬移攻击。
`──────────────────────────────────────────`

### 加密元数据存哪
封好的对象密钥、IV、算法、KMS keyID、KMS 加密的数据密钥等，都以
`X-Minio-Internal-Server-Side-Encryption-*` 系统元数据存进 `xl.meta` 的 `MetaSys`
（`internal/crypto/metadata.go`、`sse-kms.go:139`）。读对象时反向解封。

`★ 易错点 ─────────────────────────────────`
- **SSE-C 服务端不存密钥**。客户端弄丢那 32 字节密钥 = 数据永久无法解密。这是 SSE-C 的设计
  本意（服务端零知识），但运维上是高危点。
- **SSE-C 的密钥在请求头里传输**，因此**必须走 TLS**，否则密钥明文过网。MinIO 会拒绝非 TLS 的
  SSE-C 请求。
`──────────────────────────────────────────`

## 5.5 KMS 与 KES 集成

- 抽象：`internal/kms/kms.go:139`（`KMS` 结构，类型有 Builtin / MinKMS / MinKES）。
- 两个核心操作：
  - `GenerateKey`（`:228`）：请 KMS 用某主密钥生成一个 DEK（数据加密密钥，返回明文 + 密文两份）。
  - `Decrypt`（`:242`）：把 DEK 密文解回明文（需带与生成时相同的 `AssociatedData` 上下文）。
- KES 实现（`internal/kms/kes.go`）：通过 `github.com/minio/kms-go/kes` 客户端与 KES 服务通信。
- 默认内置（Builtin）KMS 用单一主密钥，仅适合测试/小规模；生产用 KES 对接 Vault 等后端。

`★ 设计洞察 ─────────────────────────────────`
- **`AssociatedData`（AAD）是 KMS 调用的安全绑定**。GenerateKey 和 Decrypt 必须传相同的
  AAD（通常含 bucket/object/context），否则解不开。这把"这把 DEK 属于谁"钉死，防止 DEK 被
  跨上下文滥用。
`──────────────────────────────────────────`

## 5.6 凭证与 JWT

- `Credentials`（`internal/auth/credentials.go:112`）：AccessKey/SecretKey/SessionToken/
  Expiration/ParentUser/Groups/Claims。
  - `IsTemp()`：有 SessionToken 且有过期时间 → STS 临时凭证。
  - `IsServiceAccount()`：有 ParentUser 且 Claims 里有 `sa-policy`。
- JWT 用于：节点间互信 token（`cmd/jwt.go`，HS512 签名，节点间 token 有效期设得极长）、
  STS 会话 token、Console/Web token。

## 5.7 本篇要点回顾

- **验签**：V4 用逐级 HMAC 派生密钥重算签名，`ConstantTimeCompare` 防计时攻击；变体有
  presigned / streaming / STS。
- **授权**：IAM 数据自举存在 `.minio.sys`，内存缓存 + 通知 reload + singleflight 防击穿；
  评估走 owner→STS→SA→普通用户的分支；匿名只看 bucket policy。
- **加密**：三种 SSE 模式共用**信封加密**，主密钥只封装对象密钥不碰大数据；上下文绑定防搬移；
  SSE-C 零知识但丢钥即丢数据。
- **KMS**：GenerateKey/Decrypt + AAD 绑定，生产用 KES。

下一篇看后台数据服务：复制、生命周期、扫描、自愈，以及它们如何协同。
