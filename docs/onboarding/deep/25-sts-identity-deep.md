# 深读 25 · STS 与身份提供方逐层精读

> STS（Security Token Service）发放**临时凭证**：短期、可收窄、绑定父身份。本篇拆开五种
> AssumeRole 的统一流程、SessionToken=自包含 JWT、外部身份提供方（OpenID/LDAP/证书/插件）的
> 身份验证、会话策略与父身份继承（`cmd/sts-handlers.go` + `internal/config/identity/`）。

---

## 1. 为什么要临时凭证

`★ STS 解决的问题 ─────────────────────────────`
- **别到处发长期密钥**：给每个应用/用户/CI 发一对永久 AccessKey/SecretKey，泄漏即灾难、轮换困难。
- **STS 发短期、受限凭证**：用你已有的身份（MinIO 用户 / 企业 SSO / LDAP / 客户端证书）换一对
  **有过期时间、权限可进一步收窄**的临时凭证。泄漏了也很快失效，且权限本就受限。
`──────────────────────────────────────────`

---

## 2. 统一流程：验身份 → 建 claims → 签 JWT → 入 IAM

五种 AssumeRole 的**身份验证方式不同，但后半段完全一致**。以 `AssumeRole`（`:260`，MinIO 内部用户）为例：
```go
// 验证内部用户身份（用户名/密钥）后：
claims.populateSessionPolicy(r.Form)              // ① 会话策略进 claims
duration, _ := openid.GetDefaultExpiration(...)   // ② 过期时长
claims[expClaim]    = UTCNow().Add(duration).Unix()
claims[parentClaim] = user.AccessKey              // ③ ★ 记录父身份
secret, _ := getTokenSigningKey()                 // ④ 取签名密钥
cred, _ := auth.GetNewCredentialsWithMetadata(claims, secret)  // ⑤ ★ 生成临时凭证（SessionToken=签名JWT）
cred.ParentUser = user.AccessKey                  // ⑥ 绑父身份
globalIAMSys.SetTempUser(ctx, cred.AccessKey, cred, "")  // ⑦ 存入 IAM
// ⑧ site-replication hook：临时凭证传播到所有 peer 站点
globalSiteReplicationSys.IAMChangeHook(ctx, SRIAMItem{Type: SRIAMItemSTSAcc, ...})
```
`★ SessionToken = 自包含 JWT ───────────────────`
- **`GetNewCredentialsWithMetadata(claims, secret)` 生成的临时凭证里，SessionToken 就是一个用 secret
  签名的 JWT**，**claims（父身份、过期时间、会话策略）全部编码在 JWT 里**。
- 所以临时凭证是**自包含**的：之后用这对临时凭证发 S3 请求时（深读 05 §5.2），服务端解码
  SessionToken（JWT）就能拿到"它的父身份是谁、会话策略是什么、何时过期"——**无需额外查表**。
- **`ParentUser` 是权限继承的纽带**：临时凭证自己没有策略，它**继承父身份的策略**（深读 09 §6：
  父策略 ∩ 会话策略）。`SetTempUser` 把它存进 IAM 的 STS 缓存（深读 09：按需加载、过期自动失效）。
- **site-replication hook**：临时凭证也广播给所有 peer 站点——这样一对临时凭证在任一站点都能用
  （多活下临时凭证跨站点有效）。
`──────────────────────────────────────────`

---

## 3. 五种 AssumeRole：身份验证各异

| 变体 | 身份来源 | 怎么验 |
|------|---------|--------|
| **`AssumeRole`** (`:260`) | MinIO 内部用户 | 验用户名/密钥，查 `PolicyDBGet` 确认用户有效 |
| **`AssumeRoleWithWebIdentity` / `WithSSO`** (`:373/613`) | OpenID/OIDC 外部 IdP | 验外部 IdP 签发的 JWT（`OpenIDConfig.Validate`），按 claim 映射策略 |
| **`AssumeRoleWithLDAPIdentity`** (`:628`) | LDAP/AD | LDAP bind 验证账密，查询用户所属组 |
| **`AssumeRoleWithCertificate`** | 客户端证书 (mTLS) | 验证客户端证书链，从证书提取身份 |
| **`AssumeRoleWithCustomToken`** | 外部认证插件 | 调用 AuthN 插件验证自定义 token |

### OpenID（SSO）验证细节
```go
// :435
globalIAMSys.OpenIDConfig.Validate(ctx, roleArn, token, accessToken, durationSeconds, claims)
```
`★ OpenID：信任外部 IdP 的签名 ─────────────────`
- 用户先在企业 IdP（Keycloak/Okta/Auth0/Google…）登录，拿到一个 IdP 签发的 **id_token（JWT）**。
  拿这个 JWT 来 AssumeRole，MinIO **验证 IdP 的签名**（用 IdP 的公钥/JWKS）确认 token 真实未篡改，
  然后按 token 里的 claim（如 `role`、`groups`）**映射到 MinIO 的策略**（通过 role ARN 或 claim
  名配置）。
- **MinIO 不存这些用户的密码**——身份验证委托给 IdP，MinIO 只验签名 + 映射策略。这是"联合身份"
  （federated identity）：企业身份系统是真相源，MinIO 信任它的签名。
`──────────────────────────────────────────`

### LDAP 验证细节
```go
// :694 LDAPConfig.GetExpiryDuration / Bind / 查询组
```
- LDAP 模式：用户提供 LDAP 账密，MinIO **bind 到 LDAP 服务器**验证，成功后**查询该用户所属的组**，
  把组映射到策略。临时凭证的 ParentUser 是 LDAP DN，组决定权限。

---

## 4. 身份提供方配置（`internal/config/identity/`）

```
internal/config/identity/
├── openid/   OpenID/OIDC：JWKS、claim 映射、role ARN
├── ldap/     LDAP/AD：server、bind DN、用户/组查询过滤器
├── tls/      证书 STS：信任的 CA
└── plugin/   外部 AuthN 插件：webhook 验证 URL
```
- 每种 IdP 是一个可插拔配置模块。MinIO 可同时配多种（OpenID + LDAP），不同 AssumeRole 端点对应
  不同 IdP。
- 这又是**接口/插件化**的体现：身份验证逻辑按 IdP 类型分模块，STS handler 调用对应模块验证，
  之后统一走"建 claims → 签 JWT"。

---

## 5. 会话策略：只能收窄

```go
// :103 populateSessionPolicy
sessionPolicyStr := form.Get(stsPolicy)
sessionPolicy, _ := policy.ParseConfig(...)
policyBuf, _ := json.Marshal(sessionPolicy)
// 存进 claims，签进 JWT
```
- AssumeRole 时可附带一个**会话策略**（inline policy）。它**只能收窄**父身份的权限，不能扩权
  （深读 09 §6：最终权限 = 父策略 ∩ 会话策略）。
- 用途：给一个临时任务发凭证时，进一步限制它"只能读某个 bucket 的某个前缀"，即使父用户权限更大。
  **最小权限原则**的实现。

---

## 6. 过期与签名密钥

- **`DurationSeconds`**：临时凭证有效期（有上下限），写入 JWT 的 `exp` claim。到期后凭证自动失效
  （深读 09：STS 按需加载、过期清理）。
- **`getTokenSigningKey`（`:245`）**：签 JWT 的密钥。临时凭证的 SessionToken 用它签名，服务端用同样
  的密钥验签。改这个密钥会让所有现存临时凭证失效。

---

## 7. 一页纸总结 STS 的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | STS 发短期、受限凭证 | 不到处发长期密钥，泄漏快失效 |
| 2 | 五种 AssumeRole 验身份各异、后半段统一 | 一套"建 claims→签 JWT→入 IAM" |
| 3 | SessionToken = 自包含签名 JWT | 父身份/会话策略/过期编码在内，无需查表 |
| 4 | ParentUser 继承父策略 | 临时凭证自己无策略，继承父的 |
| 5 | OpenID 委托外部 IdP 验签 | 联合身份，MinIO 不存密码 |
| 6 | LDAP bind + 组查询映射策略 | 企业目录身份接入 |
| 7 | IdP 可插拔（openid/ldap/tls/plugin） | 接口化身份验证 |
| 8 | 会话策略只收窄不扩权 | 最小权限 |
| 9 | DurationSeconds → exp，自动失效 | 临时性 |
| 10 | site-replication hook 传播临时凭证 | 多活下临时凭证跨站点有效 |

下一篇深读：**配置子系统**——server config 的存储、加密、版本迁移、KMS 后端、热加载。
