# 深读 10 · SSE 加密与 KMS 逐层精读

> "数据怎么保密"由 SSE（服务端加密）+ KMS 实现。本篇拆开**信封加密**的对象密钥派生与封装、
> 上下文绑定、DARE 分包加密、多 part 子密钥、三种 SSE 模式的元数据差异、以及 KES 集成与 AAD
> （`internal/crypto/key.go`、`sse*.go`、`internal/kms`）。

---

## 1. 信封加密：主密钥永不碰大数据

MinIO **不用主密钥直接加密对象**。流程是"信封加密"：
```
1. 每个对象生成一把随机 ObjectKey（256 bit）
2. 用 ObjectKey（经 DARE）加密对象数据          ← 加密 TB 级数据用的是它
3. 用"主密钥/客户端密钥"把 ObjectKey 本身封装(Seal) ← 只加密 32 字节小钥匙
4. 把封好的 ObjectKey 存进 xl.meta
```

```go
// key.go:36
type ObjectKey [32]byte   // 256-bit，注释："It must never be stored in plaintext"
// :40
func GenerateKey(extKey []byte, random io.Reader) (key ObjectKey) {
    var nonce [32]byte; io.ReadFull(random, nonce[:])        // 随机 nonce
    const Context = "object-encryption-key generation"
    mac := hmac.New(sha256.New, extKey)                      // ★ HMAC(extKey, Context || nonce)
    mac.Write([]byte(Context)); mac.Write(nonce[:])
    mac.Sum(key[:0])
    return key
}
```
`★ 为什么信封加密 ─────────────────────────────`
- **主密钥只封装 32 字节的 ObjectKey，从不接触对象数据**。加密 1TB 对象用的是临时 ObjectKey。
  好处：① 主密钥轮换（rotate）无需重新加密对象数据，只需重新封装 ObjectKey；② KMS 不会成为
  数据吞吐瓶颈（只算小钥匙）。
- **`GenerateKey` 用 HMAC + nonce 派生**：即使同一个 extKey，每次加 nonce 都产生不同的 ObjectKey。
  `errOutOfEntropy`：随机源枯竭直接 critical——加密绝不能用弱随机。
`──────────────────────────────────────────`

---

## 2. `Seal` / `Unseal`：把 ObjectKey 绑定到 bucket/object

```go
// key.go:77
type SealedKey struct {
    Key       [64]byte  // 加密+认证后的 object key
    IV        [32]byte  // 封装用的随机 IV
    Algorithm string
}
// :87
func (key ObjectKey) Seal(extKey []byte, iv [32]byte, domain, bucket, object string) SealedKey {
    var sealingKey [32]byte
    mac := hmac.New(sha256.New, extKey)
    mac.Write(iv[:])
    mac.Write([]byte(domain))                         // "SSE-C" / "SSE-S3" / "SSE-KMS"
    mac.Write([]byte(SealAlgorithm))
    mac.Write([]byte(path.Join(bucket, object)))      // ★ 把 bucket/object 混进封装密钥
    mac.Sum(sealingKey[:0])
    sio.Encrypt(&encryptedKey, bytes.NewReader(key[:]), sio.Config{Key: sealingKey[:], CipherSuites: fips.DARECiphers()})
    // → SealedKey{Key: encrypted, IV, Algorithm}
}
// :115 Unseal：同样的派生，密钥错则 ErrSecretKeyMismatch
func (key *ObjectKey) Unseal(extKey []byte, sealedKey SealedKey, domain, bucket, object string) error {
    ...
    if out, err := sio.DecryptBuffer(...); len(out) != 32 || err != nil { return ErrSecretKeyMismatch }
}
```
`★ 上下文绑定 = 防密文搬移攻击 ───────────────────`
- **封装密钥 `sealingKey` 由 `iv | domain | algorithm | bucket/object` 一起 HMAC 派生**。这意味着
  A 对象的 SealedKey 用的封装密钥里"绑死了 A 的 bucket/object"。**把 A 的 SealedKey 搬到 B 对象
  的元数据里，Unseal 时因为 bucket/object 不同，派生出的封装密钥不同 → 解不开**（`ErrSecretKeyMismatch`）。
  这防止攻击者通过移动密文/封装密钥来解密别的对象。
- **`domain` 区分 SSE 模式**：同一把 extKey 在 SSE-C 和 SSE-S3 下派生的封装密钥不同，模式间隔离。
- **`ErrSecretKeyMismatch` 就是 SSE-C 密钥错的检测点**：客户端给错密钥，Unseal 失败返回它——
  这也是"SSE-C 丢密钥=数据永久无法读"的代码出处（服务端无从恢复 extKey）。
`──────────────────────────────────────────`

---

## 3. 多 part 子密钥：`DerivePartKey`

```go
// key.go:140
func (key ObjectKey) DerivePartKey(id uint32) (partKey [32]byte) {
    var bin [4]byte; binary.LittleEndian.PutUint32(bin[:], id)
    mac := hmac.New(sha256.New, key[:])    // ★ HMAC(ObjectKey, partID)
    mac.Write(bin[:]); mac.Sum(partKey[:0])
    return partKey
}
```
- 多段上传时，**每个 part 用从 ObjectKey 派生的独立子密钥加密**（`HMAC(objectKey, partID)`）。
  各 part 密钥不同，单个 part 密钥泄漏不波及其它 part；且 part 顺序绑定（partID 进派生）。

---

## 4. ETag 也加密：`SealETag`

```go
// key.go:155
func (key ObjectKey) SealETag(etag []byte) []byte {
    if len(etag) == 0 { return etag }   // ★ 空 etag 不加密
    mac := hmac.New(sha256.New, key[:]); mac.Write([]byte("SSE-etag"))
    sio.Encrypt(&buffer, bytes.NewReader(etag), sio.Config{Key: mac.Sum(nil), ...})
}
```
`★ 细节 ────────────────────────────────────`
- **加密对象的 ETag 本身也被加密**（用从 ObjectKey 派生、"SSE-etag" 上下文的密钥）。因为对未加密
  对象 ETag = MD5(明文)，若加密对象暴露明文 MD5，等于泄漏内容指纹。所以加密对象的 ETag 也藏起来。
- **空 ETag 不加密**（注释 `:153`）：空 ETag 表示"客户端没发 MD5，后端可自选 ETag 值"，没有要
  保护的内容指纹。
`──────────────────────────────────────────`

---

## 5. 对象数据加密：DARE

数据加密用 **DARE（Data At Rest Encryption）** 算法（`github.com/minio/sio` 库）：
```go
// sse.go（EncryptSinglePart / EncryptMultiPart）
sio.EncryptReader(r, sio.Config{
    MinVersion:   sio.Version20,        // DARE 2.0
    Key:          objectKey[:],
    CipherSuites: fips.DARECiphers(),   // FIPS 模式用合规密码套件
})
// 多 part：partKey := objectKey.DerivePartKey(partID); 用 partKey 加密这个 part
```
`★ DARE 的特性 ────────────────────────────────`
- **DARE 把数据切成固定大小的包（package，~64 KiB），每包独立加密 + 认证**（AEAD）。每包带
  认证标签，**篡改任何一包都会在解密时被检出**——抗篡改。
- **分包让随机读成为可能**：要读对象中间某段，只需解密涉及的那几个包，不必从头解。配合 Range
  请求（深读 02）很自然。
- **加密改变长度**：密文比明文长（每包有 nonce+tag 开销）。所以写路径要用 `sio.DecryptedSize`
  从密文长度反推明文长度（深读 01 §7 的 `actualSize`）。
- **FIPS 模式**：`fips.DARECiphers()` 在 FIPS 构建下只用合规密码套件（AES-GCM），满足合规要求。
`──────────────────────────────────────────`

---

## 6. 三种 SSE 模式的元数据差异

| 模式 | extKey（封装用的"主密钥"）来源 | 元数据存什么 |
|------|------------------------------|-------------|
| **SSE-C** | 客户端每个请求带的 32 字节密钥 | 只存 SealedKey（封装后的 ObjectKey）+ IV + 算法；**不存 extKey** |
| **SSE-S3** | 服务端默认 KMS 主密钥生成的 DEK | SealedKey + KMS 加密的 DEK 密文 + key-id |
| **SSE-KMS** | 客户端指定的 KMS 主密钥生成的 DEK | 同 SSE-S3 + KMS context |

- 加密元数据都以 `X-Minio-Internal-Server-Side-Encryption-*` 存进 `xl.meta` 的 `MetaSys`
  （深读 03 §5：系统元数据，值是 `[]byte`）。
- **SSE-C 的关键差异**：服务端零知识——元数据里**没有 extKey**，只有用 extKey 封装后的 SealedKey。
  没有客户端再次提供的 extKey，谁也 Unseal 不了。这是设计本意，也是运维高危点（丢钥=丢数据）。
  且 SSE-C 密钥在请求头传输，**必须走 TLS**，否则密钥明文过网。

---

## 7. KMS 与 KES：DEK 与 AAD

SSE-S3/SSE-KMS 的 extKey 不是凭空来的，而是 KMS 生成的 **DEK（数据加密密钥）**：
```go
// internal/kms/kms.go
GenerateKey(ctx, req) → DEK{Plaintext, Ciphertext}   // 请 KMS 用某主密钥生成一把 DEK，返回明文+密文两份
Decrypt(ctx, req) → plaintext                         // 把 DEK 密文解回明文（需带相同 AssociatedData）
```
- 写时：`GenerateKey` 得到 DEK，用 DEK 的**明文**当 extKey 封装 ObjectKey，把 DEK 的**密文**存元数据，
  明文用完即丢。
- 读时：`Decrypt` 把元数据里的 DEK 密文解回明文，再用它 Unseal ObjectKey。
- KES 实现（`internal/kms/kes.go`）通过 `kms-go/kes` 客户端与 KES 服务通信；生产用 KES 对接 Vault 等。

`★ AAD（AssociatedData）上下文绑定 ─────────────`
- `GenerateKey` 和 `Decrypt` 必须传**相同的 AssociatedData**（通常含 bucket/object/KMS context），
  否则 KMS 拒绝解密。这把"这把 DEK 属于哪个对象/上下文"在 KMS 层钉死，防止 DEK 被跨上下文滥用——
  与 §2 的"封装密钥绑 bucket/object"是同一思想在 KMS 层的体现。**两层上下文绑定（KMS 的 AAD +
  Seal 的 HMAC context）共同保证密钥不能被搬移复用。**
`──────────────────────────────────────────`

---

## 8. 一页纸总结加密的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 信封加密：主密钥只封装 ObjectKey，不碰数据 | 密钥轮换不重加密；KMS 非吞吐瓶颈 |
| 2 | GenerateKey = HMAC(extKey, ctx‖nonce) | 同 extKey 每次派生不同 ObjectKey |
| 3 | Seal 把 bucket/object 混进封装密钥 | 防密文/SealedKey 跨对象搬移攻击 |
| 4 | domain 区分 SSE 模式 | 模式间密钥隔离 |
| 5 | Unseal 失败 = ErrSecretKeyMismatch | SSE-C 错密钥的检测点；丢钥不可恢复 |
| 6 | DerivePartKey = HMAC(ObjectKey, partID) | 每 part 独立子密钥，泄漏不扩散 |
| 7 | ETag 也加密（"SSE-etag"），空 ETag 不加密 | 防泄漏明文 MD5 内容指纹 |
| 8 | DARE 分包 AEAD（~64KiB/包） | 抗篡改 + 支持随机读 |
| 9 | sio.DecryptedSize 反推明文长度 | 密文比明文长，需还原 Content-Length |
| 10 | SSE-C 服务端不存 extKey，必须 TLS | 零知识；丢钥=丢数据；密钥头需加密信道 |
| 11 | KMS GenerateKey/Decrypt + DEK 信封 | extKey 来自 KMS，明文用完即丢 |
| 12 | AAD 上下文绑定（KMS 层 + Seal 层两道） | 双重防 DEK/ObjectKey 跨上下文滥用 |

下一篇深读：**Bucket 复制**——worker 池分级、复制状态机、MRF 持久化重试、resync 全量补传、
多目标并发、以及"对象先于 delete marker"的时序不变量在代码里如何保证。
