# 深读 08 · AWS Signature V4 校验逐层精读

> S3 用 HMAC 签名认证：客户端用 SecretKey 对"规范化的请求"算签名，服务端用同样的 SecretKey
> 重算比对。SecretKey 从不上网。本篇逐字段拆 `cmd/signature-v4.go` 的 canonical request 构造、
> 签名密钥派生、`cmd/streaming-signature-v4.go` 的 chunk 链式签名，以及那些导致
> `SignatureDoesNotMatch` 的隐蔽坑。

校验主流程（`doesSignatureMatch`，`signature-v4.go:347`）就五步：
```
1. parseSignV4(Authorization)         解析出 Credential / SignedHeaders / 客户端签名
2. extractSignedHeaders               按 SignedHeaders 列表从请求里抽出被签的 header
3. checkKeyValid(accessKey)           查出该 AK 对应的 SecretKey
4. 重建 canonicalRequest → stringToSign → signingKey → newSignature
5. compareSignatureV4(newSignature, 客户端签名)   常时间比对
```
难点全在第 4 步的"逐字段精确重建"——任何一处与 AWS 规范有 1 字节出入，签名就对不上。

---

## 1. Canonical Request：六段拼接

```go
// signature-v4.go:106
// canonicalRequest =
//   <HTTPMethod>\n <CanonicalURI>\n <CanonicalQueryString>\n
//   <CanonicalHeaders>\n <SignedHeaders>\n <HashedPayload>
func getCanonicalRequest(extractedSignedHeaders http.Header, payload, queryStr, urlPath, method string) string {
    rawQuery    := strings.ReplaceAll(queryStr, "+", "%20")   // ★ 查询串 + → %20
    encodedPath := s3utils.EncodePath(urlPath)                // ★ S3 风格路径编码
    return strings.Join([]string{
        method, encodedPath, rawQuery,
        getCanonicalHeaders(extractedSignedHeaders),
        getSignedHeaders(extractedSignedHeaders),
        payload,
    }, "\n")
}
```
`★ 逐字段的坑 ────────────────────────────────`
- **`EncodePath`**：S3 的路径编码规则和标准 URL 编码**不完全一样**（比如 `/` 不编码，但其它
  保留字符要编码）。对象名含空格、中文、`+`、`~` 等字符时，客户端和服务端必须用**完全相同**的
  编码规则，否则 canonical URI 不一致 → 签名挂。这是 `SignatureDoesNotMatch` 最常见的根因之一。
- **查询串 `+` → `%20`**：表单编码里空格是 `+`，但 canonical 要求 `%20`。漏了这一步，带空格的
  query 参数就签错。
`──────────────────────────────────────────`

### Canonical Headers：小写、排序、去空白、合并多值
```go
// :61
func getCanonicalHeaders(signedHeaders http.Header) string {
    // 1. key 转小写，收集
    // 2. sort.Strings 排序
    // 3. 对每个 key：  key:value1,value2\n
    //    value 经过 signV4TrimAll（去首尾空白、压缩内部连续空白为单个）
}
// :87
func getSignedHeaders(signedHeaders) string {
    // 小写 + 排序 + 分号连接：  "host;x-amz-content-sha256;x-amz-date"
}
```
`★ 细节 ────────────────────────────────────`
- **header 名小写 + 字典序排序**：HTTP header 名大小写不敏感、顺序无关，但签名要求**规范化**成
  小写+排序，双方才能算出一样的字符串。
- **`signV4TrimAll` 压缩 value 内部空白**：`"a   b"` → `"a b"`。因为 HTTP 传输可能改变连续空白，
  规范化掉以保证一致。
- **`SignedHeaders` 列表决定哪些 header 进签名**：客户端在 Authorization 里声明 `SignedHeaders=
  host;x-amz-date;...`，服务端**只签这些**。如果客户端签了某 header 但中间代理改了它的值，
  签名就挂——所以代理/LB 改写被签 header（如 Host）是经典踩坑点。
`──────────────────────────────────────────`

---

## 2. String To Sign 与 Scope

```go
// :131
func getStringToSign(canonicalRequest string, t time.Time, scope string) string {
    return signV4Algorithm + "\n" +                          // "AWS4-HMAC-SHA256"
           t.Format(iso8601Format) + "\n" +                  // 20060102T150405Z
           scope + "\n" +
           hex(sha256(canonicalRequest))                     // ★ 对 canonical request 取 SHA256
}
// :120  scope = 日期 / region / 服务 / aws4_request
func getScope(t time.Time, region string) string {
    return t.Format("20060102") + "/" + region + "/" + "s3" + "/" + "aws4_request"
}
```
- **`stringToSign` 不含原始请求，只含 canonical request 的 SHA256**——一层哈希压缩，让待签字符串
  定长。
- **`scope` 把签名绑定到 日期+region+服务**：跨天、跨 region、跨服务的签名互不通用。这就是为什么
  时钟偏差大、region 配错都会导致签名失败。

---

## 3. 签名密钥派生：逐级 HMAC

```go
// :141
func getSigningKey(secretKey string, t time.Time, region string, stype serviceType) []byte {
    date    := sumHMAC([]byte("AWS4"+secretKey), []byte(t.Format("20060102")))
    region_ := sumHMAC(date, []byte(region))
    service := sumHMAC(region_, []byte(stype))           // "s3"
    return    sumHMAC(service, []byte("aws4_request"))
}
// :150
func getSignature(signingKey []byte, stringToSign string) string {
    return hex(sumHMAC(signingKey, []byte(stringToSign)))
}
```
`★ 为什么是"逐级 HMAC"而不是直接 HMAC(secret, msg) ─`
- 派生链 `AWS4+secret → date → region → s3 → aws4_request` 产生一把**作用域受限的签名密钥**：
  这把 `signingKey` 只对"这一天、这个 region、s3 服务"有效。即使它泄漏，攻击者也只能伪造
  当天、该 region 的请求，**SecretKey 本身没暴露**。这是 AWS 设计的密钥隔离——presigned URL
  也是用这把派生密钥签的，所以分享 presigned URL 不会泄漏 SecretKey。
`──────────────────────────────────────────`

---

## 4. 常时间比对：防计时侧信道

```go
// :166
func compareSignatureV4(sig1, sig2 string) bool {
    return subtle.ConstantTimeCompare([]byte(sig1), []byte(sig2)) == 1
}
```
`★ 这是密码学工程的硬性要求 ─────────────────────`
- **绝不能用 `==` 或 `bytes.Equal`**。普通比较在第一个不同字节就返回，攻击者能通过测量"服务端
  多快拒绝"逐字节爆破出正确签名（timing attack）。`subtle.ConstantTimeCompare` 无论哪里不同
  都跑满全长，耗时与内容无关。
- 注释还点出一个细节：因为 hex 编码是字节序列的唯一表示，对 hex 字符串做常时间比较等价于对原始
  字节比较——所以这里直接比 hex 字符串是安全的。
- **改这段代码时，任何"优化成普通比较"的念头都是引入漏洞。**
`──────────────────────────────────────────`

---

## 5. Streaming 签名：chunk 链式签名

上传大文件时 body 是流，无法预先算整体 SHA256。Streaming 签名把 body 切成 chunk，**每个 chunk
的签名链式依赖上一个 chunk 的签名**。

```go
// streaming-signature-v4.go:54
func (cr *s3ChunkedReader) getChunkSignature() string {
    hashedChunk := hex(cr.chunkSHA256Writer.Sum(nil))        // 本 chunk 数据的 SHA256
    stringToSign := "AWS4-HMAC-SHA256-PAYLOAD\n" +
        cr.seedDate.Format(iso8601Format) + "\n" +
        getScope(cr.seedDate, cr.region) + "\n" +
        cr.seedSignature + "\n" +                            // ★ 上一个 chunk（或 seed）的签名
        emptySHA256 + "\n" +                                 // 本 chunk 无额外 header → 空串哈希
        hashedChunk
    signingKey := getSigningKey(cr.cred.SecretKey, cr.seedDate, cr.region, serviceS3)
    return getSignature(signingKey, stringToSign)
}
```
- **seed 签名**（`calculateSeedSignature`，`:101`）：先用普通 V4 流程算出"第 0 个签名"。
  要求 `x-amz-content-sha256` 必须是 `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`（`:121` 校验）。
- **链式**：`cr.seedSignature` 每验完一个 chunk 就更新为该 chunk 的签名，下一个 chunk 的
  stringToSign 又引用它。形成 `seed → chunk1 → chunk2 → ...` 的**签名链**。
- **trailer chunk**（`getTrailerChunkSignature`，`:76`）：支持把校验和（如 CRC32）放在 body 末尾
  的 trailer 里，用单独的算法 `...PAYLOAD-TRAILER` 签。

`★ 链式签名的意义 ─────────────────────────────`
- **每个 chunk 的签名都"锚定"前一个 chunk 的签名**，任何 chunk 被篡改/重排/丢失，后续所有签名
  都对不上——整条流是**防篡改、防重放的链**。这让"边接收边验证"成为可能：服务端收一个 chunk
  验一个，不必缓存整个 body 再验，内存可控（呼应深读 01 的"流式不落临时大文件"）。
- **`emptySHA256` 占位**：chunk 本身没有额外 header，那个位置放空字符串的 SHA256（固定常量）。
`──────────────────────────────────────────`

---

## 6. Presigned URL 与时钟偏差

`doesPresignedSignatureMatch`（`:211`）的差异：
- 签名放在 **query string**（`X-Amz-Signature`），不在 Authorization header。
- 带 **`X-Amz-Expires`** 过期时间，服务端检查 `now - date > expires` → 过期拒绝。
- `checkMetaHeaders`（`:233`）：校验 presigned 里声明的 meta header 与实际请求一致。
- **时钟偏差容忍**（`:238`）：签名时间与服务端时间差在 `globalMaxSkewTime` 内仍接受——因为客户端
  和服务端时钟不可能完全同步。差太多则 `RequestTimeTooSkewed`。

`★ 易错点 ────────────────────────────────────`
- **presigned URL 的 query 参数顺序/编码也进签名**。生成 presigned URL 后再往 URL 上加参数、
  或改变参数顺序，签名就失效。
- **时钟偏差**是 presigned/普通签名共同的隐蔽坑：服务器或客户端时钟漂移超过 `globalMaxSkewTime`
  会导致大面积签名失败——排查"突然所有请求 403"时，先看 NTP 是否同步。
`──────────────────────────────────────────`

---

## 7. 几种 payload 哈希模式

canonical request 的最后一段 `HashedPayload` 有几种取值：
- **普通签名**：`hex(sha256(body))`——客户端先把整个 body 哈希。
- **`UNSIGNED-PAYLOAD`**：声明"body 不参与签名"（用于不便预读 body 的场景，靠 TLS 保完整性）。
- **`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`**：streaming 模式（§5），body 逐 chunk 签。
- 服务端从 `x-amz-content-sha256` header 读到这个值，决定走哪条校验路径
  （`getRequestAuthType`，深读概览第 5 篇的分流）。

---

## 8. 一页纸总结签名校验的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | canonical request 六段拼接 | 双方必须逐字节一致才能算出同样签名 |
| 2 | `EncodePath` S3 专用路径编码 | 特殊字符对象名签名失败的头号根因 |
| 3 | 查询串 `+`→`%20` | 带空格 query 的常见坑 |
| 4 | header 小写+排序+`signV4TrimAll` 压空白 | 规范化消除大小写/顺序/空白差异 |
| 5 | SignedHeaders 决定签哪些 header | 代理改写被签 header（如 Host）→ 失败 |
| 6 | stringToSign 含 canonical 的 SHA256 + scope | 定长待签串；签名绑定 日期/region/服务 |
| 7 | signingKey 逐级 HMAC 派生 | 作用域受限密钥，泄漏不暴露 SecretKey |
| 8 | `subtle.ConstantTimeCompare` | 防计时侧信道爆破签名，绝不能换普通比较 |
| 9 | streaming chunk 链式签名 | 防篡改/重放的流，边收边验内存可控 |
| 10 | trailer chunk 单独算法 | 校验和放 body 末尾 |
| 11 | presigned 签名在 query + Expires 过期 | 改 URL 参数即失效 |
| 12 | 时钟偏差容忍 `globalMaxSkewTime` | NTP 不同步会大面积 403 |
| 13 | payload 三种模式（sha256/UNSIGNED/STREAMING） | 决定 body 是否及如何参与签名 |

下一篇深读：**IAM 存储与策略评估**——内存缓存结构、加载与 watch/reload、singleflight 防击穿、
STS/服务账号的会话策略交集、以及策略评估的 Action/Resource/Condition 匹配。
