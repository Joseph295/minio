# 深读 22 · 多段上传（Multipart Upload）逐层精读

> 大对象（GB/TB）通过多段上传：切成多个 part 并行上传、可断点续传、最后合并。本篇拆开 upload ID
> 与目录布局、每 part 独立纠删码、Complete 的 ETag/校验和/最小 part 校验与合并提交、过期 upload
> 清理（`cmd/erasure-multipart.go`）。

---

## 1. 三段式 API 与目录布局

```
NewMultipartUpload  → 返回 uploadID
PutObjectPart × N   → 各 part 独立上传（可并行、可重试）
CompleteMultipartUpload → 校验 + 合并成最终对象
（AbortMultipartUpload  → 放弃，清理）
```

### Upload ID 与存储位置
```go
// :47 getUploadIDDir
func (er erasureObjects) getUploadIDDir(bucket, object, uploadID string) string {
    uploadUUID := base64解码(uploadID) 取 "prefix.uuid" 的 uuid 部分
    return pathJoin(getMultipartSHADir(bucket, object), uploadUUID)
}
// :58 getMultipartSHADir
func (er erasureObjects) getMultipartSHADir(bucket, object string) string {
    return getSHA256Hash([]byte(pathJoin(bucket, object)))   // ★ 对象名的 SHA256
}
```
`★ 目录布局的讲究 ─────────────────────────────`
- **进行中的 multipart 数据存在 `.minio.sys/multipart`（`minioMetaMultipartBucket`），不在对象正式
  位置**。直到 Complete 才搬到正式位置。这让"上传一半"的对象对客户端不可见——半成品不污染正式空间。
- **用对象名的 SHA256 当目录**（而非对象名本身）：对象名可能很长、含特殊字符、层级很深。SHA256
  把它压成定长、文件系统友好的目录名，避免超长路径和特殊字符问题。
- **uploadID = base64(prefix.uuid)**：内嵌一个前缀（含 pool/set 路由信息）+ UUID。解码取 UUID 当
  目录。前缀让 Complete/Abort 能路由回正确的 set。
`──────────────────────────────────────────`

---

## 2. `PutObjectPart`：每个 part 是一次"迷你 PutObject"

```go
// :570（精简）
fi, _, _ := er.checkUploadIDExists(...)             // ★ 校验 uploadID 存在且有效
writeQuorum := fi.WriteQuorum(er.defaultWQuorum())
if 校验和类型不匹配 { return InvalidArgument }       // 校验和类型必须与 NewMultipartUpload 时一致
onlineDisks = shuffleDisks(onlineDisks, fi.Erasure.Distribution)  // ★ 复用 upload 的分布

partSuffix  := fmt.Sprintf("part.%d", partID)
tmpPart     := mustGetUUID() + "x" + timestamp      // ★ 唯一临时名（并发 part 安全）
tmpPartPath := pathJoin(tmpPart, partSuffix)
// 为每盘建 bitrotWriter，erasure.Encode 并行写（和深读 01 的写路径几乎一样）
n, _ := erasure.Encode(ctx, toEncode, writers, buffer, writeQuorum)
// rename 临时 part → uploadIDPath/DataDir/part.N
```
`★ 细节 ────────────────────────────────────`
- **每个 part 就是一次完整的纠删码写**：切块、算 parity、并行写各盘、bitrot——和深读 01 的
  `PutObject` 同一套（buffer 三策略、readahead、quorum 判定都复用）。区别只是写到 multipart 目录、
  且不立刻可见。
- **并发 part 安全靠唯一临时名**（`uuid + timestamp`，`:611`）：同一个对象的多个 part 可以**同时**
  上传（这正是多段上传的卖点——并行）。临时文件名带 UUID+时间戳，互不冲突；rename 到
  `part.N` 时按 partID 落位，天然不撞。
- **复用 upload 的 `Erasure.Distribution`**（`:604`）：所有 part 用同一个分布（NewMultipartUpload
  时定好），保证整个对象的所有 part 在盘间一致分布——Complete 后能当一个对象统一读。
- **part 的 ActualSize（压缩/加密前大小）单独记**：和深读 01 一样，加密 part 用 `sio.DecryptedSize`
  反推明文大小，压缩 part 保持 -1。
`──────────────────────────────────────────`

---

## 3. `CompleteMultipartUpload`：校验 + 合并

这是多段上传最精密的一步（`:1090`）。客户端发来 part 列表（编号 + ETag），服务端要**逐 part 校验
再合并**。

### ① ETag 校验（加密对象要"解密调整"）
```go
// :1242
for i, part := range parts {
    partIdx := objectPartIndex(currentFI.Parts, part.PartNumber)   // 找到这个编号的 part
    if partIdx == -1 { return InvalidPart{} }
    expPart := currentFI.Parts[partIdx]
    part.ETag = canonicalizeETag(part.ETag)                        // 去多余引号
    expETag := tryDecryptETag(objectEncryptionKey, expPart.ETag, kind == crypto.S3)  // ★ 解密调整
    if expETag != part.ETag { return InvalidPart{ExpETag, GotETag} }
}
```
`★ 加密对象的 ETag 校验陷阱 ─────────────────────`
- 注释（`:1165`）解释得很细：**加密对象的 part ETag 在后端是加密的、更长**（前面拼了 IV+sealed
  key），但客户端 PutObjectPart 时收到的是明文 MD5。Complete 时客户端发来明文 MD5，服务端要
  `tryDecryptETag` 把后端存的加密 ETag**解回明文 MD5**再比对。
  - 后端存：`20000f00...e84a59f6` + `30902184f4e62dd8...`（前缀是加密元数据 + 真 MD5）
  - 客户端发：`30902184f4e62dd8...`（纯 MD5）
- 不做这个解密调整，所有加密对象的 Complete 都会 `InvalidPart` 失败。这是改加密+multipart 交互
  时极易踩的坑。
`──────────────────────────────────────────`

### ② 最小 part 大小校验（S3 兼容性硬约束）
```go
// :1305
if (i < len(parts)-1) && !isMinAllowedPartSize(currentFI.Parts[partIdx].ActualSize) {
    return PartTooSmall{PartNumber, PartSize, PartETag}
}
```
- **除最后一个 part 外，每个 part 必须 ≥ 5MB**（S3 规范）。这是为了防止"几百万个 1 字节 part"
  把元数据撑爆。最后一个 part 不限（允许尾巴很小）。这是 S3 协议的硬性兼容点。

### ③ 校验和合并
```go
// :1266 每 part 校验 CRC32/CRC32C/SHA1/SHA256/CRC64NVME
// :1356 合并成对象级校验和
checksumType |= hash.ChecksumMultipart | hash.ChecksumIncludesMultipart
fi.Checksum = checksum.AppendTo(nil, checksumCombined)   // CRC-of-CRCs
```
`★ 多段对象的校验和是"CRC 的 CRC"───────────────`
- 整个对象的校验和不是"重算整个对象的 CRC"（那要重读所有数据），而是**对所有 part 的 CRC 拼起来
  再算一个 CRC**（`checksumCombined`）。对象级校验和 = `CRC(part1_crc ‖ part2_crc ‖ ...) + "-N"`。
  - **`FullObjectRequested`** 模式则用 `checksum.AddPart` 真正按字节合并（NVMe CRC 等支持组合）。
- 这让多段对象的完整性校验**无需重读全部数据**，只用各 part 已算好的 CRC 组合。S3 的
  `x-amz-checksum-*` 多段语义就是这样实现的。
`──────────────────────────────────────────`

### ④ Multipart ETag 与合并提交
```go
// 多段对象 ETag = md5(concat 所有 part 的 md5) + "-" + partCount
fi.Parts = 组装好的有序 part 列表
fi.Size  = sum(part.Size)
// 把合并后的 xl.meta 从 multipart 目录 rename 到对象正式位置（原子提交，同深读 04）
```
`★ 多段 ETag 的识别特征 ─────────────────────────`
- **多段对象的 ETag 形如 `<md5>-<N>`**（带 `-数字` 后缀），不是单段对象的纯 MD5。这就是为什么
  "多段上传的对象 ETag 跟你本地算的 md5 对不上"——它是 part md5 的 md5 + part 数。客户端做完整性
  校验时要按这个规则算，不能直接 md5 整个文件。
- Complete 后，最终对象的 `xl.meta` 指向 `uploadIDPath/DataDir/part.1..N`，data dir 从 multipart
  目录搬到对象正式位置。读时（深读 02）按 part 边界逐段解码——多段对象的"多 part"在 `fi.Parts`
  里，GET 时一个 part 一个 part 地读。
`──────────────────────────────────────────`

---

## 4. Abort 与过期清理

- **`AbortMultipartUpload`**：删除整个 upload 目录（`uploadIDPath`），放弃所有已传 part。
- **过期清理**（`:171` 的后台扫描）：扫 `minioMetaMultipartBucket`，对每个 upload 目录读其
  `xl.meta` 的 ModTime，**超过过期阈值（如 24h/可配）的陈旧 upload 被清理**——防止"开了 multipart
  没 Complete 也没 Abort"的孤儿 part 永久占空间。这是多段上传的垃圾回收。

---

## 5. 一页纸总结多段上传的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 进行中数据存 `.minio.sys/multipart`，Complete 才搬正式位置 | 半成品对客户端不可见 |
| 2 | 目录用对象名 SHA256 | 避免超长路径/特殊字符 |
| 3 | uploadID = base64(prefix.uuid) 内嵌路由 | Complete/Abort 能路由回正确 set |
| 4 | 每 part = 一次迷你 PutObject（复用纠删码写） | part 独立编码、可并行可重试 |
| 5 | 并发 part 靠 uuid+timestamp 临时名 | 同对象多 part 同时上传不冲突 |
| 6 | 所有 part 复用同一 Erasure.Distribution | Complete 后能当一个对象统一读 |
| 7 | 加密对象 ETag 校验要 tryDecryptETag | 后端加密 ETag 更长，需解回明文 MD5 |
| 8 | 除最后 part 外每 part ≥ 5MB | S3 硬约束，防元数据爆炸 |
| 9 | 对象校验和 = CRC-of-CRCs | 无需重读全部数据 |
| 10 | 多段 ETag = md5(part md5s)+"-N" | 解释"ETag 跟本地 md5 对不上" |
| 11 | 合并提交复用 renameData 原子改名 | 崩溃一致（同深读 04） |
| 12 | 后台清理陈旧 upload | 回收孤儿 part 占用 |

下一篇深读：**对象列举与 metacache**——List 的递归 walk、metacache 缓存复用、分页 marker、
前缀/分隔符语义。
