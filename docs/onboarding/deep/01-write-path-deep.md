# 深读 01 · 写入路径逐行精读（`erasureObjects.putObject`）

> 这是"源码精读"系列的第一篇。我们把 `cmd/erasure-object.go:1249` 的 `putObject` 一行行拆开，
> 连同它调用的 `NewErasure` / `Encode` / `multiWriter` / `streamingBitrotWriter` / `renameData`，
> 把每一个分支、每一个魔法数字、每一处并发与一致性设计讲到底。
>
> 阅读建议：左边开着 `cmd/erasure-object.go`，右边对照本文。所有行号基于撰写时的 `master`。

入口很短，真正的实现是 `putObject`：
```go
// erasure-object.go:1244
func (er erasureObjects) PutObject(...) (ObjectInfo, error) {
    return er.putObject(ctx, bucket, object, data, opts)   // 仅转发
}
```

---

## 1. 前置：审计、预条件检查与"第一把锁"

```go
// :1250
if !opts.NoAuditLog {
    auditObjectErasureSet(ctx, "PutObject", object, &er)
}
data := r.Reader   // r 是 *PutObjReader，data 是里面真正的 io.Reader
```
- `PutObjReader`（`r`）是第 2 篇讲的"装饰器链"的最外层：它内部已经套好了 hashReader、可能还有
  加密/压缩。这里取出 `r.Reader` 作为编码的输入源。

### 预条件检查（条件 PUT）
```go
// :1256
if opts.CheckPrecondFn != nil {
    if !opts.NoLock {
        ns := er.NewNSLock(bucket, object)
        lkctx, err := ns.GetLock(ctx, globalOperationTimeout)
        ...
        ctx = lkctx.Context()
        defer ns.Unlock(lkctx)
        opts.NoLock = true        // ★ 关键：标记"锁已持有"，后面别再加
    }
    obj, err := er.getObjectInfo(ctx, bucket, object, opts)
    if err == nil && opts.CheckPrecondFn(obj) {
        return objInfo, PreConditionFailed{}
    }
    if err != nil && !isErrVersionNotFound(err) && !isErrObjectNotFound(err) && !isErrReadQuorum(err) {
        return objInfo, err
    }
}
```
`★ 细节 ────────────────────────────────────`
- `CheckPrecondFn` 实现 `If-Match` / `If-None-Match` 这类条件写。它要先读到对象的当前状态，
  所以**只有条件写才会在这里提前加锁**（普通 PUT 不进这个分支）。
- **`isErrReadQuorum` 被当成"可以继续"的错误**：如果对象当前因为太多盘离线而读不出 quorum，
  预条件检查无法判定，但写入仍被允许继续——这是"宁可写成功也不要因为读不出旧版本就拒写"的
  取舍。`ObjectNotFound` / `VersionNotFound` 同理（对象本来就不存在，条件写当然能继续）。
- `opts.NoLock = true` 这一行很重要：它把"锁的所有权"往下传，避免 §7 再加一次锁导致**自死锁**。
`──────────────────────────────────────────`

### 输入大小校验
```go
// :1278
if data.Size() < -1 {
    bugLogIf(ctx, errInvalidArgument, logger.ErrorKind)
    return ObjectInfo{}, toObjectErr(errInvalidArgument)
}
```
- **`Size()` 允许等于 `-1`**，表示"大小未知的流"（如 chunked 上传、压缩流）。`< -1` 才是非法。
  记住这个 `-1` 语义，后面 buffer 分配、`ShardFileSize`、`actualSize` 都要特判它。
- `bugLogIf` 是"这不该发生"的内部 bug 日志——出现说明上层传错了，不是用户错误。

---

## 2. 计算 parity 与 writeQuorum（本对象专属，不是全局常量）

```go
// :1285
storageDisks := er.getDisks()
parityDrives := globalStorageClass.GetParityForSC(userDefined[xhttp.AmzStorageClass])
if parityDrives < 0 {
    parityDrives = er.defaultParityCount
}
if opts.MaxParity {
    parityDrives = len(storageDisks) / 2
}
```
- `GetParityForSC`：按对象的 storage class（`STANDARD` / `REDUCED_REDUNDANCY`）取 parity；
  返回 `<0` 表示没指定，落到默认值。
- `opts.MaxParity`：某些内部对象（如配置、IAM 数据）要最高冗余，直接取 `N/2`（最大可能 parity）。

### 可用性优化：写时根据离线盘动态**升级** parity
```go
// :1295
if !opts.MaxParity && globalStorageClass.AvailabilityOptimized() {
    parityOrig := parityDrives
    var offlineDrives int
    for _, disk := range storageDisks {
        if disk == nil || !disk.IsOnline() {
            parityDrives++         // 每有一块盘离线，parity +1
            offlineDrives++
        }
    }
    if offlineDrives >= (len(storageDisks)+1)/2 {
        // 离线盘 ≥ 半数 → 没 quorum，直接失败，别白写
        return ObjectInfo{}, toObjectErr(errErasureWriteQuorum, bucket, object)
    }
    if parityDrives >= len(storageDisks)/2 {
        parityDrives = len(storageDisks) / 2   // 封顶 N/2
    }
    if parityOrig != parityDrives {
        userDefined[minIOErasureUpgraded] = strconv.Itoa(parityOrig) + "->" + strconv.Itoa(parityDrives)
    }
}
```
`★ 精妙设计 ──────────────────────────────────`
- **这是 MinIO 写入韧性的核心技巧之一**。假设 EC(8,4)，正常写 8 数据 + 4 校验。如果此刻有 2 块盘
  离线，系统会把 parity 临时升到 6（数据降到 6），让这个对象用"6+6"的更高冗余写入——这样即使
  当下已经少了 2 块盘，对象依然有完整的冗余度。
- `offlineDrives >= (len+1)/2` 的**快速失败**：离线过半连 quorum 都不可能，与其写一半再回滚，
  不如立刻返回 `errErasureWriteQuorum`。注意是 `(len+1)/2`（向上取整），偶数盘时半数就算没 quorum。
- **`minIOErasureUpgraded` 面包屑**：把 `"4->6"` 这种升级记录写进对象元数据。这样后台 heal 扫到
  这个对象，知道"它当初是降级写的"，可以择机重新均衡回标准 parity。这是一个"留痕给未来的自己"
  的优雅做法——状态不靠猜，靠元数据自描述。
`──────────────────────────────────────────`

### writeQuorum 公式
```go
// :1323
dataDrives := len(storageDisks) - parityDrives
writeQuorum := dataDrives
if dataDrives == parityDrives {
    writeQuorum++          // data==parity 时 +1，保证严格多数
}
```
- 注释写的是 "writeQuorum is dataBlocks + 1"，但代码其实是 `dataBlocks`（只有 `data==parity`
  时才 +1）。**以代码为准**：例如 EC(8,4) data=8 → writeQuorum=8；EC(6,6) → writeQuorum=7。
- 为什么 `data==parity` 要 +1？因为此时 read quorum 也是 data，若 write quorum 也等于 data，
  读写 quorum 集合可能恰好不重叠（脑裂窗口）。+1 强制写多数，保证与任何读 quorum 有交集。

---

## 3. 构造 FileInfo 与"打散"盘序

```go
// :1333
partsMetadata := make([]FileInfo, len(storageDisks))
fi := newFileInfo(pathJoin(bucket, object), dataDrives, parityDrives)
fi.VersionID = opts.VersionID
if opts.Versioned && fi.VersionID == "" {
    fi.VersionID = mustGetUUID()        // 开启版本控制 → 生成版本 UUID
}
fi.DataDir = mustGetUUID()              // 本次写入的数据目录（隔离不同版本/覆盖写）
...
uniqueID := mustGetUUID()
tempObj := uniqueID                     // 临时落盘目录名
for index := range partsMetadata {
    partsMetadata[index] = fi           // 每块盘先拿一份相同的 FileInfo 副本
}
// :1358
onlineDisks, partsMetadata = shuffleDisksAndPartsMetadata(storageDisks, partsMetadata, fi)
```
`★ 细节 ────────────────────────────────────`
- **三个不同的 UUID**，别混：
  - `VersionID`：对象的版本标识（versioning 语义，对用户可见）。
  - `DataDir`：这次写入的数据物理目录名。**覆盖写时新版本用新 `DataDir`，旧数据目录留待清理**
    （见 §7 的 `oldDataDir`），这让覆盖写不会就地破坏旧数据，是崩溃一致的一环。
  - `uniqueID`/`tempObj`：临时区目录，编码先写这里，成功后再 rename 到正式位置。
- **`shuffleDisksAndPartsMetadata` 按 `ErasureDist` 打散盘序**。`fi` 里有一个分布数组，决定
  "第 i 个分片写到哪块物理盘"。每个对象的分布不同，所以盘上看到的分片顺序是"乱"的——这是
  为了让数据块/校验块在盘间均匀，不让某块盘永远只扛校验。读时再 shuffle 回来（深读 02 会讲）。
`──────────────────────────────────────────`

---

## 4. `NewErasure`：Reed-Solomon 编码器的懒加载

```go
// erasure-coding.go:42
func NewErasure(ctx, dataBlocks, parityBlocks int, blockSize int64) (e Erasure, err error) {
    if dataBlocks <= 0 || parityBlocks < 0 { return e, reedsolomon.ErrInvShardNum }
    if dataBlocks+parityBlocks > 256       { return e, reedsolomon.ErrMaxShardNum }   // ★ 256 上限
    e = Erasure{dataBlocks, parityBlocks, blockSize}
    var enc reedsolomon.Encoder
    var once sync.Once
    e.encoder = func() reedsolomon.Encoder {        // ★ 闭包 + sync.Once 懒初始化
        once.Do(func() {
            enc, _ = reedsolomon.New(dataBlocks, parityBlocks,
                       reedsolomon.WithAutoGoroutines(int(e.ShardSize())))
        })
        return enc
    }
    return
}
```
`★ 细节 ────────────────────────────────────`
- **`dataBlocks+parityBlocks > 256` 上限**：Reed-Solomon 在 GF(2^8) 上运算，最多 256 个分片。
  所以单个 erasure set 最多 256 块盘——这是数学约束，不是工程限制。
- **编码器用 `sync.Once` 懒加载**：`Erasure` 结构体可能被构造但不一定真的编码（比如只算
  `ShardFileSize`），把昂贵的矩阵初始化推迟到第一次真正编码时。
- **`WithAutoGoroutines(ShardSize)`**：reedsolomon 库会根据分片大小自动决定用多少 goroutine
  并行做 GF 运算。分片越大越值得多线程。
`──────────────────────────────────────────`

### ShardSize 与 ShardFileSize 的数学
```go
// erasure-coding.go:116
func (e *Erasure) ShardSize() int64 { return ceilFrac(e.blockSize, int64(e.dataBlocks)) }

// :121  把"原始对象大小"换算成"每块盘上分片文件的大小"
func (e *Erasure) ShardFileSize(totalLength int64) int64 {
    if totalLength == 0  { return 0 }
    if totalLength == -1 { return -1 }                 // 未知流
    numShards     := totalLength / e.blockSize
    lastBlockSize := totalLength % e.blockSize
    lastShardSize := ceilFrac(lastBlockSize, int64(e.dataBlocks))
    return numShards*e.ShardSize() + lastShardSize
}
```
- `ShardSize = ⌈blockSize / dataBlocks⌉`。每个 block 被切成 `dataBlocks` 份，每份就是一个 shard。
- `ShardFileSize` 要分别算"满 block 的部分"和"最后不满一个 block 的尾巴"——尾巴单独 `ceilFrac`，
  因为最后一块可能不满。这个换算在 inline 判断、bitrot 文件总大小、读偏移计算里反复用到。

---

## 5. Buffer 分配：三种大小策略（省内存的细节）

```go
// :1366
var buffer []byte
switch size := data.Size(); {
case size == 0:
    buffer = make([]byte, 1)        // 至少 1 字节，让 ReadFull 能触达 EOF
case size >= fi.Erasure.BlockSize || size == -1:
    buffer = globalBytePoolCap.Load().Get()      // 从全局 buffer 池借一整块
    defer globalBytePoolCap.Load().Put(buffer)
case size < fi.Erasure.BlockSize:
    // 小对象：不借整块，按需分配，cap 留足编码所需余量
    buffer = make([]byte, size, 2*size+int64(fi.Erasure.ParityBlocks+fi.Erasure.DataBlocks-1))
}
if len(buffer) > int(fi.Erasure.BlockSize) {
    buffer = buffer[:fi.Erasure.BlockSize]      // 不超过一个 block
}
```
`★ 精妙设计 ──────────────────────────────────`
- **三种策略对应三种经济学**：
  - 空对象：1 字节占位（仍要走编码流程创建空文件，见 §6 的空对象注释）。
  - 大对象 / 未知大小：从 `globalBytePoolCap` 池借满 blockSize 的 buffer，用完归还，**复用避免 GC**。
  - 小对象：`make` 一个刚好够的，**不污染 buffer 池**（小对象借大 buffer 是浪费）。
- 小对象那个 cap `2*size + parity + data - 1` 不是拍脑袋：reedsolomon 的 `Split` 会把数据
  填充对齐到 `dataBlocks` 的整数倍并附加 parity 分片，这个 cap 保证后续 in-place 编码不需要
  二次扩容（避免 `append` 重新分配）。`-1` 是对齐取整的边界修正。**这种"精确预留容量"是
  MinIO 在热路径上抠内存的典型手法。**
`──────────────────────────────────────────`

---

## 6. 为每块盘建 Writer，并执行流式编码

### inline 判定与 writer 构造
```go
// :1388
var inlineBuffers []*bytes.Buffer
if globalStorageClass.ShouldInline(erasure.ShardFileSize(data.ActualSize()), opts.Versioned) {
    inlineBuffers = make([]*bytes.Buffer, len(onlineDisks))
}
shardFileSize := erasure.ShardFileSize(data.Size())
writers := make([]io.Writer, len(onlineDisks))
for i, disk := range onlineDisks {
    if disk == nil || !disk.IsOnline() { continue }   // 离线盘对应 writer 留 nil
    if len(inlineBuffers) > 0 {
        buf := grid.GetByteBufferCap(int(shardFileSize) + 64)     // ★ +64 给 bitrot 头留余量
        inlineBuffers[i] = bytes.NewBuffer(buf[:0])
        defer grid.PutByteBuffer(buf)
        writers[i] = newStreamingBitrotWriterBuffer(inlineBuffers[i], DefaultBitrotAlgorithm, erasure.ShardSize())
        continue
    }
    writers[i] = newBitrotWriter(disk, bucket, minioMetaTmpBucket, tempErasureObj, shardFileSize, DefaultBitrotAlgorithm, erasure.ShardSize())
}
```
- **inline 路径**：小对象的分片写进**内存 buffer**（最终塞进 `xl.meta`），不落独立 part 文件。
  `+64` 是给每个 shard 前缀的 bitrot 哈希预留空间（highwayhash256 是 32 字节，留 64 富余）。
- **非 inline 路径**：`newBitrotWriter` 直接往临时盘文件写。

### streamingBitrotWriter：盘上分片文件的真实格式
```go
// bitrot-streaming.go:108 newStreamingBitrotWriter
buf := globalBytePoolCap.Load().Get()
rb := ringbuffer.NewBuffer(buf[:cap(buf)]).SetBlocking(true)     // ★ 阻塞式环形缓冲
bw := &streamingBitrotWriter{
    iow:          ioutil.NewDeadlineWriter(rb.WriteCloser(), globalDriveConfig.GetMaxTimeout()),
    closeWithErr: rb.CloseWithError,
    ...
}
bw.canClose.Add(1)
go func() {                                                       // ★ 独立 goroutine 落盘
    defer bw.canClose.Done()
    totalFileSize := int64(-1)
    if length != -1 {
        bitrotSumsTotalSize := ceilFrac(length, shardSize) * int64(h.Size())   // 校验和占用
        totalFileSize = bitrotSumsTotalSize + length
    }
    rb.CloseWithError(disk.CreateFile(context.TODO(), origvolume, volume, filePath, totalFileSize, rb))
}()
```
每次 `Write(p)`（`bitrot-streaming.go:44`）：
```go
b.h.Reset(); b.h.Write(p); hashBytes := b.h.Sum(nil)
b.iow.Write(hashBytes)   // 先写 32 字节哈希
b.iow.Write(p)           // 再写该 shard 的数据
if int64(len(p)) < b.shardSize { b.finished = true }  // 不满一个 shard = 最后一块
```
`★ 盘上格式 + 并发设计 ─────────────────────────`
- **盘上 part 文件的物理布局是 `[hash_0][shard_0][hash_1][shard_1]...`**：每个 shard 前面紧跟
  它自己的 32 字节 highwayhash。所以文件总大小 = `数据 + ⌈length/shardSize⌉ × 32`。读时按这个
  布局边读边校验（深读 02 详解）。
- **ringbuffer + 独立 goroutine**：`Write` 把"算哈希 + 塞环形缓冲"和"真正的磁盘 I/O"解耦。
  调用方写进 ringbuffer（满了会阻塞 = 自然背压），后台 goroutine 从 ringbuffer 读出来调
  `disk.CreateFile` 落盘。这让 N+K 块盘的写入真正并行，慢盘不阻塞编码主循环。
- **`DeadlineWriter`**：给磁盘写加超时（`globalDriveConfig.GetMaxTimeout()`），卡死的坏盘不会
  无限拖住整个写入——超时就当这块盘失败，靠 quorum 兜底。
- **`Close` 必须 `canClose.Wait()`**（`:89`）：注释解释得很清楚——`io.Pipe` 的 `Close()` 可能
  在数据真正落盘前就返回，若不等后台 goroutine 完成，紧接着的 `Read` 会读到不完整数据。这是
  一个**真实踩过的竞态坑**，留了大段注释警示。
`──────────────────────────────────────────`

### 大文件预读优化
```go
// :1414
toEncode := io.Reader(data)
if data.Size() >= bigFileThreshold {
    pool := globalBytePoolCap.Load()
    bufA := pool.Get(); bufB := pool.Get()
    defer pool.Put(bufA); defer pool.Put(bufB)
    ra, err := readahead.NewReaderBuffer(data, [][]byte{bufA[:blockSize], bufB[:blockSize]})
    if err == nil { toEncode = ra; defer ra.Close() }
}
```
- 大文件用**双缓冲 readahead**：一个 buffer 在被编码时，另一个已经在从网络/磁盘预读下一块，
  让"读输入"和"编码+写盘"流水线化，吃满带宽。

### Encode 主循环 + multiWriter
```go
// erasure-encode.go:69
func (e *Erasure) Encode(ctx, src, writers, buf, quorum) (total int64, err error) {
    writer := &multiWriter{writers, quorum, make([]error, len(writers))}
    for {
        n, err := io.ReadFull(src, buf)            // 读满一个 block
        ... // EOF / ErrUnexpectedEOF 容忍
        if n == 0 && total != 0 { break }          // 读完了
        blocks, _ := e.EncodeData(ctx, buf[:n])    // Split 成 data 份 + Encode 出 parity 份
        if err = writer.Write(ctx, blocks); err != nil { return 0, err }
        total += int64(n)
        if eof { break }
    }
    return total, nil
}
```
`multiWriter.Write`（`erasure-encode.go:34`）的并发与 quorum 判定：
```go
for i := range p.writers {
    if p.errs[i] != nil { continue }              // 这块盘已经失败过，跳过（错误粘连）
    if p.writers[i] == nil { p.errs[i] = errDiskNotFound; continue }
    n, p.errs[i] = p.writers[i].Write(blocks[i])
    if p.errs[i] == nil && n != len(blocks[i]) { p.errs[i] = io.ErrShortWrite; p.writers[i] = nil }
    else if p.errs[i] != nil { p.writers[i] = nil }
}
nilCount := countErrs(p.errs, nil)
if nilCount >= p.writeQuorum { return nil }        // ★ 够 quorum 就算这块成功，不等慢盘
return reduceWriteQuorumErrs(...)                  // 不够 → 失败
```
`★ 细节 ────────────────────────────────────`
- **错误粘连（error latching）**：一旦某块盘在某个 block 出错，`p.writers[i]=nil`，后续所有
  block 都跳过它。不会"这个 block 失败下个 block 又试"——失败的盘就彻底放弃，避免写出残缺分片。
- **`nilCount >= writeQuorum` 提前返回**：每写一个 block 就检查 quorum。注释还点出一个微妙点：
  **heal 时用 `writeQuorum=1`** 来修单块盘，这个短路逻辑让"只写 1 块盘"也能成功返回，
  否则 `reduceWriteQuorumErrs` 会因为其它盘没写而误报 quorum 错误。一个函数服务两种调用场景。
- 注意 `multiWriter` **不是**为每块盘起 goroutine——并发性来自每个 `bitrotWriter` 内部的
  ringbuffer+goroutine（§6）。`multiWriter` 只是顺序把 block 投递给各 writer 的 ringbuffer，
  投递本身很快（除非 ringbuffer 满了背压）。
`──────────────────────────────────────────`

---

## 7. 收尾：校验、元数据填充、原子提交

### 编码后的校验
```go
// :1429
n, erasureErr := erasure.Encode(ctx, toEncode, writers, buffer, writeQuorum)
closeErrs := closeBitrotWriters(writers)            // 关闭所有 writer（触发 flush + 等 goroutine）
if erasureErr != nil { return ObjectInfo{}, toObjectErr(erasureErr, ...) }
if closeErr := reduceWriteQuorumErrs(ctx, closeErrs, objectOpIgnoredErrs, writeQuorum); closeErr != nil {
    return ObjectInfo{}, toObjectErr(closeErr, ...)  // 关闭阶段才暴露的写错误也要过 quorum
}
if n < data.Size() {                                 // 客户端给的字节比声明的少
    return ObjectInfo{}, IncompleteBody{Bucket: bucket, Object: object}
}
```
- **两道 quorum 检查**：编码时（`multiWriter`）一道，关闭时（`closeBitrotWriters` 把后台 goroutine
  的最终错误收上来）又一道。因为 ringbuffer 异步，有些写错误只有在 `Close` 等 goroutine 时才浮现。

### actualSize：压缩/加密下的真实大小
```go
// :1456
actualSize := data.ActualSize()
if actualSize < 0 {
    switch {
    case fi.IsCompressed():  // 压缩流且未知 → 无法得知，保持 -1
    case encrypted:
        decSize, err := sio.DecryptedSize(uint64(n))   // 从密文长度反推明文长度
        if err == nil { actualSize = int64(decSize) }
    default:
        actualSize = n
    }
}
```
- `n` 是**落盘的字节数**（可能是加密/压缩后的）。`actualSize` 要还原成**用户原始对象大小**，
  用于对外的 `Content-Length`。加密用 `sio.DecryptedSize` 从 DARE 密文长度精确反算明文长度。

### 把分片数据/校验和写进各盘的 FileInfo
```go
// :1480
for i, w := range writers {
    if w == nil { onlineDisks[i] = nil; continue }   // 这块盘没写成功 → 标记下线
    if len(inlineBuffers) > 0 && inlineBuffers[i] != nil {
        partsMetadata[i].Data = inlineBuffers[i].Bytes()   // inline：数据进元数据
    } else {
        partsMetadata[i].Data = nil
    }
    partsMetadata[i].AddObjectPart(1, "", n, actualSize, modTime, compIndex, nil)  // part.1
    partsMetadata[i].Versioned = opts.Versioned || opts.VersionSuspended
    partsMetadata[i].Checksum  = fi.Checksum
}
```
- **`AddObjectPart(1, ...)`**：普通 PUT 只有一个 part（编号 1）。多段上传才会有多个 part
  （那是另一条路径 `CompleteMultipartUpload`）。

### etag、content-type、storage-class 的几处微妙处理
```go
// :1496
userDefined["etag"] = r.MD5CurrentHexString()
if opts.PreserveETag != "" {
    if !opts.ReplicationRequest {
        userDefined["etag"] = opts.PreserveETag
    } else if kind != crypto.S3 {
        // 复制请求 + SSE-S3 时不保留源 etag（因为重新加密会改变 etag 语义）
        userDefined["etag"] = opts.PreserveETag
    }
}
// :1509  没给 content-type → 按扩展名猜
if userDefined["content-type"] == "" {
    userDefined["content-type"] = mimedb.TypeByExtension(path.Ext(object))
}
// :1514  STANDARD 是默认，不存进元数据省空间
if userDefined[xhttp.AmzStorageClass] == storageclass.STANDARD {
    delete(userDefined, xhttp.AmzStorageClass)
}
```
`★ 细节 ────────────────────────────────────`
- **复制 + SSE-S3 的 etag 例外**（`:1500`）：跨站点复制时通常要保留源对象 etag 以便比对；
  但 SSE-S3 加密对象的 etag 不是明文 MD5，复制目标会重新加密，保留源 etag 会造成 etag 与
  实际内容不符。所以这种组合下**不**保留。这是一个非常细的正确性边界。
- **`STANDARD` 不落元数据**：默认 storage class 不存，省 `xl.meta` 空间；读时缺失就当 STANDARD。
  典型的"约定优于配置"省空间手法。
`──────────────────────────────────────────`

### 填充剩余元数据 + "第二把锁"
```go
// :1520
for index := range partsMetadata {
    partsMetadata[index].Metadata = userDefined
    partsMetadata[index].Size     = n
    partsMetadata[index].ModTime  = modTime
    if len(inlineBuffers) > 0 { partsMetadata[index].SetInlineData() }   // 打 inline 标记
    if opts.DataMovement       { partsMetadata[index].SetDataMov() }     // 重平衡/解碰撞搬运标记
}
// :1532  ★ 真正的写锁在这里才加 —— 编码全程没持锁！
if !opts.NoLock {
    lk := er.NewNSLock(bucket, object)
    lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
    if err != nil { return ObjectInfo{}, err }
    ctx = lkctx.Context()
    defer lk.Unlock(lkctx)
}
```
`★ 重大设计：锁的临界区极小化 ───────────────────`
- **整个昂贵的编码 + 写盘过程（§4–§7 前半）都没有持有 namespace 锁！** 数据被写到**临时区**
  （`tempObj`），不会被任何读路径看到。只有最后一步——把临时区原子改名到正式位置——才需要
  在锁保护下做。
- 这是巨大的并发优化：一个对象的写入主体（可能几秒、几 GB）不阻塞对该对象的其它读/写；
  互斥窗口被压缩到"rename + commit"这一小段。代价是**两个并发 PUT 同一对象**可能各自写完
  临时区，最后串行 rename——后 rename 的胜出（last-writer-wins），前者的临时区被清理。
- `opts.NoLock` 在 §1 的条件写分支里已被设为 true（锁已持有），这里就不重复加——避免自死锁。
`──────────────────────────────────────────`

### renameData：并行原子改名 + 失败回滚
```go
// erasure-object.go:1013
func renameData(ctx, disks, srcBucket, srcEntry, metadata, dstBucket, dstEntry, writeQuorum) (...) {
    g := errgroup.WithNErrs(len(disks))
    fvID := mustGetUUID()
    for index := range disks { metadata[index].SetTierFreeVersionID(fvID) }  // 分层"自由版本"标识

    diskVersions := make([][]byte, len(disks))
    dataDirs     := make([]string, len(disks))
    for index := range disks {
        g.Go(func() error {
            if disks[index] == nil { return errDiskNotFound }
            fi := metadata[index]
            if fi.Erasure.Index == 0 { fi.Erasure.Index = index + 1 }   // ★ 1-based 条带索引
            if !fi.IsValid() { return errFileCorrupt }
            resp, err := disks[index].RenameData(ctx, srcBucket, srcEntry, fi, dstBucket, dstEntry, RenameOptions{})
            if err != nil { return err }
            diskVersions[index] = resp.Sign
            dataDirs[index]     = resp.OldDataDir
            return nil
        }, index)
    }
    errs := g.Wait()
    err := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
    if err != nil {
        // ★ 没达 quorum → 回滚已成功的盘（UndoWrite），把对象删掉
        dg := errgroup.WithNErrs(len(disks))
        for index, nerr := range errs {
            if nerr != nil { continue }
            dg.Go(func() error {
                return disks[index].DeleteVersion(context.Background(), dstBucket, dstEntry, metadata[index], false,
                    DeleteOptions{UndoWrite: true, OldDataDir: dataDirs[index]})
            }, index)
        }
        dg.Wait()
    }
    var dataDir string; var versions []byte
    if err == nil {
        versions = reduceCommonVersions(diskVersions, writeQuorum)
        for index, dversions := range diskVersions {
            if errs[index] != nil { continue }
            if !bytes.Equal(dversions, versions) {        // ★ 版本签名不一致 → 取更长的那个
                if len(dversions) > len(versions) { versions = dversions }
                break
            }
        }
        dataDir = reduceCommonDataDir(dataDirs, writeQuorum)
    }
    return evalDisks(disks, errs), versions, dataDir, err
}
```
`★ 这是写入一致性的最高潮，细节密集 ─────────────`
- **`RenameData` 是单盘上的原子操作**：把临时目录里的 `part.1` + `xl.meta` 一次性改名到
  `bucket/object/<DataDir>/`。文件系统的 `rename` 在同一挂载点上是原子的——要么整体生效，
  要么完全没发生。所以**对象的"出现"是原子的**：永远不会读到"有 part 没 xl.meta"的中间态。
- **`Erasure.Index = index + 1`（1-based）**：记录这块盘是条带里的第几个分片。0 被保留为"未设",
  所以从 1 开始。读时靠它把分片对回正确的逻辑位置。
- **失败回滚 `UndoWrite`**：如果改名没达到 quorum，对已经改名成功的盘执行 `DeleteVersion`
  并带 `UndoWrite:true` + `OldDataDir`——这会删掉新写的，并尝试**恢复**被覆盖的旧 `DataDir`。
  注释还说：如果连回滚都失败也不告诉调用方，反正这个"悬挂对象"会被 active healing 清理。
  **没有事务管理器，靠"回滚 + 兜底 heal"达成最终一致。**
- **`reduceCommonVersions` + 取更长版本**：各盘返回的"版本签名"（`resp.Sign`，代表该对象当前
  所有版本的指纹）若不一致，说明有的盘版本历史比别的全。取**更长**的那个并返回 `versions`——
  这个 `versions` 非空就意味着"盘间版本有分歧"，回到 `putObject` §7 末尾会触发**针对这些版本的
  MRF heal**（见下）。这是"写入时顺便发现并安排修复版本不一致"的精巧联动。
`──────────────────────────────────────────`

### commit、挑选最终 fi、安排 MRF 修复
```go
// :1556
if err = er.commitRenameDataDir(ctx, bucket, object, oldDataDir, onlineDisks, writeQuorum); err != nil {
    return ObjectInfo{}, toObjectErr(err, ...)        // 清理被覆盖的旧 DataDir
}
for i := 0; i < len(onlineDisks); i++ {               // 任挑一块在线盘的 meta 作为返回
    if onlineDisks[i] != nil && onlineDisks[i].IsOnline() { fi = partsMetadata[i]; break }
}
if !opts.Speedtest {
    if len(versions) == 0 {
        // 版本无分歧 → 只为"写入期间离线的盘"安排部分修复
        for i := 0; i < len(onlineDisks); i++ {
            if onlineDisks[i] != nil && onlineDisks[i].IsOnline() { continue }
            er.addPartial(bucket, object, fi.VersionID)    // 进 MRF（修这一个版本）
            break
        }
    } else {
        // 版本有分歧 → 安排修复涉及的所有版本
        globalMRFState.addPartialOp(PartialOperation{
            Bucket: bucket, Object: object, Queued: time.Now(),
            Versions: versions, SetIndex: er.setIndex, PoolIndex: er.poolIndex,
        })
    }
}
fi.ReplicationState = opts.PutReplicationState()
fi.IsLatest = true                                    // 锁内新增的版本，必是最新
return fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended), nil
```
`★ 收尾的两条 MRF 分支 ─────────────────────────`
- **`commitRenameDataDir`**：覆盖写时，旧版本的 `DataDir`（`oldDataDir`）现在可以删了。它被
  推迟到 rename 成功后才删——又是一处"先确保新数据就位，再清旧数据"的崩溃一致设计。
- **两种修复触发**：
  - `len(versions)==0`（版本一致）：只要有盘在写入期间离线没写上，就把这个对象的**这个版本**
    扔进 MRF（`addPartial`），后台把缺的分片补到那些盘。
  - `len(versions)!=0`（版本分歧）：把**涉及的所有版本**打包成 `PartialOperation` 交给
    `globalMRFState`，带上 `SetIndex/PoolIndex` 让 heal 精确定位。
- `IsLatest = true`：因为整段是在 namespace 写锁内完成的，这个新版本就是当前最新版，直接置位
  而不必再去读盘确认。
`──────────────────────────────────────────`

### `deleteAll` 临时区清理（defer 早就排好）
```go
// :1385  在 §6 之前就 defer 了
defer er.deleteAll(context.Background(), minioMetaTmpBucket, tempObj)
```
- 无论成功失败，函数返回时清理临时区。注意它用 `context.Background()`——即使请求 ctx 被取消，
  清理仍要执行，不能因为客户端断开就留下临时垃圾。

---

## 8. 一页纸总结这条路径的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 编码全程不持锁，只 rename 持锁 | 把互斥窗口从"整个上传"压到"一次改名"，并发吞吐的关键 |
| 2 | parity 写时按离线盘动态升级 + `minIOErasureUpgraded` 面包屑 | 故障期间写入仍保持冗余，并留痕给 heal |
| 3 | `offlineDrives >= (N+1)/2` 快速失败 | 没 quorum 就别白写 |
| 4 | buffer 三策略 + 小对象精确 cap + 池复用 | 热路径抠内存、降 GC |
| 5 | bitrotWriter = ringbuffer + goroutine + DeadlineWriter | 真并行落盘、自然背压、坏盘超时不拖累 |
| 6 | 盘上格式 `[hash][shard]...`，总大小含校验和 | bitrot 的物理基础 |
| 7 | `multiWriter` 错误粘连 + `nilCount>=quorum` 短路 + heal 复用 writeQuorum=1 | 一套写逻辑服务正常写与单盘 heal |
| 8 | `RenameData` 文件系统原子改名 | 对象"出现"是原子的，无中间态可读 |
| 9 | 失败 `UndoWrite` 回滚 + 兜底 active heal | 无事务管理器的最终一致 |
| 10 | `versions` 分歧 → 写时顺带安排 MRF 修复 | 自愈与写入联动 |
| 11 | `commitRenameDataDir` 推迟删旧 DataDir | 先就位新数据再清旧，崩溃一致 |
| 12 | 临时区清理用 `context.Background()` | 客户端断开也要清垃圾 |

下一篇深读：**读取路径**——`getObjectFileInfo` 如何并行读 `xl.meta` 凑 quorum、
`parallelReader` 如何"够了就停"、Reed-Solomon 如何在缺块时重建，以及 `io.Pipe` 背压。
