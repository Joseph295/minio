# 深读 02 · 读取路径逐行精读（`GetObjectNInfo` → `Decode`）

> 接上篇写入路径。读取路径表面是"把对象读出来发给客户端"，内部却藏着 MinIO 最精巧的并发代码——
> `parallelReader` 的"触发器通道"自平衡并行读。本篇逐行拆 `cmd/erasure-object.go:202`
> 的 `GetObjectNInfo`、`getObjectFileInfo`（读元数据凑 quorum）、`getObjectWithFileInfo`（逐 part
> 解码）、`cmd/erasure-decode.go` 的 `parallelReader`/`Decode`，以及 bitrot 读取端的偏移换算。

读路径分三段：**①读元数据凑 quorum → ②建管道边解码边发送 → ③逐 part 并行读+重建**。

---

## 1. `GetObjectNInfo`：读锁的"提前释放"与各种短路

```go
// :207
var unlockOnDefer bool
nsUnlocker := func() {}
defer func() { if unlockOnDefer { nsUnlocker() } }()

if !opts.NoLock {
    lock := er.NewNSLock(bucket, object)
    lkctx, err := lock.GetRLock(ctx, globalOperationTimeout)   // ★ 读锁（多读者可并发）
    if err != nil { return nil, err }
    ctx = lkctx.Context()
    unlockOnDefer = true
    nsUnlocker = func() { lock.RUnlock(lkctx) }
}
fi, metaArr, onlineDisks, err := er.getObjectFileInfo(ctx, bucket, object, opts, true)
```
`★ 设计：读锁能提前释放的理由（源码注释原话）─────`
注释（`:224-233`）解释了一个很妙的优化——**读锁只需保护到"元数据被验证、reader 就绪"，
不需要保护整个数据发送过程**：
- inline 对象的数据在读 `xl.meta` 时**已经进内存**了（`fi.Data`），之后即使有并发写改了盘上的
  `xl.meta` 也不影响这次读到的内存数据。
- 非 inline 对象的元数据 quorum 在锁内已验证完；真正"把字节流写给客户端"这一步不需要串行化
  并发写者（写者写的是新版本的新 `DataDir`，不动当前正在读的旧 `DataDir`）。

所以 `:287` 有这一行：
```go
if unlockOnDefer {
    unlockOnDefer = fi.InlineData() || len(fi.Data) > 0   // inline → defer 里释放；非 inline → 交给 reader 释放
}
```
inline 数据已在内存，defer 释放锁没问题；非 inline 则把锁释放权交给 `GetObjectReader` 的清理函数
（`fn(pr, h, pipeCloser, nsUnlocker)` vs `fn(pr, h, pipeCloser)`，`:302-306`），让锁活到流真正
建立。**读锁的持有时间被压到最短**——这是高并发读的关键。
`──────────────────────────────────────────`

### 一连串短路（每个都值得记住）
```go
// :243
objInfo := fi.ToObjectInfo(...)
if objInfo.DeleteMarker {                       // ① 命中删除标记
    if opts.VersionID == "" {
        return &GetObjectReader{ObjInfo: objInfo}, toObjectErr(errFileNotFound, ...)
    }
    return &GetObjectReader{ObjInfo: objInfo}, toObjectErr(errMethodNotAllowed, ...)
}
// :257  SSE-C 对象 + 复制请求 → 不解密（按密文原样复制到目标）
if crypto.SSEC.IsEncrypted(objInfo.UserDefined) && opts.ReplicationRequest {
    opts.NoDecryption = true
}
// :261  零字节对象 → 直接返回空 reader，连 pipe 都不建
if objInfo.Size == 0 {
    if _, _, err := rs.GetOffsetLength(objInfo.Size); err != nil { return ..., err }
    return NewGetObjectReaderFromReader(bytes.NewReader(nil), objInfo, opts)
}
// :273  已转储到远端 tier 的对象 → 走 tier driver 拉取
if objInfo.IsRemote() {
    gr, err := getTransitionedObjectReader(ctx, bucket, object, rs, h, objInfo, opts)
    ...
    unlockOnDefer = false
    return gr.WithCleanupFuncs(nsUnlocker), nil
}
```
`★ 细节 ────────────────────────────────────`
- **删除标记的两种返回**：无版本号请求命中 delete marker → `errFileNotFound`（对用户就是"不存在"）；
  指定版本号却指向 delete marker → `errMethodNotAllowed`（你显式问一个"已删除"的版本，语义不同）。
- **SSE-C + 复制 → 不解密**：跨站点复制 SSE-C 对象时，源端不持有解密所需的客户端密钥，只能把
  密文原样搬过去。这与写路径里"SSE-S3 复制不保留 etag"是同一类跨加密复制的边界处理。
- **零字节对象不建 pipe**：省掉 goroutine + pipe 的开销，直接给个空 reader。小优化但热路径高频。
`──────────────────────────────────────────`

### 建立"边解码边发送"的管道
```go
// :291
pr, pw := xioutil.WaitPipe()
go func() {
    pw.CloseWithError(er.getObjectWithFileInfo(ctx, bucket, object, off, length, pw, fi, metaArr, onlineDisks))
}()
pipeCloser := func() { pr.CloseWithError(nil) }   // 客户端提前断开时，让上面的 goroutine 退出
```
- `WaitPipe` 是带 `Wait` 语义的管道：解码在一个 goroutine 里写 `pw`，HTTP 层从 `pr` 读。
- **背压自然形成**：客户端读得慢 → `pr` 读得慢 → `pw.Write` 阻塞 → 解码 goroutine 减速 →
  读盘减速。整条链路被客户端速度反向节流，不会把重建结果堆在内存。
- `pipeCloser` 处理"客户端中途断开"：调用它会让解码 goroutine 的 `pw.Write` 收到错误而退出，
  不泄漏 goroutine。

---

## 2. `getObjectFileInfo`：流式凑 quorum 读元数据

这是读路径里最绕但最值得读的函数（`:707`）。它要**并行问所有盘要 `xl.meta`，一边收一边判断
够不够 quorum，够了就尽早返回**。

### 并行发起 + 边到边收
```go
// :715
done := make(chan bool, er.setDriveCount)   // 每块盘一个"完成信号"
...
go func() {                                  // 后台并行问所有盘
    wg := sync.WaitGroup{}
    for i, disk := range disks {
        if disk == nil || !disk.IsOnline() { done <- false; continue }
        wg.Add(1)
        go func(i int, disk StorageAPI) {
            defer wg.Done()
            if opts.VersionID != "" {
                fi, err = disk.ReadVersion(ctx, "", bucket, object, opts.VersionID, ropts)   // 指定版本
            } else {
                rfi, err = disk.ReadXL(ctx, bucket, object, readData)                          // 最新版
                if err == nil { fi, err = fileInfoFromRaw(rfi, ...) }
            }
            rw.Lock(); rawArr[i] = rfi; metaArr[i], errs[i] = fi, err; rw.Unlock()
            done <- err == nil
        }(i, disk)
    }
    wg.Wait(); xioutil.SafeClose(done)
    ... // mrfCheck 异步 heal，见下
}()
```

### 主循环：达到 quorum 就尽早 break
```go
// :824  minDisks 启发式：先猜一个最小盘数，减少不必要的等待
minDisks := er.setDriveCount - er.defaultParityCount   // （还不知道对象真实 parity，只是估算）

// :864
for success := range done {                 // 每收到一块盘的结果就处理一次
    totalResp++; if success { validResp++ }

    if totalResp >= minDisks && opts.FastGetObjInfo {   // 快速模式：够多盘说"没有" → 直接判不存在
        ok := countErrs(errs, errFileNotFound) >= minDisks || countErrs(errs, errFileVersionNotFound) >= minDisks
        if ok { err = errFileNotFound; break }
    }
    if totalResp < er.setDriveCount {
        if !opts.FastGetObjInfo { continue }            // 普通模式：等齐所有盘再算（更稳）
        if validResp < minDisks   { continue }
    }

    // 版本桶 + 无版本号：必须等齐所有盘，用 rawFileInfo 解析出"最新版本"
    if opts.VersionID == "" && totalResp == er.setDriveCount {
        fi, onlineMeta, onlineDisks, modTime, etag, err = calcQuorum(pickLatestQuorumFilesInfo(ctx, rawArr, errs, ...))
    } else {
        fi, onlineMeta, onlineDisks, modTime, etag, err = calcQuorum(metaArr, errs)
    }
    if err == nil && (fi.InlineData() || len(fi.Data) > 0) { break }   // ★ inline 数据已到手，立刻收工
}
```
`★ 设计：流式 quorum 的取舍 ───────────────────`
- **`done` 是带缓冲的 channel**，后台 goroutine 把每块盘的成败投进去，主循环 `range done` 边收边算。
  这让"够 quorum 就返回"成为可能，不必死等最慢的盘。
- **`FastGetObjInfo`（快速模式）vs 普通模式**：快速模式（多用于内部高频探测）凑够 `minDisks`
  就敢下结论，省延迟；普通模式更保守，**等齐所有盘**再 `calcQuorum`，避免"前几块盘恰好是
  少数派旧元数据"导致误判。这是延迟 vs 正确性的显式开关。
- **`inline 数据到手立刻 break`**：小对象的数据就在 `xl.meta` 里，一旦 `calcQuorum` 成功且
  `fi.Data` 非空，整个对象（元数据+数据）都齐了，没必要再等剩下的盘——一次 `xl.meta` 读
  就完成了整个 GET。**这是 inline 设计在读路径上的红利**。
- **版本桶 + 无版本号要等齐所有盘**：因为"哪个是最新版本"需要看全所有盘的版本历史
  （`pickLatestQuorumFilesInfo`），少数盘可能漏了某些版本。
`──────────────────────────────────────────`

### `calcQuorum`：从元数据反推 quorum 再校验
```go
// :831
calcQuorum := func(metaArr, errs) (...) {
    readQuorum, _, err := objectQuorumFromMeta(ctx, metaArr, errs, er.defaultParityCount)  // ★ 反推本对象 parity
    if err != nil { return ..., err }
    if err := reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum); err != nil { return ..., err }
    onlineDisks, modTime, etag := listOnlineDisks(disks, metaArr, errs, readQuorum)
    fi, err := pickValidFileInfo(ctx, metaArr, modTime, etag, readQuorum)   // 选出"多数派"那份元数据
    ...
}
```
- `objectQuorumFromMeta` 就是深读会专门讲的"每对象 parity 可不同、用 `commonParity` 反推"
  （见 03/概览第 3 篇）。读时不能假设全局 parity，必须从各盘 `xl.meta` 里统计出这个对象当初的 parity。
- `pickValidFileInfo` 用 `modTime`（或退而求其次用 `etag`）选出"出现次数达 quorum 的那份元数据"
  作为权威 `fi`——少数派（旧的/损坏的）元数据被丢弃。

### 三个容易忽略但很重要的尾部检查
```go
// :907  读失败后：悬挂对象检测
if err != nil {
    if totalResp == er.setDriveCount && shouldCheckForDangling(err, errs, bucket) {
        _, derr := er.deleteIfDangling(...)        // 确认是悬挂残骸 → 删掉
        ...
    }
    if v, ok := err.(InsufficientReadQuorum); ok && v.Type == RQInconsistentMeta {
        err = errFileNotFound   // 元数据互相矛盾且不够 quorum → 当作不存在
    }
    return fi, nil, nil, toObjectErr(err, ...)
}
// :933  分布数组长度 != 在线盘数 → 有人手动改了后端盘！拒绝
if !fi.Deleted && len(fi.Erasure.Distribution) != len(onlineDisks) {
    // "looks like backend disks have been manually modified refusing to heal"
    return fi, nil, nil, toObjectErr(err, ...)
}
// :940  逐盘复核元数据：erasure 信息一致 + modTime（或 etag）一致，否则剔除这块盘
filterOnlineDisksInplace(fi, onlineMeta, onlineDisks)
for i := range onlineMeta {
    if onlineMeta[i].IsValid() && onlineMeta[i].Erasure.Equal(fi.Erasure) {
        ok := onlineMeta[i].ModTime.Equal(modTime)
        if modTime.IsZero() || modTime.Equal(timeSentinel) { ok = etag != "" && etag == fi.Metadata["etag"] }
        if ok { continue }
    }
    onlineMeta[i] = FileInfo{}; onlineDisks[i] = nil   // 不一致 → 这块盘的数据不可信，不从它读
}
```
`★ 细节 ────────────────────────────────────`
- **悬挂对象（dangling）**：写到一半崩溃、或回滚没干净的残骸。读到所有盘都回应但凑不齐有效
  对象时，`deleteIfDangling` 会确认并清理它——读操作顺便做垃圾回收。
- **`Distribution` 长度不符 = 人为篡改后端**：如果有人手动往盘目录里塞文件/改结构，分布数组
  对不上在线盘数，MinIO 宁可报错也**拒绝 heal**（怕把人为错误"修"成数据损坏）。这是一条
  防呆 + 防误修的保险。
- **逐盘 modTime/etag 复核**：即使某块盘有 `xl.meta`，若它的 modTime 与多数派不一致（可能是
  旧版本残留），也把它剔除，**不从不一致的盘读数据**——避免重建时混入旧分片。
`──────────────────────────────────────────`

### 读元数据后顺手触发 heal（`mrfCheck`）
```go
// :959
select {
case mrfCheck <- fi.ShallowCopy():        // 把权威 fi 发给后台 goroutine
case <-ctx.Done(): ...
}
```
后台 goroutine（`:777`）收到后判断：
```go
if countErrs(errs, errDiskNotFound) > 0 { return }   // 有盘整个离线 → 不在这 heal（等常规 heal）
missingBlocks := 统计 errFileNotFound/errFileVersionNotFound/errFileCorrupt 的盘数
if missingBlocks > 0 && missingBlocks < fi.Erasure.DataBlocks {   // 缺的不多、可重建
    globalMRFState.addPartialOp(PartialOperation{Bucket, Object, VersionID, SetIndex, PoolIndex, ...})
}
```
- **读元数据时如果发现少数盘缺/坏这个对象，就排个 MRF 修复**。条件 `missingBlocks < dataBlocks`
  保证"还有足够分片能重建"才修（缺太多重建不了，交给更高层处理）。读操作再次顺手安排自愈。

---

## 3. `getObjectWithFileInfo`：逐 part 并行读 + 重建

```go
// :309
onlineDisks, metaArr = shuffleDisksAndPartsMetadataByIndex(onlineDisks, metaArr, fi)  // ★ 按分布还原盘序
...
partIndex, partOffset, _ := fi.ObjectToPartOffset(ctx, startOffset)   // Range 落在第几个 part 的什么偏移
lastPartIndex, _, _ := fi.ObjectToPartOffset(ctx, endOffset)
erasure, _ := NewErasure(ctx, fi.Erasure.DataBlocks, fi.Erasure.ParityBlocks, fi.Erasure.BlockSize)

var healOnce sync.Once
for ; partIndex <= lastPartIndex; partIndex++ {
    partNumber := fi.Parts[partIndex].Number
    partSize   := fi.Parts[partIndex].Size
    partLength := partSize - partOffset
    if partLength > (length - totalBytesRead) { partLength = length - totalBytesRead }
    tillOffset := erasure.ShardFileOffset(partOffset, partLength, partSize)

    readers := make([]io.ReaderAt, len(onlineDisks))
    prefer  := make([]bool, len(onlineDisks))
    for index, disk := range onlineDisks {
        if disk == OfflineDisk || !metaArr[index].IsValid() { continue }
        if !metaArr[index].Erasure.Equal(fi.Erasure)        { continue }   // erasure 参数必须与权威一致
        checksumInfo := metaArr[index].Erasure.GetChecksumInfo(partNumber)
        partPath := pathJoin(object, metaArr[index].DataDir, fmt.Sprintf("part.%d", partNumber))
        readers[index] = newBitrotReader(disk, metaArr[index].Data, bucket, partPath, tillOffset,
                            checksumInfo.Algorithm, checksumInfo.Hash, erasure.ShardSize())
        prefer[index] = disk.Hostname() == ""     // ★ 本地盘（Hostname 空）优先
    }

    written, err := erasure.Decode(ctx, writer, readers, partOffset, partLength, partSize, prefer)
    closeBitrotReaders(readers)                   // ★ 不能 defer（在 for 里会攒一堆打开的文件）
    if err != nil {
        if written == partLength {                // 数据已经发完了，但底层有缺/坏
            if errors.Is(err, errFileNotFound) || errors.Is(err, errFileCorrupt) {
                healOnce.Do(func() {
                    globalMRFState.addPartialOp(PartialOperation{
                        Bucket, Object, VersionID: fi.VersionID,
                        BitrotScan: errors.Is(err, errFileCorrupt),   // 区分"缺文件"还是"坏块"
                        ...
                    })
                })
                err = nil                          // ★ 已成功发给客户端 → 吞掉错误，继续
            }
        }
        if err != nil { return toObjectErr(err, ...) }
    }
    totalBytesRead += partLength
    partOffset = 0                                 // 只有第一个 part 有起始偏移，后续从 0
}
```
`★ 读时自愈（heal-on-read）的精髓 ───────────────`
- **重建成功 ≠ 数据健康**。`Decode` 可能用校验块补齐了缺失的数据块——客户端拿到了正确数据，
  但盘上确实有块缺了/坏了。这里的逻辑是：**只要 `written == partLength`（该发的都发了）且错误是
  "缺文件/坏块"，就 (a) 安排 MRF 修复，(b) 把 `err` 置 nil 继续**。用户完全无感，后台默默修。
- **`BitrotScan` 标志**：区分"文件不见了"（补回去即可）和"文件在但内容坏了"（需要更彻底的
  bitrot 深扫）。修复策略据此不同。
- **`healOnce`**：一个 part 内部多块坏也只排一次 heal，避免刷爆 MRF 队列。
- **`closeBitrotReaders` 故意不 defer**：注释明说——在 for 循环里 defer 会累积大量打开的文件
  描述符直到函数返回。手动在每轮结束关闭，是个常见但容易忘的资源管理点。
- **`prefer[index] = disk.Hostname() == ""`**：本地盘的 `Hostname()` 为空。本地优先 → 重建尽量
  用本机分片，省网络带宽和延迟（分布式部署里尤其重要）。
`──────────────────────────────────────────`

---

## 4. `parallelReader`：触发器通道驱动的自平衡并行读

这是 `cmd/erasure-decode.go` 的核心，也是 MinIO 最值得品的并发设计。目标：**从 N+K 个 reader 里
读出 `dataBlocks` 个有效分片就够重建，慢盘/坏盘自动被后面的盘顶替，且不多读。**

### 触发器通道机制
```go
// :145  Read() 内部
readTriggerCh := make(chan bool, len(p.readers))
defer xioutil.SafeClose(readTriggerCh)
for i := 0; i < p.dataBlocks; i++ {
    readTriggerCh <- true            // ★ 先投 dataBlocks 个 true → 启动 dataBlocks 个并行读
}

readerIndex := 0
for readTrigger := range readTriggerCh {
    if p.canDecode(newBuf) { break }            // 已经凑够 dataBlocks 个分片 → 停
    if readerIndex == len(p.readers) { break }  // 盘用完了 → 停
    if !readTrigger { continue }                // false = 上一个读成功了，不需要新读

    wg.Add(1)
    go func(i int) {
        defer wg.Done()
        rr := p.readers[i]
        if rr == nil { readTriggerCh <- true; return }   // 这个 reader 不可用 → 触发下一个
        ...
        n, err := rr.ReadAt(p.buf[bufIdx], p.offset)
        if err != nil {
            // 记录错误类型（缺文件/坏块/盘没了），置 nil
            p.readers[i] = nil
            readTriggerCh <- true        // ★ 读失败 → 触发再读一块（顶替）
            return
        }
        newBuf[bufIdx] = p.buf[bufIdx][:n]
        readTriggerCh <- false           // ★ 读成功 → 投 false，不再多读
    }(readerIndex)
    readerIndex++
}
wg.Wait()
if p.canDecode(newBuf) {
    p.offset += p.shardSize
    if missingPartsHeal == 1 { return newBuf, errFileNotFound }    // ★ 带着 heal 信号返回
    if bitrotHeal == 1       { return newBuf, errFileCorrupt }
    return newBuf, nil
}
return nil, errErasureReadQuorum
```
`★ 这段代码为什么精妙 ─────────────────────────`
- **"成功投 false、失败投 true"是整个自平衡的灵魂**：channel 里 `true` 的数量 = "还需要再发起
  的读数量"。初始投 `dataBlocks` 个 `true` → 同时跑 `dataBlocks` 个读。每成功一个就投 `false`
  （净需求 -1），每失败一个就投 `true`（净需求不变，换一块盘再试）。循环在"凑够 `dataBlocks`
  个成功"或"盘用完"时停。**结果：永远只并行读刚好够用的盘数，坏盘被无缝顶替，好盘多了也不浪费读。**
- **慢盘的代价被冗余吸收**：如果某块盘慢（还没返回），它对应的 `true`/`false` 还没投进 channel，
  但其它已成功的读已经把 `newBuf` 填到 `canDecode` → 主循环 break，不等那块慢盘。**尾延迟被
  纠删码冗余天然吃掉**——这正是第 3 篇说的"冗余既抗故障也抗尾延迟"的代码出处。
- **heal 信号随数据一起返回**：用 `atomic` 标志 `missingPartsHeal`/`bitrotHeal` 记录"读的过程中
  发现了缺/坏"，即使成功凑齐也把对应 error 返回给上层 `Decode`/`getObjectWithFileInfo`，
  触发读时自愈（§3）。**重建成功和"需要修"两件事被同时上报**。
`──────────────────────────────────────────`

### `preferReaders`：把本地盘换到前面
```go
// :88
func (p *parallelReader) preferReaders(prefer []bool) {
    next := 0
    for i, ok := range prefer {
        if !ok || p.readers[i] == nil { continue }
        // 把第 i 个（preferred）reader 交换到第 next 个位置
        p.readers[next], p.readers[i] = p.readers[i], p.readers[next]
        p.readerToBuf[next], p.readerToBuf[i] = i, next
        next++
    }
}
```
- 因为触发器循环按 `readerIndex` 从 0 递增地"取下一块盘"，把本地盘换到数组前面，就让它们
  **被优先发起读**。`readerToBuf` 同步交换以保证分片落到正确的 buffer 槽位（重建要求分片
  顺序与逻辑位置对应）。

### stashBuffer：能复用就复用
```go
// :56
if globalBytePoolCap.Load().WidthCap() >= len(readers)*shardSize {
    b = globalBytePoolCap.Load().Get()        // 一整块池 buffer，切成 len(readers) 段
    for i := range bufs { bufs[i] = b[i*shardSize : (i+1)*shardSize] }
}
```
- 如果池子的单块容量够装下"所有 reader × shardSize"，就从池里借一整块切片复用；否则
  （老对象 blockSize 可能更大）退化为按需 `make`。又是一处热路径抠内存。

---

## 5. `Decode`：按 block 循环、重建、写出请求范围

```go
// :239
func (e Erasure) Decode(ctx, writer, readers, offset, length, totalLength, prefer) (written int64, derr error) {
    reader := newParallelReader(readers, e, offset, totalLength)
    if len(prefer) == len(readers) { reader.preferReaders(prefer) }
    defer reader.Done()

    startBlock := offset / e.blockSize
    endBlock   := (offset + length) / e.blockSize
    for block := startBlock; block <= endBlock; block++ {
        // 算出本 block 要写出的 [blockOffset, blockLength)（处理首尾不整块的 Range）
        ...
        bufs, err = reader.Read(bufs)                 // 并行读出 dataBlocks 个分片（可能带 heal 信号）
        if len(bufs) > 0 {
            if errors.Is(err, errFileNotFound) || errors.Is(err, errFileCorrupt) {
                if derr == nil { derr = err }          // ★ 记下 heal 信号，但不中断
            }
        } else if err != nil {
            return -1, err                             // 连重建都做不到 → 真失败
        }
        if err = e.DecodeDataBlocks(bufs); err != nil { return -1, err }   // RS 补齐缺失数据块
        n, err := writeDataBlocks(ctx, writer, bufs, e.dataBlocks, blockOffset, blockLength)  // 只写请求范围
        if err != nil { return -1, err }
        bytesWritten += n
    }
    if bytesWritten != length { return bytesWritten, errLessData }
    return bytesWritten, derr     // ★ 正常返回也可能带 derr（heal 信号）
}
```
`★ 细节 ────────────────────────────────────`
- **`DecodeDataBlocks` 只重建数据块、不验证校验块**（用 `ReconstructData`）。因为读路径已经
  靠 bitrot 逐分片校验过了，没必要再花成本验校验块。对应的 `Heal` 路径才用
  `DecodeDataAndParityBlocks`（`Reconstruct`，连校验块一起重建并验证）。
- **`writeDataBlocks` 处理 Range**：一个 block 重建出完整 blockSize 的数据，但客户端可能只要
  其中 `[blockOffset, blockLength)` 一段（断点续传/分段下载），只写这一段给 writer。
- **`derr` 双重身份**：返回值既是"是否真失败"（`return -1, err`）也是"成功但需要 heal"
  （`return bytesWritten, derr`）。上层 `getObjectWithFileInfo` 正是靠这个 `derr` 在
  `written == partLength` 时触发 MRF。
`──────────────────────────────────────────`

---

## 6. `streamingBitrotReader`：物理偏移换算与逐分片校验

读盘时必须把"逻辑分片偏移"换算成"盘上物理偏移"，因为每个分片前面有 32 字节哈希。

```go
// bitrot-streaming.go:161
func (b *streamingBitrotReader) ReadAt(buf []byte, offset int64) (int, error) {
    if offset % b.shardSize != 0 { return 0, errUnexpected }   // ★ 必须按 shardSize 对齐
    if b.rc == nil {                                            // 首次：打开流
        b.currOffset = offset
        streamOffset := (offset/b.shardSize)*int64(b.h.Size()) + offset   // ★ 物理偏移 = 逻辑偏移 + 前面所有哈希
        if len(b.data) == 0 && b.tillOffset != streamOffset {
            b.rc, err = b.disk.ReadFileStream(ctx, b.volume, b.filePath, streamOffset, b.tillOffset-streamOffset)
        } else {
            b.rc = io.NewSectionReader(bytes.NewReader(b.data), streamOffset, ...)   // inline：读内存
        }
    }
    if offset != b.currOffset { return 0, errUnexpected }       // ★ 只支持顺序读
    b.h.Reset()
    io.ReadFull(b.rc, b.hashBytes)     // 先读 32 字节哈希
    io.ReadFull(b.rc, buf)             // 再读分片数据
    b.h.Write(buf)
    if !bytes.Equal(b.h.Sum(nil), b.hashBytes) { return 0, errFileCorrupt }   // ★ 当场校验
    b.currOffset += int64(len(buf))
    return len(buf), nil
}
```
`★ 细节 ────────────────────────────────────`
- **物理偏移换算 `(offset/shardSize)*hashSize + offset`**：盘上每个 shard 前有个 `hashSize`
  （highwayhash256 = 32）字节的哈希。要读第 `k` 个 shard，前面有 `k` 个哈希共 `k*32` 字节，
  所以物理偏移 = 逻辑偏移 + 前面哈希总字节。`tillOffset` 在构造时也做了同样的换算（`:210`）。
- **只支持顺序读、要求对齐**：`offset != currOffset` 或不对齐都返回 `errUnexpected`（注释直言
  "Can never happen unless there are programmer bugs"）。bitrot 流是顺序消费的，随机读会破坏
  哈希校验的连续性。
- **inline 走内存 SectionReader**：`b.data` 非空（inline 对象的数据在 `xl.meta` 里）时，直接在
  内存字节上开 SectionReader，根本不碰磁盘。与 §2 的"inline 数据到手立刻 break"呼应——inline
  对象的整个读取都在内存完成。
- **`Close` 里 `DrainBody`**（`:155`）：非 inline 走 `ReadFileStream`（HTTP/grid 流），关闭前
  把剩余 body 读空，让底层网络连接能被**复用**而不是丢弃重建。
`──────────────────────────────────────────`

---

## 7. 一页纸总结读路径的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 读锁可提前释放（inline 数据已在内存 / 非 inline 写新 DataDir 不动旧的） | 读锁持有时间最短化，高并发读 |
| 2 | delete marker / 零字节 / 远端 tier / SSE-C 复制 多重短路 | 各种边界各有正确语义 |
| 3 | `getObjectFileInfo` 用 `done` channel 流式凑 quorum | 不等最慢盘，够了就返回 |
| 4 | inline 数据到手立刻 break | 小对象一次 `xl.meta` 读完成整个 GET |
| 5 | FastGetObjInfo vs 普通模式 | 延迟 vs 正确性的显式开关 |
| 6 | 逐盘 modTime/etag 复核，剔除不一致盘 | 不从旧/坏元数据的盘读数据 |
| 7 | `Distribution` 长度不符 → 拒绝 heal | 防人为篡改被"修"成损坏 |
| 8 | 悬挂对象读时顺手 GC | 清理崩溃残骸 |
| 9 | `parallelReader` 触发器通道：成功投 false、失败投 true | 自平衡并行读，坏盘无缝顶替，尾延迟被冗余吸收 |
| 10 | 本地盘 `prefer` 优先 | 省网络带宽与延迟 |
| 11 | heal 信号随数据一起返回（atomic 标志 + derr） | 重建成功与"需要修"同时上报，读时自愈 |
| 12 | `closeBitrotReaders` 不 defer | 避免 for 循环累积 fd |
| 13 | bitrot 物理偏移换算 + 顺序对齐 + 当场校验 | 每分片独立校验的物理实现 |
| 14 | inline 读走内存 SectionReader；非 inline `Close` 时 DrainBody | inline 全内存；流式复用连接 |

下一篇深读：**`xl.meta` 元数据格式**——`xlMetaV2` 的多版本容器、msgp 手写序列化、
inline 数据布局、版本增删（`AddVersion`/`DeleteVersion`）的就地合并逻辑。
