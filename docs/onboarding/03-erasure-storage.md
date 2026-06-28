# 03 · 存储引擎与纠删码

这一篇深入 MinIO 的"地基"：数据怎么被切块编码、`xl.meta` 元数据长什么样、bitrot 怎么防、
quorum 到底怎么算。如果你对纠删码不熟，先读 §3.1 的速成。

## 3.1 纠删码速成（30 秒理解 Reed-Solomon）

把一个对象切成 `M` 个**数据块**（data shard），再用线性代数算出 `K` 个**校验块**（parity shard），
一共 `M+K` 个块，分别写到 `M+K` 块盘上。

- **核心性质**：任意 `M` 个块（无论是数据块还是校验块）就能还原全部原始数据。
- **抗故障能力**：可以丢失任意 `K` 块盘而不丢数据。
- **空间开销**：`(M+K)/M`。例如 EC(8,4)：12 块存 8 块的数据，开销 1.5×，却能扛 4 盘故障；
  对比三副本开销 3× 只能扛 2 副本故障——EC 完胜。

MinIO 里 `M = dataBlocks`、`K = parityBlocks`，一个 Erasure Set 的盘数 = `M+K = setDriveCount`。
`K` 默认值可配（storage class），也会被"可用性优化"动态调高。

## 3.2 三层存储结构回顾 + Quorum 计算

三层（Pool / Set / Erasure）已在第 1 篇讲过职责。这里聚焦**底层 `erasureObjects` 的 quorum**：

- 结构：`cmd/erasure.go:47-70`，关键字段 `setDriveCount`、`defaultParityCount`、`getDisks()`、
  `nsMutex`（namespace 锁）。

### Write Quorum（写成功的门槛）
`cmd/erasure.go:85-91`（`defaultWQuorum`）：
```
dataCount = setDriveCount - parityCount
writeQuorum = dataCount
if dataCount == parityCount:          // 例如 EC(4,4)/setDrive=8
    writeQuorum = dataCount + 1        // +1 防"脑裂"：保证多数
```
> 例：8 盘 4 校验 → data=4, parity=4, 相等 → **writeQuorum = 5**。
> 例：16 盘 4 校验 → data=12, parity=4 → **writeQuorum = 12**。

### Read Quorum（能读的门槛）
`cmd/erasure.go:94-96`（`defaultRQuorum`）：`readQuorum = setDriveCount - parityCount = dataCount`。
直觉：要重建数据，至少得有 `dataCount` 块在。

`★ 易错点：两种 quorum 别混淆 ─────────────────`
- **"读元数据"的 quorum** 和 **"读数据块"的 quorum** 不是一回事。
  `objectQuorumFromMeta`（`cmd/erasure-metadata.go:531-565`）读 `xl.meta` 时，期望的元数据
  read quorum 取 `N/2`（能确认"最新版本"是什么）；而真正重建数据要 `dataCount` 个数据/校验块。
- 而且**每个对象的 parity 可能不同**（写入时的 storage class、或写入时坏盘多导致动态提高 parity），
  所以读时要从各盘 `xl.meta` 里**反推**这个对象当初用的 parity——这就是 `commonParity`
  （`cmd/erasure-metadata.go:461-498`）干的事：统计各盘记录的 parity，取"出现次数最多且满足
  `occ >= N-parity`"的那个。新人最容易在这里误以为 parity 是全局常量。
`──────────────────────────────────────────`

### Quorum 判定的实现：`reduceQuorumErrs`
`cmd/erasure-metadata-utils.go:137-146`。它的逻辑很优雅：
```
对所有盘返回的 error 做"投票"，找出出现次数最多的那个 err（含 nil 也算一种"结果"）
if maxCount >= quorum:  return 那个最常见的 err   // 可能是 nil = 成功
else:                   return quorumErr          // 凑不齐 → write/read quorum 错误
```
- `reduceWriteQuorumErrs` / `reduceReadQuorumErrs` 是它的两个特化（`:150` `:156`）。
- **精妙处**：把"多少盘成功"抽象成"哪个结果占多数"。如果多数盘返回同一个真实错误
  （比如 `errFileNotFound`），那它本身就是答案；只有当没有任何结果达到 quorum 时，才报
  "quorum 不足"。

## 3.3 PUT 的纠删码写路径（逐步）

入口 `erasureObjects.PutObject`（`cmd/erasure-object.go:1244`）。

**① 算 parity（不是常量！）** `:1287`
```go
parityDrives = globalStorageClass.GetParityForSC(userMeta["x-amz-storage-class"])
if parityDrives < 0: parityDrives = er.defaultParityCount
// 可用性优化：写入时若发现有盘离线，临时调高 parity，让对象更耐故障
if opts.MaxParity: parityDrives = len(disks)/2
else if globalStorageClass.AvailabilityOptimized():
    for each offline disk: parityDrives++
```

**② 算 writeQuorum** `:1323`（公式同 §3.2）。

**③ 为每块盘准备 FileInfo + Writer** `:1332-1412`
- 每块盘拿到一份 `FileInfo` 副本（记录它在条带里的 index、分布等）。
- 生成唯一 `DataDir`（一个 UUID 目录，放这个版本的 part 数据）。`:1341`
- 建 Reed-Solomon 编码器 `NewErasure(M, K, blockSize)`，blockSize 默认 1 MiB 量级。`:1360`
- 对每块盘建 writer：
  - **小对象 inline**：写进内存 buffer（最后塞进 `xl.meta`）。
  - **大对象**：`newBitrotWriter` 直接写盘上的临时文件。

**④ 流式编码 + 并行写** `erasure-encode.go:69-110`
```
erasure.Encode(ctx, src, writers, buf, writeQuorum):
  循环读 blockSize 大小的块:
    EncodeData(): Split 成 M 份 + Encode 出 K 份
    并行 writers[i].Write(shard_i)        # M+K 个 goroutine 同时写
  每写一块检查是否还满足 writeQuorum，掉太多盘就提前失败
```

**⑤ inline 决策** `:1388`
```go
if globalStorageClass.ShouldInline(shardFileSize(actualSize), versioned):
    // 数据直接内联进 xl.meta（默认阈值 ~128 KiB）
```

**⑥ 原子提交：renameData** `:1543`
- 把临时目录 `tmp/<uuid>/` 的 part + `xl.meta` **原子改名**到 `bucket/object/<DataDir>/`。
- 然后清理旧 `DataDir`（覆盖写时的老版本数据）。`commitRenameDataDir` `:1556`

`★ 设计洞察 ─────────────────────────────────`
- **"先写临时再原子改名"是崩溃一致性的关键**。如果写到一半进程挂了，临时目录里的半成品不会
  污染正式对象；只有 `renameData` 成功（且达 quorum）才算对象存在。这把"写对象"做成了近似
  原子的操作。
- **inline 小对象**避免了"一个 1KB 文件也要开 N+K 个独立文件 + 元数据文件"的元数据放大。
  小对象的数据和元数据合并成一个 `xl.meta`，大幅减少 inode 消耗和小文件 IOPS。这是 MinIO
  扛海量小对象的秘诀之一。
`──────────────────────────────────────────`

## 3.4 GET 的纠删码读路径（逐步）

入口 `erasureObjects.GetObjectNInfo`（`cmd/erasure-object.go:202`），核心解码在
`getObjectWithFileInfo`（`:309`）。关键在 `erasure-decode.go`：

```
Decode(ctx, writer, readers, offset, length, totalLength, prefer):   erasure-decode.go:239
  parallelReader 并行读各盘，但只需先到的 M 块有效分片
  对每个数据块:
    parallelReader.Read():
       - 优先读 prefer 标记的盘（通常是本地盘，省网络）
       - 凑齐 M 个有效 shard 就停（慢盘/坏盘自动被绕过）
    DecodeDataBlocks(): ReconstructData() 用 RS 数学补齐缺失的数据块
    writeDataBlocks(): 把重建出的原始字节写给 writer（→ io.Pipe → HTTP）
```

`★ 设计洞察 ─────────────────────────────────`
- **`parallelReader` 的"够了就停"**：它不等所有盘，先凑齐 `M` 块就重建。这意味着 N+K 里有
  K 块慢/坏，读延迟也几乎不受影响——冗余被用来吸收尾延迟，而不仅仅是抗故障。
- **`prefer` 本地优先**：分布式部署里，能从本机盘读就不走网络，省带宽降延迟。
`──────────────────────────────────────────`

## 3.5 `xl.meta`：每个对象的元数据格式（XL Format V2）

每个对象版本在每块盘上对应一个 `xl.meta` 文件。它的二进制格式（`cmd/xl-storage-format-v2.go`）：

### 文件头 `:42-71`
```
[ 'X' 'L' '2' ' ' ]            魔数 xlHeader（4 字节）
[ major(2) minor(2) ]         版本，当前 major=1 minor=3
[ bin32 长度 ][ msgpack 正文 ]
[ xxhash(正文) 的 CRC ]         校验元数据自身完整性
[ inline data ... ]           小对象的实际数据直接跟在后面
```

### 正文：`xlMetaV2`（多版本容器）`:904-914`
```go
type xlMetaV2 struct {
    versions []xlMetaV2ShallowVersion  // 这个对象的所有版本（versioning！）
    data     xlMetaInlineData          // inline 的小对象数据
    metaV    uint8
}
```
- **支持多版本**：一个对象可以有多个版本，每个版本是数组里一项；最新版在末尾（`IsLatest`）。
- 版本有三种类型（`VersionType`，`:104-114`）：
  - `ObjectType`：普通对象版本（`xlMetaV2Object`）
  - `DeleteType`：删除标记（`xlMetaV2DeleteMarker`，versioning 下的"软删除"）
  - `LegacyType`：旧 V1 格式对象（向后兼容）

### 一个对象版本：`xlMetaV2Object` `:155-175`
```go
type xlMetaV2Object struct {
    VersionID, DataDir   [16]byte    // 版本 UUID、数据目录 UUID
    ErasureAlgorithm     ErasureAlgo // ReedSolomon
    ErasureM, ErasureN   int         // 数据块/校验块数（每对象独立！）
    ErasureBlockSize     int64
    ErasureIndex         int         // 本盘在条带里的位置
    ErasureDist          []uint8     // 条带分布（写时打散的盘顺序）
    BitrotChecksumAlgo   ChecksumAlgo
    PartNumbers, PartSizes, PartETags, PartActualSizes ...  // 各 part 信息
    Size, ModTime        int64
    MetaSys              map[string][]byte   // 系统元数据（inline 标记、加密、复制状态…）
    MetaUser             map[string]string   // 用户自定义元数据
}
```

`★ 关键洞察：ErasureDist 与 ErasureIndex ──────`
- 写一个对象时，MinIO **不会**简单地"data 块都写前几块盘"。它按 `ErasureDist` 把分片打散到
  不同盘上（每个对象的分布不同），这样**数据块和校验块在盘间均匀分布**，没有哪块盘永远只存
  校验块。读的时候用 `shuffleDisksAndPartsMetadataByIndex`（第 2 篇提过）按 `ErasureDist`
  还原顺序。新人调试时如果直接 `ls` 盘上的目录，会发现分片顺序"乱"——这是设计，不是 bug。
`──────────────────────────────────────────`

### 序列化：MessagePack + xxhash
- `AppendTo`（`:1179-1234`）手写 msgpack：头 + 各版本头 + 各版本元数据 + xxhash CRC + inline data。
- 用 `msgp`（代码生成，见 `*_gen.go` 文件）做高性能序列化——这也是为什么仓库里有大量
  `xl-storage-format-v2_gen.go` 这类自动生成文件。

`★ 易错点 ─────────────────────────────────`
- **不要手改 `*_gen.go`**！它们由 `//go:generate msgp` 自动生成。要改结构体，改源 struct
  再重新 `go generate`，否则下次生成会覆盖你的改动，且容易造成序列化不一致。
- **版本号 major/minor 的语义**：major 变化是破坏性的（格式不兼容），minor 是兼容追加。改
  格式时改错这两个数会让老节点读不了新数据，或反之。
`──────────────────────────────────────────`

## 3.6 单块盘的实现：`xlStorage`

`StorageAPI` 的本地实现（`cmd/xl-storage.go`）。几个关键方法：

- **`CreateFile`**（`:2092`）→ `writeAllDirect`（`:2131`）：写一个 part 文件。
  - 用 **O_DIRECT**（绕过 page cache）写大文件，配合对齐 buffer 池（`ODirectPoolLarge/Small`）。
  - 校验写入字节数：少了 `errLessData`，多了 `errMoreData`。
- **`ReadFile`**（`:1868`）：读并可选 bitrot 校验（见 §3.7）。
- **`WriteMetadata`**（`:1464`）：写 `xl.meta`。
  - **首次写**（`fi.Fresh`）：新建 `xlMetaV2`，`AddVersion(fi)`，`AppendTo` 序列化，落盘。
  - **更新**：先 `ReadAll` 现有 `xl.meta`，`Load` 后 `AddVersion` 追加新版本，再落盘。

`★ 设计洞察：O_DIRECT ─────────────────────────`
- 对象存储的访问模式是"写一次、之后大多顺序读"，page cache 命中率低，反而会挤占内存、引入
  double buffering。O_DIRECT 让 MinIO 自己掌控 I/O，配合对齐 buffer 池减少 GC 压力。代价是
  对齐要求严格（offset/长度要按扇区对齐），所以才有 `CopyAligned` 和专门的 buffer 池。
`──────────────────────────────────────────`

## 3.7 Bitrot：对抗"静默数据损坏"

磁盘可能在你不知情时翻转某个 bit（bit rot）。MinIO 对**每个分片**都存校验和，读时验证。

- 默认算法：`HighwayHash256S`（`cmd/xl-storage-format-v1.go:158`）——为流式校验优化，比
  SHA256 快得多。
- **写时**：`streamingBitrotWriter`（`cmd/bitrot-streaming.go`）按 `shardSize` 分块，
  落盘格式是 `[hash][data][hash][data]...`，每块数据前面跟它的哈希。
- **读时**：`bitrotReader` 边读边重算哈希，和存的哈希比对，不符返回 `errFileCorrupt`
  （`cmd/bitrot.go:164-216` 的 `bitrotVerify`）。
- 校验信息存在 `xl.meta` 的 `Erasure.Checksums`（每 part 一条 `ChecksumInfo{PartNumber, Algorithm, Hash}`）。

`★ 设计洞察 ─────────────────────────────────`
- **bitrot 检测和纠删码重建是一对组合拳**：读到某块 corrupt → 当作"这块盘没有这块数据" →
  纠删码用其它块重建 → 后台 heal 把坏块修回去（第 6 篇）。所以静默损坏对用户是透明的：
  读不会返回坏数据，只会自动绕过并修复。
- **streaming（分块）vs whole（整文件）两种 bitrot writer**：大文件用 streaming，可以边写边
  校验、支持随机读某段时只校验涉及的块；小文件/特殊场景用 whole。
`──────────────────────────────────────────`

## 3.8 本篇要点回顾

| 概念 | 关键点 |
|------|--------|
| 纠删码 | EC(M,K)，任意 M 块可重建，省空间抗故障 |
| parity 非常量 | 每对象独立，受 storage class 与可用性优化影响，读时靠 `commonParity` 反推 |
| write quorum | `data`（data==parity 时 +1）；read quorum = `data` |
| quorum 判定 | `reduceQuorumErrs`：对各盘结果"投票"，多数即答案 |
| 写一致性 | 临时目录 + `renameData` 原子改名 |
| inline | 小对象数据塞进 `xl.meta`，省 inode 抗小文件 |
| xl.meta | 多版本容器，msgp 序列化，`ErasureDist` 打散分片 |
| O_DIRECT | 绕过 page cache，对齐 buffer 池 |
| bitrot | 每分片 highwayhash，读时校验，配合 EC 自动绕过 + heal 修复 |

下一篇进入"水平主轴"的分布式机制：节点间怎么通信、分布式锁怎么保证并发安全。
