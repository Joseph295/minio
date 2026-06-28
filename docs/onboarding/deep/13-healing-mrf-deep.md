# 深读 13 · Healing 自愈逐层精读（系列收官）

> MinIO 把"盘会坏、写会半途失败、换盘要重建"当常态。自愈让坏数据自己修好，对用户透明。
> 本篇拆开 MRF 队列与磁盘持久化、heal 判定 `shouldHealObjectOnDisk`、`healObject` 的 outdated
> 盘识别与可修性判断、`Erasure.Heal` 分片重建（`cmd/erasure-healing.go`、`cmd/mrf.go`、
> `cmd/erasure-decode.go`）。这是源码精读系列的收官篇。

---

## 1. 三条 heal 触发路径（回顾 + 落点）

1. **读时发现**（深读 02 §3）：GET 读到 bitrot/缺块 → 纠删码当场绕过返回正确数据 → `addPartialOp`
   把"这个对象需要修"扔进 MRF。用户无感。
2. **scanner 采样**（深读 12 §2）：扫描时按 1/1024 概率抽查 + `abandonedChildren` 失踪名单 →
   发现不一致就排 heal。
3. **写后发现**（深读 01 §7）：写入时盘离线/版本分歧 → MRF。
4. **手动/换盘**：`mc admin heal`，或换盘后后台全量重建。

---

## 2. MRF：待修队列的磁盘持久化

```go
// mrf.go
type PartialOperation struct {
    Bucket, Object, VersionID string
    Versions    []byte         // 版本分歧时要修的版本集
    SetIndex, PoolIndex int    // 精确定位到哪个 set
    Queued      time.Time
    BitrotScan  bool           // ★ 是否需要 bitrot 深扫（"坏块"而非"缺文件"）
}
type mrfState struct {
    opCh chan PartialOperation   // 容量 100000
    ...
}
```
- **`opCh` 容量 100000**：读/写/扫描发现的待修操作投进来，后台 worker 消费去 heal。
- **关机持久化**：`mrfState` 在 shutdown 时把队列**序列化落盘**到
  `.minio.sys/buckets/.heal/mrf/list.bin`（带 format+version 头）。**重启后重新加载接着修**——
  待修任务不因重启丢失。
- **`BitrotScan` 标志**：区分"文件不见了"（补回去）和"文件在但内容坏了"（需 bitrot 深扫确认坏在
  哪）。深读 02 §3 的读时自愈正是据此设置它。

`★ MRF vs scanner：快慢两条修复线 ───────────────`
- **MRF 是"快速精确修复"**：读/写当场发现问题，带着精确坐标（bucket/object/version/set/pool）
  立刻排队，秒级响应。
- **scanner 是"慢速全量兜底"**：周期性全盘扫，捞起 MRF 漏掉的、丢弃的（深读 11 §5 重试超限丢给
  scanner）、以及从未被访问过的冷对象。
- 两者合起来：热数据问题秒修，冷数据问题终会被扫到。**这就是"自愈是一等公民"的双线保障。**
`──────────────────────────────────────────`

---

## 3. 判定要不要修：`shouldHealObjectOnDisk`

对**每块盘**判断"这个对象在这块盘上要不要修"（`erasure-healing.go:179`）：
```go
func shouldHealObjectOnDisk(erErr error, partsErrs []int, meta, latestMeta FileInfo) (bool, bool, error) {
    if errors.Is(erErr, errFileNotFound|errFileVersionNotFound|errFileCorrupt) {
        return true, true, erErr                  // 文件/版本缺失或 xl.meta 损坏 → 修（含元数据）
    }
    if erErr == nil {
        if meta.XLV1 { return true, true, errLegacyXLMeta }       // 旧 V1 格式 → 总是修（升级）
        if !latestMeta.Equals(meta) { return true, true, errOutdatedXLMeta }  // 元数据落后于多数派 → 修
        if !meta.Deleted && !meta.IsRemote() {
            for _, partErr := range partsErrs {
                if partErr == checkPartFileNotFound { return true, false, errPartMissing }  // part 缺失 → 修(仅数据)
                if partErr == checkPartFileCorrupt { return true, false, errPartCorrupt }   // part 损坏 → 修(仅数据)
            }
        }
        return false, false, nil                  // 一切正常 → 不修
    }
    return false, false, erErr
}
```
- 返回 `(shouldHeal, needsMetaHeal, reason)`。`needsMetaHeal=true` 表示连 `xl.meta` 都要重写；
  part 缺失/损坏则 `xl.meta` 没问题、只补数据。
- **元数据落后（`!latestMeta.Equals(meta)`）也要修**：某盘的 `xl.meta` 比多数派旧（少了某个版本），
  把最新的 meta 写过去。这修复深读 01/04 里"版本分歧"留下的不一致。

---

## 4. `healObject`：识别 outdated 盘 + 判可修性

```go
// erasure-healing.go:296（核心）
// 1. 读所有盘的 xl.meta → 选出 latestMeta（quorum 权威版本）
// 2. 逐盘判定
outDatedDisks := make([]StorageAPI, len(storageDisks))
disksToHealCount, xlMetaToHealCount := 0, 0
for i := range onlineDisks {
    yes, isMeta, reason := shouldHealObjectOnDisk(errs[i], dataErrsByDisk[i], partsMetadata[i], latestMeta)
    if yes { outDatedDisks[i] = storageDisks[i]; disksToHealCount++; if isMeta { xlMetaToHealCount++ } }
}
if disksToHealCount == 0 { return }              // 都好 → 没活干

// 3. ★ 可修性判断：要修元数据的盘数 > parity → 修不了（凑不齐 quorum 的好元数据）
cannotHeal := !latestMeta.XLV1 && !latestMeta.Deleted && xlMetaToHealCount > latestMeta.Erasure.ParityBlocks
if cannotHeal && quorumETag != "" {              // ETag 都一致 → 给个机会再试
    cannotHeal = false
}
// 4. 逐 part 检查：某 part 失败盘数 > parity → 那个 part 重建不了
for _, partErrs := range dataErrsByPart {
    if countPartNotSuccess(partErrs) > latestMeta.Erasure.ParityBlocks { /* 该 part 不可修 */ }
}
```
`★ 可修性的边界 ───────────────────────────────`
- **`xlMetaToHealCount > parityBlocks` → 无法修**：如果坏掉元数据的盘超过 parity 数，意味着好的
  元数据不足 read quorum，无法确定权威版本——这种对象会交给 dangling 逻辑（`isObjectDangling`，
  `:990`）判断是否删除。
- **ETag 一致的"救一把"**：即使元数据看起来不够，但所有盘的 ETag 都相同（内容其实一致），就放宽
  `cannotHeal`，尝试修复。这是一处"宁可多救不轻易判死"的容错。
- **逐 part quorum**：多 part 对象里某个 part 的失败盘超过 parity，那个 part 重建不了——精确到
  part 级别判断可修性。
`──────────────────────────────────────────`

---

## 5. 分片重建：`Erasure.Heal`

确定要修后，用 `Erasure.Heal`（`erasure-decode.go:317`，深读 02 读过）重建：
```go
func (e Erasure) Heal(ctx, writers, readers, totalLength, prefer) error {
    reader := newParallelReader(readers, e, 0, totalLength)   // 从好盘并行读
    for block := range blocks {
        bufs, _ := reader.Read(bufs)                          // 凑齐 dataBlocks 个分片
        e.DecodeDataAndParityBlocks(ctx, bufs)                // ★ Reconstruct：重建数据+校验块并验证
        w := multiWriter{writers, writeQuorum: 1, ...}        // ★ writeQuorum=1：只写 outdated 盘
        w.Write(ctx, bufs)                                    // 把重建的分片写回坏盘
    }
}
```
`★ heal 重建的两个关键区别 ─────────────────────`
- **用 `DecodeDataAndParityBlocks`（`Reconstruct`）而非 `DecodeDataBlocks`（`ReconstructData`）**：
  普通读只重建数据块（深读 02 §5），heal 要**连校验块一起重建并验证**——因为目的是把坏盘上的
  分片（可能是数据块也可能是校验块）补全，且要确保重建结果自洽。
- **`writeQuorum=1`**：复用 `multiWriter`（深读 01 §6），但 writeQuorum 设 1——因为只往
  `outDatedDisks`（坏盘）写，写一块算一块成功，不要求多数。深读 01 §6 那条注释
  "HealFile uses writeQuorum=1" 的落点就在这里。
- **`writers` 只对应 outdated 盘**：好盘不动，只把重建出的分片写到需要修的盘。
`──────────────────────────────────────────`

---

## 6. heal 期间的瞬态标记（呼应深读 03）

```go
// erasure-healing.go:209
xMinIOHealing = ReservedMetadataPrefix + "healing"    // 标记"此对象正在被 heal"
xMinIODataMov = ReservedMetadataPrefix + "data-mov"   // 标记"正在 decom/rebalance 搬运"
```
- `SetHealing()` / `SetDataMov()` 在 heal/搬运时给 `FileInfo` 打标记，让下游知道这是 heal/搬运
  写入（比如 RenameData 据此走不同分支，深读 04 §1 的 `healing := fi.Healing()`）。
- **深读 03 §5 的伏笔在此收束**：`AddVersion` 持久化版本时**显式跳过** `xMinIOHealing`/`xMinIODataMov`
  ——它们是流程内传递的瞬态信号，绝不能固化进对象的持久元数据。heal 标记只在"这次 heal 操作"
  期间有意义，写进 `xl.meta` 会污染对象状态。

---

## 7. 换盘全量重建

换上一块新盘后，后台 heal（`global-heal.go` 的 `newBgHealSequence`，开启 `healDeleteDangling`）
会遍历**本该落在这块盘上的所有对象**，逐个用 `Erasure.Heal` 把缺失的分片重建到新盘。
- 本质就是对每个对象执行 §4–§5：新盘对该对象而言就是一块"缺分片的 outdated 盘"，重建补上即可。
- **复用对象级 heal**，没有专门的"盘级迁移协议"——和深读 05 的 decom/rebalance"逐对象重写"
  是同一种"复用现有路径"的哲学。

---

## 8. 一页纸总结 Healing 的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 三/四条触发：读时、scanner、写后、换盘 | 多入口发现问题 |
| 2 | MRF opCh 容量 10万 + 关机持久化 + 重启重载 | 待修任务不因重启丢失 |
| 3 | BitrotScan 区分"缺文件"vs"坏块" | 修复策略不同 |
| 4 | MRF 快速精确 + scanner 慢速兜底 | 热数据秒修、冷数据终被扫到 |
| 5 | shouldHealObjectOnDisk 判定树 | 缺失/损坏/落后/legacy/part 缺损分别处理 |
| 6 | 元数据落后也修 | 修复版本分歧不一致 |
| 7 | xlMetaToHealCount > parity → 不可修 | 好元数据不足 quorum 交给 dangling |
| 8 | ETag 一致"救一把" | 宁可多救不轻易判死 |
| 9 | 逐 part quorum 判可修性 | 精确到 part 级 |
| 10 | Erasure.Heal 用 Reconstruct（含校验块） | 补全坏盘任意分片并自洽验证 |
| 11 | writeQuorum=1 只写坏盘 | 复用 multiWriter，写一块算一块 |
| 12 | heal/datamov 瞬态标记 AddVersion 跳过 | 不污染持久元数据（呼应深读 03） |
| 13 | 换盘 = 逐对象 heal，无盘级协议 | 复用对象级路径的一贯哲学 |

---

## 系列收官：13 篇精读串成一句话

```
写入(01) 把数据切块编码、临时区落盘、原子改名提交；
读取(02) 自平衡并行读、缺块当场重建、读时顺手排修；
元数据(03) 多版本容器、惰性反序列化、自带 CRC；
单盘(04) O_DIRECT、fsync、提交顺序保证崩溃一致；
路由(05) 三次哈希定位、扩容加新池、迁移即重写；
———— 以上是「数据怎么在一台/一组盘上安全地存取」————
grid(06) 一对节点一条多路复用连接承载所有远端调用；
锁(07)  quorum 加锁 + 续约 + 回滚，无外部协调器的互斥；
———— 以上是「多节点怎么协同」————
签名(08) HMAC 重算 + 常时间比对认证「你是谁」；
IAM(09)  缓存 + singleflight + 策略合并评估「你能做什么」；
加密(10) 信封加密 + 上下文绑定保「数据怎么保密」;
———— 以上是「安全闭环」————
复制(11) 异步多目标 + MRF 溢出 + resync 时序不变量；
扫描(12) 一次遍历统计 + 采样 heal + ILM 搭车；
自愈(13) 双线触发 + 分片重建，让坏数据自己修好。
———— 以上是「后台如何持续维护数据」————
```

读完这 13 篇 + 7 篇概览，你对 MinIO 的理解应当已经达到"亲手通读过核心源码"的程度。
剩下的，是打开任意一个你还没细读的文件（`batch-handlers.go`、`site-replication.go`、
`metrics-v2.go`……），用同样的方法——**追调用链、问为什么这么设计、找不变量**——把它读透。
