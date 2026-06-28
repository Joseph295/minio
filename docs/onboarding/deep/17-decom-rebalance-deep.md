# 深读 17 · 退役（Decommission）与重平衡（Rebalance）逐层精读

> 深读 05 说过"迁移=逐对象重写"。本篇兑现这句话的代码：退役（清空一个 pool 再移除）和
> 重平衡（多 pool 间摊平使用率）如何**通过正常 PutObject 路径重写对象**、靠 `DataMovement`+
> `SrcPoolIdx` 让对象落到别的 pool、保留所有元数据、按版本顺序搬运、断点续传
> （`cmd/erasure-server-pool-decom.go` / `-rebalance.go`）。

---

## 1. 核心手法：搬运 = 读出来 + 重新写进去

`decommissionObject`（`:608`）和 `rebalanceObject`（`:841`）几乎一样——把对象**读出来再用
`PutObject` 重新写一遍**：
```go
// decommissionObject（普通对象路径，:673）
hr, _ := hash.NewReader(ctx, io.LimitReader(gr, objInfo.Size), objInfo.Size, "", "", actualSize)
z.PutObject(ctx, bucket, objInfo.Name, NewPutObjReader(hr), ObjectOptions{
    DataMovement: true,            // ★ 标记这是搬运写
    SrcPoolIdx:   idx,             // ★ 源 pool 索引（让目标选 pool 时排除它）
    VersionID:    objInfo.VersionID,
    MTime:        objInfo.ModTime,         // ★ 保留原修改时间
    UserDefined:  objInfo.UserDefined,     // ★ 保留用户元数据
    PreserveETag: objInfo.ETag,            // ★ 保留 ETag 保证元数据一致
    IndexCB:      func() []byte { return objInfo.Parts[0].Index },  // ★ 保留压缩索引
    NoAuditLog:   true,
})
```
`★ 为什么这样设计 ─────────────────────────────`
- **复用 ObjectLayer 的读写路径，没有专门的"块迁移协议"**（深读 05 已点出，这里是代码）。搬运
  天然继承纠删码、加密、版本、quorum 的所有保证——**搬运期间崩溃也安全**：重写没完成的对象，
  源位置还在，重试即可（呼应深读 01 的崩溃一致）。
- **`DataMovement: true` + `SrcPoolIdx`** 是搬运的方向阀：
  - `DataMovement` 让 `xl.meta` 打上 `SetDataMov` 瞬态标记（深读 03 §5：`AddVersion` 持久化时
    会跳过它，不污染元数据）。
  - `SrcPoolIdx` 告诉 `getAvailablePoolIdx`（深读 05 §3）**排除源 pool**，所以重写一定落到**别的
    pool**——这才实现了"搬出去"。
- **大量 `Preserve*`**：搬运必须保持对象**字节级一致**——ETag、VersionID、ModTime、UserDefined、
  压缩 Index 全部原样保留。否则搬完元数据变了，客户端会发现 ETag 变化、版本时间错乱。压缩 Index
  尤其关键：不保留它，搬运后的对象**解压会失败**。
`──────────────────────────────────────────`

### 多段对象特殊处理
```go
// :621 multipart 对象不能一次 PutObject，要重走 multipart 流程
res, _ := z.NewMultipartUpload(ctx, bucket, objInfo.Name, ObjectOptions{DataMovement: true, SrcPoolIdx: idx, ...})
for i, part := range objInfo.Parts {
    z.PutObjectPart(ctx, ..., part.Number, NewPutObjReader(hr), ObjectOptions{PreserveETag: part.ETag, IndexCB: ...})
}
z.CompleteMultipartUpload(ctx, ..., parts, ObjectOptions{DataMovement: true, MTime: objInfo.ModTime})
```
- 大对象按原 part 边界**逐 part 重传**，保持分段结构不变。每个 part 也保留 ETag 和压缩 Index。

---

## 2. 版本顺序与逐版本决策

```go
// decommissionEntry :817
fivs, _ := entry.fileInfoVersions(bi.Name)
versionsSorter(fivs.Versions).reverse()   // ★ 按 ModTime 升序（老→新）排
for _, version := range fivs.Versions {
    if filterLifecycle(...) { expired++; continue }   // ① ILM 已过期 → 跳过（让它被删而非搬）
    if version.Deleted && remainingVersions == 1 && rcfg == nil {
        continue                                       // ② 孤立删除标记 → 跳过（没东西可搬）
    }
    if version.Deleted {
        z.DeleteObject(ctx, ..., ObjectOptions{        // ③ 删除标记 → 在目标 pool 重建删除标记
            Versioned: true, VersionID: versionID, MTime: version.ModTime,
            DataMovement: true, DeleteMarker: true, SkipDecommissioned: true, SrcPoolIdx: idx})
    } else {
        z.decommissionObject(ctx, idx, bi.Name, gr)    // ④ 普通版本 → 重写
    }
}
```
`★ 逐版本搬运的几个讲究 ─────────────────────────`
- **`reverse()` 升序（老版本先搬）**：和深读 11 复制的"对象先于 delete marker"同一个不变量——
  版本要按时间顺序重建，否则目标 pool 的版本历史会错乱。这里建一个"从老到新的栈"。
- **全版本保留 `Versioned: true`**（注释 `:865`）：**无论 bucket 当前 versioning 配置如何**，被退役
  pool 上的**所有历史版本都必须保留搬走**。因为退役是物理迁移，不能因为"现在关了 versioning"就
  丢掉历史版本。
- **删除标记也要搬**（③）：删除标记是版本历史的一部分，用 `DeleteObject(DeleteMarker: true)` 在
  目标 pool 重建它，保持版本链完整。
- **ILM 过期的跳过搬运**（①）：马上要被生命周期删掉的对象，**搬它是浪费**——直接 enqueue 过期
  删除（`globalExpiryState.enqueueByDays`），不搬。这是"别给将死之人搬家"的优化。
- **`isDataMovementOverWriteErr` 被忽略**（`:881`）：搬运和**实时写入**可能竞争（用户正好覆盖了
  正在搬的对象）。这种"搬运被实时写覆盖"的错误被当作正常忽略——实时写的是更新版本，搬运的旧
  版本作废无所谓。
`──────────────────────────────────────────`

---

## 3. 列举待搬对象 + worker 并行

```go
// listObjectsToDecommission :711
listingQuorum := (set.setDriveCount + 1) / 2   // 版本须存在于约半数盘
listPathRaw(ctx, listPathRawOptions{
    disks: disks, recursive: true, minDisks: listingQuorum,
    agreed:  fn,                                          // 所有盘一致的条目直接处理
    partial: func(entries, _) { if e, ok := entries.resolve(&resolver); ok { fn(*e) } },  // 分歧条目按 quorum 解析
})
```
```go
// decommissionPool :747
workerSize := len(pool.sets)        // 每个 set 一个 decom worker
workerSize += len(pool.sets)        // ★ 再加每 set 一个 List worker
wk, _ := workers.New(workerSize)
```
- **并行度 = sets 数 × 2**：每个 set 一个搬运 worker + 一个列举 worker。列举和搬运流水线化——
  一边列对象一边搬，不必先列完再搬。
- **`listPathRaw` 的 agreed/partial**：列举跨盘进行，所有盘一致的条目走 `agreed`，有分歧的走
  `partial` 用 `metadataResolutionParams` 按 quorum 解析出权威版本（呼应深读 03 `mergeXLV2Versions`）。

---

## 4. Decommission vs Rebalance：异同

| | Decommission | Rebalance |
|---|---|---|
| 目的 | **清空**一个 pool（要移除它） | **摊平**多个 pool 的使用率（都保留） |
| 触发 | 管理员 `mc admin decommission start` | 加新（空）pool 后使用率失衡 |
| 谁参与 | 被退役的 pool（标 Suspended） | 使用率偏离均值的 pool |
| 搬完后 | pool 可下线 | 源对象删除以释放空间 |
| 状态文件 | `poolMeta` | `rebalance.bin` |

### Rebalance 的参与判定
```go
// rebalance.go:199
if pfi := AvailableSpace/TotalSpace; pfi < r.PercentFreeGoal {
    r.PoolStats[idx].Participating = true     // ★ 空闲率低于目标 → 参与（把数据搬出去）
}
```
`★ Rebalance 的目标：均衡空闲率 ─────────────────`
- `PercentFreeGoal` 通常取各 pool 空闲率的某个目标值。**空闲率低于目标的 pool（偏满）参与搬出**，
  数据搬到偏空的 pool，直到各 pool 空闲百分比接近。
- 注意是**百分比**而非绝对量——不同大小的 pool 比的是"满的程度"。一个 100TB 满 50% 和一个
  10TB 满 50% 的"健康度"一样。
- rebalance 搬完一个对象后会**删除源副本**（释放偏满 pool 的空间），这是和 decommission 的关键
  区别（decom 是最后整个 pool 移除，rebalance 是逐对象搬移并删源）。
`──────────────────────────────────────────`

---

## 5. 断点续传：状态持久化

- **decommission**：进度记在 `poolMeta`（`z.poolMeta.CountItem(idx, size, failure)`，`:893`），
  持久化到磁盘。重启后从断点继续，不重搬已搬的。
- **rebalance**：状态记在 `rebalance.bin`（`rebalMeta`，`saveRebalanceStats`），含每个 pool 的
  参与状态、已处理 bucket、统计。`nextRebalBucket` 取下一个待处理 bucket。
- 两者都和深读 14 batch job 的检查点是**同一种思想**：长时间运行的搬运任务必须可恢复，进度落盘。

---

## 6. 一页纸总结退役/重平衡的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 搬运 = 读出来 + PutObject 重写 | 复用读写路径，继承所有保证，崩溃安全 |
| 2 | DataMovement + SrcPoolIdx 方向阀 | 排除源 pool，对象落到别处 |
| 3 | 大量 Preserve（ETag/VID/MTime/Index） | 搬运保持字节级一致，压缩 Index 不丢则可解压 |
| 4 | multipart 逐 part 重传 | 保持分段结构 |
| 5 | reverse() 老版本先搬 | 版本顺序不变量（同复制） |
| 6 | 全版本保留（Versioned:true 不看配置） | 物理迁移不丢历史版本 |
| 7 | 删除标记也重建搬运 | 版本链完整 |
| 8 | ILM 过期对象跳过搬运 | 别给将死对象搬家，直接 enqueue 过期 |
| 9 | isDataMovementOverWriteErr 忽略 | 搬运与实时写竞争时让实时写赢 |
| 10 | worker = sets×2（搬运+列举流水线） | 列举与搬运并行 |
| 11 | listPathRaw agreed/partial + quorum | 跨盘列举按 quorum 解析权威版本 |
| 12 | decom 清空移除 vs rebalance 摊平保留 | 两种数据移动的不同目的 |
| 13 | rebalance 均衡空闲百分比、搬后删源 | 比"满的程度"，真正释放偏满 pool |
| 14 | poolMeta/rebalance.bin 断点续传 | 长任务可恢复（同 batch 检查点） |

下一篇深读：**Bucket Lifecycle（ILM）**——规则求值引擎、过期/转储 action 判定、tier 驱动抽象、
与 scanner 的协作触发。
