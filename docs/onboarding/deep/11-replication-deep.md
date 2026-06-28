# 深读 11 · Bucket 复制逐层精读

> 复制把一个 bucket 的对象**异步**搬到另一个（或多个）目标。本篇拆开 worker 池分级与自动扩缩、
> 非阻塞入队 + MRF 磁盘溢出、复制状态机、多目标并发、resync 全量补传，以及"对象先于 delete
> marker"的时序不变量（`cmd/bucket-replication.go`）。

---

## 1. 异步模型：PUT 不等复制

写一个对象时，PUT 路径只把它**标记为待复制**（写 `replication-status=PENDING` 元数据）并入队，
**立即返回成功**。真正的复制由后台 worker 池异步完成。这样复制延迟/目标故障不拖累前台写入。

---

## 2. 入队：分级 + 非阻塞 + MRF 溢出 + 自动扩缩

`queueReplicaTask`（`:2197`）是入队核心，密度极高：
```go
// ① 大对象（≥ minLargeObjSize，128MiB）→ 静态大 worker 池，按 bucket+name 哈希分配
if ri.Size >= int64(minLargeObjSize) {
    h := xxh3.HashString(ri.Bucket + ri.Name)
    select {
    case p.lrgworkers[h%len(p.lrgworkers)] <- ri:    // 入大 worker 通道
    default:                                          // ★ 满了 → 落 MRF + 可能扩容
        p.queueMRFSave(ri.ToMRFEntry())
        if p.ActiveLrgWorkers() < maxLWorkers { p.ResizeLrgWorkers(existing+1, existing) }
    }
    return
}
// ② 普通对象：heal/existing 复制 → mrfReplicaCh；新对象 → getWorkerCh（按名哈希选 worker）
switch ri.OpType {
case HealReplicationType, ExistingObjectReplicationType: ch = p.mrfReplicaCh; healCh = p.getWorkerCh(...)
default:                                                  ch = p.getWorkerCh(ri.Name, ri.Bucket, ri.Size)
}
select {
case healCh <- ri:
case ch <- ri:
default:                                                  // ★ 所有通道满 → 落 MRF + 按优先级处理
    globalReplicationPool.Get().queueMRFSave(ri.ToMRFEntry())
    switch prio {
    case "fast": 警告"跟不上流量"
    case "slow": 警告"建议提优先级"
    default /*auto*/:                                      // ★ 自动扩 worker
        if p.ActiveWorkers() < maxWorkers { p.ResizeWorkers(len(p.workers)+1, existing) }
        if p.ActiveMRFWorkers() < maxMRFWorkers { p.ResizeFailedWorkers(...) }
    }
}
```
`★ 入队设计的四层精妙 ─────────────────────────`
- **大对象单独通道**：≥128MiB 的大对象走静态大 worker 池，不和小对象抢普通 worker——否则一个
  大对象复制几分钟会堵死后面一堆小对象。按 `bucket+name` 哈希分配，保证同一对象的任务稳定落到
  同一 worker。
- **非阻塞 `select { case ch<-ri: default: }`**：入队**永不阻塞前台**。通道满了立刻走 default
  分支落 MRF（磁盘），而不是卡住调用方。前台写入的吞吐不被复制速度拖累。
- **MRF 是溢出缓冲**：跟不上时溢出到磁盘持久化队列，稍后重试（§4）。**不丢任务**（除非超重试上限）。
- **auto 优先级自动扩 worker**：默认 `auto` 模式下，发现跟不上就动态加 worker（到 `WorkerMaxLimit=500`）；
  `fast`/`slow` 是固定档位，只打警告提示用户调配置。**复制吞吐能随负载自适应**。
`──────────────────────────────────────────`

---

## 3. 多目标并发复制

`replicateObject`（`:1032`）：一个对象可配置复制到多个目标（ARN）。
```go
// 读 bucket 复制配置 → 筛出该对象要复制的目标 ARN 列表
// 为每个目标起独立 goroutine 并发复制（ri.replicateObject / ri.replicateAll per target）
// 用 replicatedInfos 聚合各目标结果 → 回写对象的 replication-status 元数据
```
- **每个目标一个 goroutine 并发**：复制到 3 个站点不是串行 3 次，而是并发。
- **聚合后回写状态**：所有目标都成功 → `COMPLETED`；部分失败 → 记录每个目标的状态。

---

## 4. 复制状态机（存在对象元数据里）

复制状态以系统元数据存在对象的 `xl.meta`（深读 03 的 MetaSys）：
- `replication-status`：`PENDING` / `COMPLETED` / `FAILED` / `REPLICA`。
- `replica-status`：标记"我是从别处复制过来的副本"（防止复制回环——副本不再被复制回源）。
- `replication-timestamp` / `replica-timestamp`：时间戳。

`★ 细节：REPLICA 标记防回环 ───────────────────`
- 双向复制下，A→B 复制过去的对象在 B 上带 `REPLICA` 标记。B 看到这个标记就知道"这是复制来的，
  别再复制回 A"——否则 A↔B 会无限互相复制同一个对象。这个标记是双向复制不死循环的关键。
`──────────────────────────────────────────`

---

## 5. MRF：失败重试的磁盘持久化队列

MRF（Most Recently Failed）是复制的"重试兜底"。

### 累积 + 定期/阈值刷盘 `persistMRF`
```go
// :3500
entries := make(map[string]MRFReplicateEntry)        // ★ 按 versionID 去重
mTimer := time.NewTimer(mrfSaveInterval)
for {
    select {
    case <-mTimer.C:        saveMRFToDisk(); mTimer.Reset(...)   // 定期刷
    case e := <-p.mrfSaveCh: entries[e.versionID] = e            // 收到新条目
                             if len(entries) >= mrfMaxEntries { saveMRFToDisk() }  // 满阈值刷
    case <-p.ctx.Done():    saveMRFToDisk(); return              // 关机尽量保存
    }
}
// saveMRFToDisk：先 queueMRFHeal（把这些条目排进 heal 重试）再写盘
```

### 入队 + 重试上限 `queueMRFSave`
```go
// :3547
if entry.RetryCount > mrfRetryLimit {                // ★ 重试超限 → 丢弃，交给 scanner
    atomic.AddUint64(&p.stats.mrfStats.TotalDroppedCount, 1); return
}
select {
case p.mrfSaveCh <- entry:                           // 入队
default:                                              // 队列满 → 丢弃 + 计数
    atomic.AddUint64(&p.stats.mrfStats.TotalDroppedCount, 1)
}
```
`★ MRF 的几个设计 ─────────────────────────────`
- **按 versionID 去重累积**：同一个对象版本反复入 MRF 只保留一份，避免重复重试。
- **定期 + 阈值双触发刷盘**：`mrfSaveInterval` 时间到、或攒够 `mrfMaxEntries` 条就落盘
  （msgp 格式，带 format/version 头，`persistToDrive` `:3572`）。重启后重新加载接着重试。
- **重试上限 → 丢弃 → scanner 兜底**：一个对象重试超过 `mrfRetryLimit` 次还失败，就从 MRF 丢掉
  （计入 `TotalDroppedCount`）。**不是放弃复制，而是降级**——交给 data scanner（深读 12）在
  下一轮全盘扫描时重新发现"这个对象没复制"并重新排队。MRF 是"快速重试通道"，scanner 是"慢速
  全量兜底"。两层保证最终一致。
- **队列满也丢弃 + 计数**：MRF 队列本身满了也丢（非阻塞），但 `TotalDroppedCount` 让运维能监控
  "复制是否在丢任务"。丢的最终也由 scanner 补。
`──────────────────────────────────────────`

---

## 6. Resync：给已有数据的 bucket 加复制规则后补传

给一个**已经有存量对象**的 bucket 新加复制规则时，存量对象需要补传——这是 resync
（`replicationResyncer`，`:2834` 附近）。
```go
// resyncBucket：用 objectAPI.Walk() 遍历所有对象的所有版本，逐个判断是否需要重新复制
```
`★ 时序不变量："对象先于 delete marker" ───────────`
- `Walk` **按版本升序（从老到新）遍历**。这是为了保证**一个对象的内容版本先于它的 delete marker
  被复制**。
- 想象 versioning 下：v1（创建）→ v2（删除标记）。如果先复制 v2（删除标记）再复制 v1，目标端会
  先看到"删除"、再看到"创建"——状态错乱，目标上这个对象会"复活"或状态不一致。**升序遍历保证
  目标端看到的版本顺序与源端一致**。
- 这个不变量在改任何复制/resync 遍历逻辑时**极易被破坏**：一旦改成降序或并发乱序，就会出现
  "删除标记先到"的诡异 bug。深读概览第 6 篇也强调过这条。
`──────────────────────────────────────────`

---

## 7. 一页纸总结 Bucket 复制的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 异步：PUT 标记 PENDING 即返回 | 复制延迟/目标故障不拖累前台写 |
| 2 | 大对象（≥128MiB）单独 worker 池 | 大对象不堵死小对象 |
| 3 | 入队非阻塞 `select{...default:}` | 前台永不被复制速度阻塞 |
| 4 | 通道满 → 溢出 MRF（磁盘） | 跟不上时不丢任务，落盘重试 |
| 5 | auto 优先级自动扩 worker（→500） | 复制吞吐随负载自适应 |
| 6 | 多目标并发（每目标一 goroutine） | 多站点复制不串行 |
| 7 | REPLICA 标记 | 双向复制防无限回环 |
| 8 | MRF 按 versionID 去重累积 | 同版本反复失败只留一份 |
| 9 | MRF 定期+阈值刷盘，重启重载 | 不丢重试任务 |
| 10 | 重试超限 → 丢弃 → scanner 兜底 | 快速重试 + 慢速全量两层最终一致 |
| 11 | 丢弃计入 TotalDroppedCount | 运维可监控复制健康 |
| 12 | resync 用 Walk 升序遍历 | 保证"对象先于 delete marker"复制 |

下一篇深读：**Data Scanner 与生命周期**——folderScanner 递归扫描、data-usage 三层缓存合并、
heal 采样概率、ILM 评估搭车、限速与目录压缩。
