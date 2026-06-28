# 深读 12 · Data Scanner 与生命周期逐层精读

> data scanner 是后台最关键的常驻 goroutine：一次遍历同时做**用量统计 + heal 采样 + 生命周期
> 评估 + 复制检查**。本篇拆开扫描节奏与动态限速、folderScanner 递归、heal 采样概率、缓存压缩
> 自平衡、`getSize` 即修复、ILM 搭车评估（`cmd/data-scanner.go`、`data-usage-cache.go`）。

---

## 1. 扫描节奏：周期 + 抖动 + 动态限速

```go
// :49 关键常量
dataScannerSleepPerFolder = time.Millisecond   // 每个文件夹之间睡 1ms
dataUsageUpdateDirCycles  = 16                  // 每 16 个周期访问一遍所有文件夹
dataScannerStartDelay     = 1 * time.Minute     // 启动延迟 & 周期间隔
healObjectSelectProb      = 1024                // heal 采样：1/1024 的对象被选中深查
// :66
scannerSleeper = newDynamicSleeper(2, time.Second, true)  // 动态睡眠器
// :74
func initDataScanner(ctx, objAPI) {
    go func() {
        for {
            runDataScanner(ctx, objAPI)
            duration := time.Duration(rng.Float64() * float64(scannerCycle.Load()))  // ★ 抖动
            if duration < time.Second { duration = time.Second }
            time.Sleep(duration)
        }
    }()
}
```
`★ 限速设计：不抢前台 I/O ───────────────────────`
- **`scannerSleeper`（dynamicSleeper）**：扫描每处理一个条目就可能睡一下。动态睡眠器根据当前
  负载调整睡眠时长——前台繁忙时多睡（让出 I/O），空闲时少睡（加快扫描）。`weSleep()` 还会判断
  是否处于 idle throttle 模式。**scanner 是"低优先级后台公民"，绝不和用户请求抢 IOPS。**
- **周期间抖动**（`rng.Float64() * scannerCycle`）：各节点/各盘的扫描周期错开，避免所有盘同时
  扫描造成 I/O 尖峰。
- **`runDataScanner` 用 `globalLeaderLock`**（深读概览第 6 篇）：全集群同一时刻只有一个 leader
  在驱动扫描，避免重复。
`──────────────────────────────────────────`

### 深扫 vs 普扫
```go
// :91 getCycleScanMode
// BitrotScanCycle == -1 → 永远 HealNormalScan（只查缺失，不验 bitrot）
// BitrotScanCycle == 0  → 永远 HealDeepScan（验 bitrot，贵）
// 否则：距上次 bitrot 深扫超过周期 → 这轮 HealDeepScan
```
- **bitrot 深扫很贵**（要读每个分片重算哈希），所以不是每轮都做，而是按 `BitrotScanCycle` 周期
  性触发。普扫只检查"分片在不在"，深扫才校验"分片内容对不对"。

---

## 2. folderScanner：递归扫描 + getSize 即修复

`scanFolder`（`:401`）递归遍历目录。对每个对象：
```go
// :490
wait := noWait
if f.weSleep() { wait = scannerSleeper.Timer(ctx) }   // 动态限速
item := scannerItem{Path, bucket, prefix, objectName, lifeCycle: activeLifeCycle, replication: replicationCfg}
item.heal.enabled = thisHash.modAlt(...) && f.shouldHeal()   // ★ heal 采样
item.heal.bitrot  = f.scanMode == HealDeepScan
sz, err := f.getSize(item)                            // ★ getSize 内部会顺带 heal！
...
delete(abandonedChildren, pathJoin(item.bucket, item.objectPath()))  // 找到了 → 从"失踪名单"移除
into.addSizes(sz); into.Objects++                    // 累加用量
```
`★ 一次遍历做四件事 ───────────────────────────`
- **`getSize(item)` 不只是"取大小"——它内部会评估 ILM、按采样做 heal、检查复制**。把"需要逐对象
  看一眼"的所有后台工作摊到这一次遍历里（深读概览第 6 篇的"搭车"经济学）。逐对象遍历很贵
  （IOPS），绝不为每件事各扫一遍。
- **`abandonedChildren`（失踪名单）**：上一轮缓存里有、但这一轮没扫到的对象，留在这个 map 里。
  扫完后这些"上次有这次没有"的对象会被排进 heal——它们要么真被删了，要么某盘缺了需要修。
  在循环里找到一个就 `delete` 一个，剩下的就是真失踪的。
`──────────────────────────────────────────`

### heal 采样概率：`modAlt` + 深目录降频
```go
// :508
item.heal.enabled = thisHash.modAlt(f.oldCache.Info.NextCycle/folder.objectHealProbDiv,
                                    f.healObjectSelect/folder.objectHealProbDiv) && f.shouldHeal()
// :670 深层目录把 objectHealProbDiv 设为 dataUsageUpdateDirCycles(16)
folder.objectHealProbDiv = dataUsageUpdateDirCycles
```
`★ 为什么是概率采样而非全量 heal ─────────────────`
- **不是每个对象每轮都 heal**（太贵），而是 **1/`healObjectSelectProb`（1024）** 的概率抽查。
  `modAlt` 根据对象哈希 + 当前周期决定这个对象这轮是否被选中——保证**长期下每个对象都会被轮到**
  （周期推进，被选中的对象集合滚动变化），又不在单轮造成 I/O 风暴。
- **深目录 `objectHealProbDiv` 降频**：越深的目录除以越大的因子，heal 检查更稀疏。因为深目录
  对象多、变化少，没必要和顶层同频率查。这是"按热度分配 heal 预算"。
`──────────────────────────────────────────`

---

## 3. 缓存压缩：让 data-usage 缓存不爆

扫描结果存进 `dataUsageCache`（每个目录一个 `dataUsageEntry`：size/objects/versions + 大小直方图）。
海量对象下这棵树会爆，所以要**压缩（compact）**：
```go
// 注释 :285 压缩触发条件
// 1) 分支(含子目录)对象数 < dataScannerCompactLeastObject(500)
// 2) 单目录子目录数 > dataScannerCompactAtFolders
// 3) 只有对象没有子目录
// 另：递归子项 > dataScannerCompactAtChildren(10000) → 递归压缩对象最少的分支直到达标
// bucket 根永不压缩
```
`★ 压缩 = 自平衡的缓存树 ───────────────────────`
- **压缩 = 把一棵子树坍缩成一个汇总条目**（只保留总量，丢弃子目录明细）。这让缓存大小有上界，
  不随对象数无限膨胀。
- **自平衡**（注释 :301）：扫描某分支时假设它会"解压"，若对象分布变了，重扫时会重新压缩到合适
  形态——这棵树会随数据分布动态 rebalance，小分支被压、热分支保持明细。
- **`forceCompact`（:594）**：子目录数超过强制阈值（`dataScannerForceCompactAtFolders=25万`）时
  强制压缩，并 log 警告"expect reduced scanner performance"——告诉运维这个 bucket 目录结构
  病态（太多目录），扫描会变慢。
- **bucket 根永不压缩**：根的汇总要随时可读（`mc admin info` 的容量来自它）。
`──────────────────────────────────────────`

### 跨盘合并
```go
// data-usage-cache.go:846  merge(other)
```
- 每块盘扫出一份缓存，`merge` 把多盘结果聚合成 bucket 级总量。这就是 `mc admin info` 容量的来源——
  **最终一致的采样快照，滞后一个扫描周期**（深读概览第 6 篇强调的"非实时账本"）。

---

## 4. 生命周期（ILM）评估：搭 scanner 的车

scanner 扫到对象时，`applyActions`（`:1038`）评估 ILM 规则：
```go
// 无 ILM 规则 → 只做 heal/replication 检查
// 有规则 → lifecycle.NewEvaluator().Eval(objOpts) 得到 action：
//   DeleteAction        → applyExpiryRule        过期删除
//   TransitionAction    → queueTransitionTask    转储到远端 tier
//   DeleteVersionAction → 加入待删队列            清理旧版本
```
- **ILM 不单独扫一遍**，而是复用 scanner 的这次遍历。`activeLifeCycle`（该 prefix 上是否有规则）
  和 `replicationCfg` 在进入目录时就查好，扫到对象顺带评估。
- **Transition（转储）**：冷数据搬到便宜的远端 tier（S3/Azure/GCS），本地只留指针（深读 03 的
  tier 元数据）。`transitionState` worker 池异步执行，`transitionCh` 容量 100000。
- **过期**：`expiryState` worker 池异步删除到期对象/旧版本。

`★ 细节：scanner 是后台服务的"调度中枢" ───────────`
- data scanner 不只是统计工具——它是 heal、ILM、复制补传（深读 11 的 MRF 兜底）的**共同触发器**。
  这些后台工作都"挂在"scanner 的遍历上：扫到一个对象 → 统计它 + 看要不要 heal + 看 ILM 到期没 +
  看复制了没。**理解 scanner，就理解了 MinIO 所有"逐对象后台维护"的入口。**
`──────────────────────────────────────────`

---

## 5. 一页纸总结 Scanner 的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | dynamicSleeper 动态限速 + weSleep idle throttle | scanner 不抢前台 IOPS |
| 2 | 周期抖动 + globalLeaderLock | 各盘扫描错峰，全集群单 leader |
| 3 | bitrot 深扫按周期触发（贵） | 普扫查缺失、深扫验内容，按需做 |
| 4 | getSize 内部即 heal/ILM/复制检查 | 一次遍历做四件事的搭车经济学 |
| 5 | abandonedChildren 失踪名单 → 排 heal | "上次有这次没有"的对象被修 |
| 6 | heal 概率采样 1/1024（modAlt） | 长期全覆盖又不造成单轮 I/O 风暴 |
| 7 | 深目录 objectHealProbDiv 降频 | 按热度分配 heal 预算 |
| 8 | 缓存压缩自平衡 + bucket 根不压 | 缓存大小有界，根汇总随时可读 |
| 9 | forceCompact + 警告 | 病态目录结构提示运维 |
| 10 | 跨盘 merge 成 bucket 总量 | `mc admin info` 容量来源，滞后一周期 |
| 11 | ILM 搭车评估（不单独扫） | 复用遍历，省 IOPS |
| 12 | scanner 是 heal/ILM/复制补传共同触发器 | 所有逐对象后台维护的入口 |

下一篇深读（系列收官）：**Healing 自愈**——MRF 队列与磁盘持久化、heal 序列、
`shouldHealObjectOnDisk` 判定、分片重建（`Erasure.Heal`）、换盘全量重建。
