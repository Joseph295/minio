# 06 · 数据服务：复制、生命周期、扫描、自愈

前五篇讲的是"请求路径上"的同步逻辑。本篇讲"请求路径外"的后台数据服务——它们是常驻
goroutine + 任务队列，默默维护数据的分布、生命周期、用量统计和健康。

先看全景：MinIO 启动后拉起的后台 goroutine（`cmd/server-main.go:1018-1099`）。

```
serverMain 末段拉起的常驻后台任务：
  ├─ Data Scanner            initDataScanner            扫描：统计用量 + 触发 heal/lifecycle
  ├─ Replication Pool        initBackgroundReplication  bucket 复制 worker + MRF
  ├─ ILM Expiry workers      initBackgroundExpiry       对象过期删除
  ├─ Transition workers      globalTransitionState.Init 对象转储到远端 tier
  ├─ Tier Config Manager     globalTierConfigMgr.Init   远端分层后端连接
  ├─ Bucket Metadata Sys     globalBucketMetadataSys.Init  bucket 配置内存缓存
  ├─ Site Replication        globalSiteReplicationSys.Init 站点间 heal routine
  └─ Batch Jobs              globalBatchJobsMetrics.init  批量任务
```

`★ 贯穿全篇的设计：单 Leader + 任务队列 + 磁盘溢出 ──`
- **单 Leader**：扫描、站点复制 heal 这类"全集群只该有一个在跑"的任务，用 `globalLeaderLock`
  保证整个集群同一时刻只有一个节点执行。
- **任务队列 + worker 池**：复制、过期、转储都是"生产任务 → 入 channel → worker 消费"。
- **磁盘溢出**：队列满了不丢，溢出的任务持久化到磁盘（MRF），稍后重试。
这三点是所有后台服务的共同骨架，下面每个服务都是它的变体。
`──────────────────────────────────────────`

## 6.1 Bucket 复制（异步、多目标、可重试）

把一个 bucket 的对象异步复制到另一个（或多个）目标。核心在 `cmd/bucket-replication.go`。

### Worker 池：`ReplicationPool`（`:1842`）
- 三类 worker：
  - 普通 worker（`workers`）：常规对象。
  - 大文件 worker（`lrgworkers`，固定 10 个）：≥ 128MiB 的大对象单独通道，避免大文件堵住小文件。
  - MRF worker（`mrfReplicaCh`，容量 100000）：失败重试。
- 优先级档位（`:1900-1955`）：`fast`(500) / `slow`(50) / `auto`(100，默认)。

### 复制流程（`replicateObject` `:1032`）
```
1. 读 bucket 复制配置，按对象元数据筛出目标 ARN（可多目标）
2. 为每个目标起独立 goroutine 并发复制（:1107）
3. 等所有目标完成，聚合结果，回写对象的 replication-status 元数据
```
- 复制状态记在对象元数据里（`:62-81`）：`replication-status`、`replication-timestamp`、
  `replica-status`（标记"我是别人复制过来的副本"）等。

### 失败重试：MRF（Most Recently Failed）
- 队列满或 worker 忙 → `queueMRFSave()` 把任务**持久化到磁盘**（`.minio.sys/buckets/.heal/mrf/`）。
- `processMRF()` 后台消费，`persistMRF()` 定期落盘。重启也不丢失待重试任务。

### 重新同步：`replicationResyncer`（`:2834`）
- 当你给一个已有数据的 bucket **新加**复制规则时，存量对象需要补传。
- `resyncBucket` 用 `objectAPI.Walk()` **按升序**遍历所有版本逐个补传。

`★ 易错点：复制顺序与 delete marker ────────────`
- resync/复制必须保证**对象先于它的 delete marker 复制**，否则目标端会先看到"删除"再看到
  "创建"，导致状态错乱。`Walk` 按版本升序遍历就是为了这个时序正确性。改复制逻辑时这条不变量
  极易被破坏。
`──────────────────────────────────────────`

## 6.2 Site Replication（站点级全量镜像）

Bucket 复制是"桶到桶、只复制对象数据"；Site Replication 是"站点到站点、复制几乎一切"。

| | Bucket Replication | Site Replication |
|---|---|---|
| 粒度 | bucket→bucket | 整个部署→整个部署 |
| 内容 | 对象数据 | **IAM + bucket 配置 + 对象 + ILM 规则** |
| 配置 | `PUT ?replication` | admin API |
| 方向 | 单/双向 | 多向（n-way 对等） |

- 核心：`SiteReplicationSys`（`cmd/site-replication.go:198`），状态记所有 peer 站点
  （`srStateV1.Peers`）和一个跨站点认证用的服务账号。
- **Heal routine**（`startHealRoutine` `:4265`）：用 `globalLeaderLock` 保证单实例，定期：
  1. `healIAMSystem()`：先修复身份（用户/组/策略）——**身份必须先于数据**，否则数据复制过去
     没有对应的访问主体。
  2. `healBuckets()`（`:4444`）：再修复 bucket 及其所有配置（versioning → object-lock →
     SSE → replication → policy → tags → quota → ILM）。

`★ 设计洞察 ─────────────────────────────────`
- **站点复制把"配置"也当数据复制**。多数据中心场景里，你在 A 站建个用户/改个策略，会自动同步
  到 B、C 站。这让多活变得可运维。"先 heal IAM 再 heal bucket"的顺序，是因为权限是数据访问的
  前提——顺序错了会出现"对象到了但没人有权读"的窗口。
`──────────────────────────────────────────`

## 6.3 生命周期（ILM）：过期与分层

ILM（Information Lifecycle Management）规则让对象到期自动删除，或冷数据自动转储到便宜的
远端存储（tier）。核心在 `cmd/bucket-lifecycle.go` + `cmd/data-scanner.go`。

### 两个后台状态机
- `expiryState`（`:171`）：过期 worker 池。
- `transitionState`（`:413`）：转储 worker 池，`transitionCh` 容量 100000。

### 规则评估在哪触发？——搭 scanner 的车
ILM 不是单独扫一遍，而是**复用 data scanner 的遍历**。scanner 扫到一个对象时
（`applyActions` `cmd/data-scanner.go:1038`）：
```
用 lifecycle.NewEvaluator() 评估这个对象 →  得到 action：
   DeleteAction        → applyExpiryRule        过期删除
   TransitionAction    → queueTransitionTask    转储到 tier
   DeleteVersionAction → 加入待删队列            清理旧版本
```

### 转储（Transition）
- `transitionObject`（`:688`）把对象数据搬到远端 tier（S3/Azure/GCS 等），本地只留一个"指针"。
- 读取已转储对象时 `getTransitionedObjectReader`（`:752`）通过
  `globalTierConfigMgr.getDriver(tier)` 从远端拉，支持 range。

`★ 设计洞察：扫描与生命周期共用一次遍历 ──────────`
- 遍历海量对象很贵（IOPS）。MinIO 不为"统计用量"扫一遍、再为"生命周期"扫一遍，而是**一次
  遍历顺便把 ILM 评估、heal 采样、复制检查全做了**。这是 scanner 设计的核心经济学——把所有
  "需要逐对象看一眼"的后台工作摊到同一次扫描里。
`──────────────────────────────────────────`

## 6.4 Data Scanner：统计 + 触发器

`cmd/data-scanner.go` 是后台最重要的 goroutine 之一。

### 启动与节奏（`initDataScanner` `:74`、`runDataScanner` `:157`）
- 单 Leader（`globalLeaderLock`）。
- 周期 `scannerCycle`（默认 1 分钟）+ 随机抖动，避免所有节点同步扫描。
- 调 `objAPI.NSScanner()` 真正执行，扫完保存 cycle 信息。

### 扫描算法 `scanFolder`（`:401`）
- 递归遍历目录，对每个对象 `getSize` 取大小/元数据。
- **顺便做三件事**：
  1. **用量统计**：累加到 `dataUsageEntry`（size/objects/versions + 大小直方图 + 分层统计）。
  2. **heal 采样**：按概率 `modAlt` 抽样选对象做 heal 检查（深扫还会验 bitrot）。
  3. **lifecycle/replication**：检查 prefix 上是否有规则，有就评估并入队。

### 限速与压缩
- `dataScannerSleepPerFolder`（1ms）等节流参数，控制扫描不抢占前台 I/O。
- 目录项太多时**主动压缩**缓存（`dataScannerCompactAtChildren=10000` 等阈值），避免缓存爆炸。

### 用量缓存：`data-usage-cache.go`
- `dataUsageCache` 用 msgp 序列化 + zstd 压缩存盘。
- 三层缓存（`oldCache`/`newCache`/`updateCache`）做增量更新，扫描进度可断点续传式合并。

`★ 易错点 ─────────────────────────────────`
- **用量统计是"最终一致"的，不是实时精确**。`mc admin info` 看到的容量来自 scanner 的上一轮
  结果，可能滞后一个扫描周期。新人常误以为它是实时账本——它不是，是后台采样聚合的快照。
`──────────────────────────────────────────`

## 6.5 自愈（Healing）：让坏数据自己修好

### 三条 heal 触发路径
1. **读时发现**：GET 读到 bitrot/缺块 → 纠删码当场绕过返回正确数据 → 同时把"这个对象需要修"
   记进 MRF。
2. **scanner 采样**：扫描时抽查对象，发现不一致就入 heal 队列。
3. **主动/全量 heal**：`mc admin heal`，或换盘后的全盘重建。

### MRF（`cmd/mrf.go`）
- `mrfState.opCh`（容量 100000）收"部分失败的操作"（`PartialOperation`：bucket/object/version
  + set/pool 坐标 + 是否要 bitrot 深扫）。
- 关机时把队列**序列化落盘**（`.minio.sys/buckets/.heal/mrf/list.bin`），重启接着修。

### Heal 一个对象（`cmd/erasure-healing.go`）
- `shouldHealObjectOnDisk`（`:179`）判断某块盘上的这个对象要不要修：
  文件不存在 / 版本不存在 / corrupt / 元数据与多数不一致 / part 文件缺失或损坏。
- 修复 = 用其它好盘的分片**重建**缺失/损坏盘上的分片，再写回那块盘。本质就是第 3 篇的
  纠删码 Decode + 重新 Encode 落盘。

### 后台 heal 序列（`cmd/global-heal.go`）
- `newBgHealSequence`（`:49`）：后台常驻 heal，开启 `HealDeleteDangling`（清理永远修不好的悬挂
  对象）。
- 换盘场景：新盘上线后，后台 heal 把本该在这块盘上的所有分片逐个重建回来。

`★ 设计洞察：自愈是一等公民 ─────────────────────`
- MinIO 把"数据会坏、盘会换、写会半途失败"当作**常态**而非异常来设计。读时绕过 + 后台修复 +
  关机持久化待修队列，三者合起来让"运维不必盯着每块盘"。这是它敢用纠删码（而非更保守的多副本）
  还能让人放心的底气——冗余不仅用于"读时容错"，更被持续地"补满"。
`──────────────────────────────────────────`

## 6.6 后台 goroutine 全景速查

| 子系统 | 初始化 | goroutine 数 | 职责 | 单Leader? |
|--------|--------|-------------|------|-----------|
| Data Scanner | `initDataScanner` | 1 | 统计用量 + 触发 heal/ILM | 是 |
| Bucket 复制 | `initBackgroundReplication` | N+大文件+MRF | 异步复制对象 | 否（各节点复制本地数据） |
| Site 复制 | `SiteReplicationSys.Init` | 1 | 同步 IAM/配置/对象 | 是 |
| ILM 过期 | `initBackgroundExpiry` | N | 删除过期对象 | — |
| Transition | `transitionState.Init` | M | 转储到远端 tier | — |
| Bucket Metadata | `globalBucketMetadataSys.Init` | 0 | 内存缓存 bucket 配置 | — |
| Tier Config | `globalTierConfigMgr.Init` | 1 | 管理远端 tier 连接 | — |
| Batch Jobs | `globalBatchJobsMetrics.init` | 2 | 批量复制/删除等任务 | — |

## 6.7 本篇要点回顾

- 后台服务共用骨架：**单 Leader（防重复）+ 任务队列/worker 池 + 磁盘溢出（MRF，不丢任务）**。
- Bucket 复制异步多目标可重试，注意"对象先于 delete marker"的时序不变量。
- Site 复制连配置带 IAM 一起同步，"先 heal IAM 再 heal bucket"。
- ILM 评估**搭 scanner 的车**，一次遍历做完统计 + heal 采样 + 生命周期。
- 用量是**最终一致**的快照，不是实时账本。
- 自愈是一等公民：读时绕过 + 后台重建 + 待修队列持久化。

最后一篇收口：把跨模块的精妙设计、并发陷阱、版本兼容和运维易错点集中梳理。
