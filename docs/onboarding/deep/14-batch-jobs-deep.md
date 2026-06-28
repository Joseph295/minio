# 深读 14 · Batch Jobs 批量任务逐层精读

> Batch Jobs 是管理员驱动的**大批量操作**：批量复制（replicate）、批量过期（expire）、
> 批量密钥轮换（keyrotate）。它们用 YAML 清单提交、断点续传、重启可恢复、可取消。本篇拆开
> 任务池、检查点持久化、节点分配、resume 恢复、过滤谓词与 snowball 归档优化
> （`cmd/batch-handlers.go` + `batch-{replicate,expire,rotate}.go`）。

---

## 1. 与 bucket replication 的区别

| | Bucket Replication（深读 11） | Batch Replicate |
|---|---|---|
| 触发 | 持续异步（写一个复制一个） | 一次性批量（管理员发起一个 job） |
| 范围 | 增量（新写入） | 存量全量（按过滤器选对象） |
| 配置 | bucket 复制规则 | YAML job 清单 |
| 生命周期 | 常驻 | 跑完即结束 |
| 控制 | 自动 | start/status/cancel/describe admin API |

Batch 是"一锤子买卖"的批处理；bucket replication 是"细水长流"的持续复制。

---

## 2. 任务池 `BatchJobPool`

```go
// :1851
type BatchJobPool struct {
    jobCh        chan *BatchJobRequest          // 待执行 job 队列（缓冲 10000）
    jobCancelers map[string]context.CancelFunc  // 每个 job 一个取消函数
    workerKillCh chan struct{}
    workerSize   int
}
// :1866 newBatchJobPool
go func() {
    jpool.resume(randomWait)          // ★ 启动时恢复未完成的 job
    jpool.cleanupReports(randomWait)  // ★ 清理已完成的旧 job 报告
}()
```
- `AddWorker`（`:1966`）消费 `jobCh`，按类型分派：
  ```go
  case job.Replicate != nil:
      if job.Replicate.RemoteToLocal() { job.Replicate.StartFromSource(...) }  // 远端→本地
      else                             { job.Replicate.Start(...) }            // 本地→远端
  case job.KeyRotate != nil: job.KeyRotate.Start(...)
  case job.Expire    != nil: job.Expire.Start(...)
  ```
- `queueJob`（`:2040`）：给每个 job 建独立的可取消 context（存进 `jobCancelers`），非阻塞入队
  （队列满返回错误）。
- `canceler`（`:2063`）：按 job ID 取消正在跑的 job——`CancelBatchJob` admin API 的落点。

`★ randomWait：避免节点齐步走 ───────────────────`
```go
// :1876
randomWait := func() time.Duration {
    return time.Duration(rand.Float64() * float64(globalEndpoints.NEndpoints()) * time.Hour)
}
```
- resume 和 cleanup 的触发时刻**随节点数加随机抖动**。如果所有节点同时 resume（重启后）或同时
  cleanup，会造成 I/O 尖峰和锁竞争。抖动把它们打散到几个小时的窗口里。节点越多，窗口越宽。
`──────────────────────────────────────────`

---

## 3. 两份持久化：job 清单 + 检查点

Batch job 在磁盘上有**两份**持久化（都存在 `.minio.sys` 系统桶）：

### ① Job 清单 `BatchJobRequest`
```go
// :768
batchJobPrefix = "batch-jobs"
// save → .minio.sys/buckets/batch-jobs/{jobID}（job.bin）
```
- 提交 job 时把整个清单（源/目标/凭证/过滤器/重试配置）落盘。**重启后能重新加载**。

### ② 检查点 `batchJobInfo`
```go
// :735
type batchJobInfo struct {
    JobID, JobType string
    Bucket, Object string   // ★ 最后处理到的 bucket/object（断点）
    Complete, Failed bool
    Objects, ObjectsFailed, BytesTransferred ... int64   // 进度统计
}
// 存 → .minio.sys/buckets/batch-jobs/reports/{jobID}/batch-replicate.bin
```
`★ 断点续传：Bucket/Object 记录进度 ───────────────`
- `batchJobInfo` 记录**最后成功处理的对象**（`ri.Bucket`/`ri.Object`）。`Start`（`:1049`）
  开头 `lastObject := ri.Object`，遍历时从这个点之后继续——**一个跑了一半的批量复制，重启后
  从断点续，不重头来**。
- `Complete=true` 直接返回（`:1045`），不重跑已完成的 job。
`──────────────────────────────────────────`

### 检查点节流 `updateAfter`
```go
// :936
func (ri *batchJobInfo) updateAfter(ctx, api, duration, job) error {
    if now.Sub(ri.LastUpdate) >= duration {   // ★ 距上次持久化够久才写盘
        ri.LastUpdate = now
        // marshal + saveConfig
    }
}
```
- **不是每处理一个对象就写一次检查点**（太频繁，写放大），而是**每隔 `duration` 才持久化一次**。
  代价：崩溃可能丢失最近 `duration` 内的进度（会重做那一小段），但换来检查点写入开销可控。
  这是"进度精度 vs 写开销"的经典权衡。

---

## 4. 节点分配：一个 job 只在一个节点跑

```go
// resume :1953
_, nodeIdx := parseRequestToken(req.ID)
if nodeIdx > -1 && GetProxyEndpointLocalIndex(globalProxyEndpoints) != nodeIdx {
    continue   // ★ 这个 job 不属于本节点，跳过
}
```
`★ 细节 ────────────────────────────────────`
- **job ID 里编码了"该 job 归哪个节点执行"**（`nodeIdx`）。集群里每个节点 resume 时只捡属于自己
  的 job，**同一个 job 不会被多个节点重复执行**。
- 这是一种**无锁的任务分片**：不靠 leader 选举或分布式锁分配 job，而是在创建 job 时就把它"钉"
  到某个节点（通过 ID 编码），各节点各扫各的。简单且无协调开销。
`──────────────────────────────────────────`

---

## 5. 对象选择：过滤谓词 `selectObj`

```go
// :1062
selectObj := func(info FileInfo) bool {
    if OlderThan > 0 && time.Since(info.ModTime) < OlderThan { return false }   // 太新跳过
    if NewerThan > 0 && time.Since(info.ModTime) >= NewerThan { return false }  // 太老跳过
    if CreatedAfter/CreatedBefore 不满足 { return false }
    if len(Tags) > 0 { 解析对象标签，匹配才选 }
    if len(Metadata) > 0 { 匹配 x-amz-meta-* 或标准 header 才选 }
    // 源或目标是非 MinIO（S3）→ 只复制最新版本（像 mc mirror）
    return !isSourceOrTargetS3 || info.IsLatest
}
```
- 批量复制不是无脑全复制，而是按**时间/标签/元数据**过滤。这让"只复制上个月之前、带某 tag 的
  对象"成为可能。
- **非 MinIO 目标只复制最新版**：S3/Azure 等不支持 MinIO 的多版本语义，所以退化为"只镜像顶层
  版本"，像 `mc mirror`。

---

## 6. 重试与远端客户端

```go
// :1148
for attempts := 1; attempts <= retryAttempts; attempts++ {   // 默认 3 次
    walkCh := make(chan itemOrErr[ObjectInfo], 100)
    // 用 minio-go Core 客户端连远端
    c, _ := minio.NewCore(u.Host, &minio.Options{Creds: ..., Transport: getRemoteInstanceTransport()})
    // 遍历源对象 → selectObj 过滤 → ReplicateToTarget/ReplicateFromSource
}
```
- **整个 job 级重试**（默认 3 次，`batchReplJobDefaultRetries`）：一轮跑完若有失败对象，整体
  重试，配合检查点只重做失败的部分。
- 远端用 **minio-go `Core` 客户端**（低层 API）——batch 复制是"MinIO 主动去推/拉"，所以用客户端
  SDK 连远端，而非 bucket replication 那套内部机制。

### Snowball 归档优化
```go
// :1156
if Snowball 启用 && 源和目标都是 MinIO {
    // 把多个小对象打包成一个 tar 归档上传（writeAsArchive）
}
```
`★ Snowball：小对象打包 ───────────────────────`
- 批量复制海量**小对象**时，逐个 PUT 的 RTT 开销巨大。Snowball 模式把一批小对象打成一个 tar
  归档，一次性传到目标，目标端再解包。**用一次大传输替代成千次小传输**，大幅提升小对象批量
  复制吞吐。只在 MinIO→MinIO 时可用（需目标端支持解归档）。
`──────────────────────────────────────────`

---

## 7. 生命周期管理

- **admin API**：`StartBatchJob`（`:1734`）提交、`BatchJobStatus`/`DescribeBatchJob` 查询、
  `CancelBatchJob`（`:1816`）取消、`ListBatchJobs` 列表。
- **`cleanupReports`（`:1889`）**：周期性扫 `batch-jobs/reports`，把 `Complete||Failed` 且超过
  `oldJobsExpiration` 的 job 报告删掉——已完成的 job 不会无限堆积。
- **metrics**（`:2081` `batchJobMetrics`）：`countItem`/`trackCurrentBucketObject` 实时累加进度，
  `report` 供 `DescribeBatchJob` 返回，`purgeJobMetrics`（`:2183`）清理过期内存指标。

---

## 8. 一页纸总结 Batch Jobs 的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 一次性批量 vs bucket replication 的持续增量 | 存量全量 vs 增量两种复制场景 |
| 2 | 三类 job（replicate/expire/keyrotate）统一池 | 一套调度框架服务多种批处理 |
| 3 | 两份持久化：job 清单 + 检查点 | 重启可重载 job 并从断点续 |
| 4 | 检查点记 Bucket/Object 断点 | 跑一半重启不重头 |
| 5 | updateAfter 节流持久化 | 进度精度 vs 写开销权衡 |
| 6 | job ID 编码 nodeIdx 分配节点 | 无锁任务分片，不重复执行 |
| 7 | randomWait 抖动 resume/cleanup | 避免节点齐步走造成 I/O 尖峰 |
| 8 | selectObj 时间/标签/元数据过滤 | 精确选择批处理对象 |
| 9 | 非 MinIO 目标只复制最新版 | 退化为 mc mirror 语义 |
| 10 | job 级重试 + 检查点 | 只重做失败部分 |
| 11 | Snowball 小对象打包归档 | 一次大传输替代千次小传输 |
| 12 | cleanupReports 清理完成的旧 job | 报告不无限堆积 |

下一篇深读：**Site Replication 站点级全量同步**——多站点对等拓扑、IAM+配置+对象的全量复制、
`startHealRoutine` 的两阶段 heal、复制 hook 的注入点。
