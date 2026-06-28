# 深读 16 · Metrics V2 指标体系逐层精读

> Prometheus 周期性 scrape MinIO 拉指标。但很多指标的采集**很贵**（StorageInfo 要问所有盘、
> 用量来自 scanner 缓存）。如果每次 scrape 都重算，监控本身会拖垮系统。Metrics V2 的核心就是
> **按组分级缓存 + 依赖门控 + 无锁热路径**。本篇拆开指标描述符、`cachevalue` 缓存机制、依赖
> 门控、Prometheus collector（`cmd/metrics-v2.go` + `internal/cachevalue`）。

---

## 1. 指标的两个数据结构

```go
// :316 描述符（Prometheus 风格）
type MetricDescription struct {
    Namespace MetricNamespace   // minio / cluster / node / bucket
    Subsystem MetricSubsystem   // capacity / drive / usage / replication ...
    Name      MetricName        // total / used_bytes / latency ...
    Help      string
    Type      MetricTypeV2      // gauge / counter / histogram
    Buckets   []float64         // histogram 的桶边界
}
// :326 一个指标值
type MetricV2 struct {
    Description    MetricDescription
    StaticLabels   map[string]string   // 固定标签（每个指标一样）
    Value          float64
    VariableLabels map[string]string   // 可变标签（每个时间序列不同，如 bucket=xxx、drive=/data1）
    Histogram      map[string]uint64   // 直方图数据
}
```
- 最终导出成 Prometheus 的 `minio_cluster_capacity_usage_bytes{...}` 这种 `namespace_subsystem_name`。
- **StaticLabels vs VariableLabels**：static 是这个指标恒定的标签（如 server 地址）；variable 是
  区分不同时间序列的标签（如哪个 bucket、哪块盘）。Prometheus 的"高基数"问题就来自 variable
  label——bucket/对象级标签太多会爆 series 数。

---

## 2. 按组缓存：`MetricsGroupV2`

```go
// :336
type MetricsGroupV2 struct {
    metricsCache  *cachevalue.Cache[[]MetricV2]   // ★ 每组一个 TTL 缓存
    cacheInterval time.Duration                   // 这组的刷新间隔（TTL）
    metricsGroupOpts MetricsGroupOpts              // 依赖声明
}
// :360 注册采集函数
func (g *MetricsGroupV2) RegisterRead(read func(ctx) []MetricV2) {
    g.metricsCache = cachevalue.NewFromFunc(g.cacheInterval, cachevalue.Opts{ReturnLastGood: true},
        func(ctx) ([]MetricV2, error) {
            // ★ 采集前先检查依赖是否就绪（见 §4）
            ...
            return read(GlobalContext), nil
        })
}
// :450 取值（scrape 时调）
func (g *MetricsGroupV2) Get() []MetricV2 {
    m, _ := g.metricsCache.Get()
    metrics := make([]MetricV2, 0, len(m))
    for i := range m { metrics = append(metrics, m[i].clone()) }   // ★ clone 防止并发 scrape 互相改
    return metrics
}
```
`★ 分组 + 各组独立 TTL ─────────────────────────`
- **指标按组（capacity / drive / usage / replication / scanner ...）划分，每组有自己的 TTL**
  （`cacheInterval`）。便宜的指标可以 TTL 短（更实时），昂贵的指标 TTL 长（少算）。**采集成本
  和指标实时性按组单独权衡**。
- **`Get()` 返回 clone**：scrape 是并发的（多个 Prometheus / 多个 endpoint）。返回缓存值的深拷贝，
  避免一个 scrape 改了 map 影响另一个，或缓存值被外部 mutate。
`──────────────────────────────────────────`

---

## 3. `cachevalue`：无锁热路径 + 单飞刷新 + 兜底

缓存的灵魂是 `internal/cachevalue`（`Get` → `GetWithCtx`，`cache.go:96`）：
```go
func (t *Cache[T]) GetWithCtx(ctx) (T, error) {
    v := t.val.Load()                                // ★ atomic load，无锁
    if v != nil && tNow-vTime < ttl { return *v }    // ★ 热路径：TTL 内直接返回，零锁零计算

    if t.opts.NoWait && v != nil && tNow-vTime < 2*ttl {   // NoWait：返回稍旧值 + 异步刷新
        if t.updating.TryLock() { go func(){ defer t.updating.Unlock(); t.update(...) }() }
        return *v
    }

    t.updating.Lock(); defer t.updating.Unlock()     // ★ 过期 → 取刷新锁（单飞）
    if time.Since(...) < ttl { return *t.val.Load() } // 双检：等锁期间别人刚刷过
    t.update(ctx)                                     // 真正调 updateFn
    return *t.val.Load()
}
// :142 update
func (t *Cache[T]) update(ctx) error {
    val, err := t.updateFn(ctx)
    if err != nil {
        if t.opts.ReturnLastGood && t.val.Load() != nil { return nil }  // ★ 失败 → 保留上次好值
        return err
    }
    t.val.Store(&val); t.lastUpdateMs.Store(now)                        // atomic store
}
```
`★ 这个缓存为什么精妙 ─────────────────────────`
- **TTL 内无锁热路径**：`atomic.Pointer` load + 时间比较，**完全无锁、无计算**。一个 5s TTL 的
  指标组，5s 内不管被 scrape 多少次，都直接返回同一份缓存——**采集成本和 scrape 频率彻底解耦**。
  Prometheus 每 15s scrape 一次，1s 也 scrape 一次，对后端压力一样（采集只在 TTL 过期时发生一次）。
- **单飞刷新（`updating` 锁）**：TTL 过期瞬间若有 100 个并发 scrape，**只有一个拿到锁去 `updateFn`
  （贵的采集），其余 99 个等它**（双检后直接拿新值）。避免"缓存过期瞬间 100 个 scrape 同时重算
  StorageInfo"打爆后端——这是深读 09 singleflight 思想的又一处应用。
- **`ReturnLastGood` 兜底**：采集失败（某盘超时等）时，**返回上一次成功的值而非报错**。监控宁可
  看到稍旧的数据，也不要因为一次采集抖动就出现指标缺口/告警风暴。
- **`NoWait` 异步刷新**：可选模式下，返回稍旧值的同时后台异步刷新，**scrape 永不阻塞**。适合
  绝不能让 scrape 卡住的场景。
`──────────────────────────────────────────`

---

## 4. 依赖门控：启动期/未启用子系统不报错

```go
// :360 RegisterRead 的采集函数开头
if g.metricsGroupOpts.dependGlobalObjectAPI {
    if newObjectLayerFn() == nil { return []MetricV2{}, nil }   // ★ 对象层没就绪 → 返回空
}
if g.metricsGroupOpts.dependGlobalKMS { if GlobalKMS == nil { return []MetricV2{}, nil } }
if g.metricsGroupOpts.dependGlobalSiteReplicationSys { if !globalSiteReplicationSys.isEnabled() { return []MetricV2{}, nil } }
// ... 一长串依赖检查
return read(GlobalContext), nil
```
```go
// :343 依赖声明
type MetricsGroupOpts struct {
    dependGlobalObjectAPI, dependGlobalKMS, dependGlobalIAMSys,
    dependGlobalSiteReplicationSys, dependGlobalNotificationSys,
    dependGlobalLockServer, dependGlobalIsDistErasure, ... bool
}
```
`★ 优雅降级 ───────────────────────────────────`
- **每个指标组声明它依赖哪些子系统**（对象层 / KMS / IAM / site replication ...）。采集前检查这些
  依赖是否就绪/启用——**没就绪就返回空指标，而不是 panic 或报错**。
- 解决两个真实问题：① **启动期 scrape**：进程刚起、对象层还没好（深读 02 启动顺序），此时 scrape
  不会崩，相关指标暂时为空。② **未启用的子系统**：没配 KMS/site replication 的部署，这些指标组
  返回空，不会污染指标也不报错。
- 这是"指标系统对子系统状态的防御性编程"——监控绝不能因为被监控对象没准备好而自己挂掉。
`──────────────────────────────────────────`

---

## 5. Prometheus Collector：MetricV2 → prometheus.Metric

```go
// :4151 三种 collector（cluster / node / bucket）实现 prometheus.Collector
func (c *minioClusterCollector) Collect(out chan<- prometheus.Metric) {
    // 遍历相关的 MetricsGroupV2，调 g.Get()（命中缓存），逐个转成 prometheus.Metric
}
// :4041 collectMetric
ch <- prometheus.MustNewConstMetric(desc, valueType, metric.Value, labelValues...)
```
- MinIO 实现 Prometheus 的 `Collector` 接口（`Describe`/`Collect`）。scrape 来时 `Collect` 被调，
  它对每个指标组调 `Get()`（命中 TTL 缓存），把 `MetricV2` 转成 `prometheus.MustNewConstMetric`
  推进 channel。
- **三个 namespace 三个 collector**：cluster（集群级，如总容量）、node（本节点级，如本机盘延迟）、
  bucket（桶级，如每桶用量）。bucket 级因为高基数（每个 bucket 一组 series）单独管理。

---

## 6. 一页纸总结 Metrics V2 的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | MetricDescription = namespace_subsystem_name | Prometheus 风格分层命名 |
| 2 | StaticLabels vs VariableLabels | 区分恒定标签与时间序列标签（高基数源） |
| 3 | 指标按组、每组独立 TTL | 采集成本与实时性按组权衡 |
| 4 | Get() 返回 clone | 并发 scrape 不互相 mutate |
| 5 | cachevalue TTL 内 atomic 无锁热路径 | 采集成本与 scrape 频率彻底解耦 |
| 6 | 单飞刷新（updating 锁 + 双检） | 过期瞬间不被并发 scrape 打爆后端 |
| 7 | ReturnLastGood 兜底 | 采集抖动不造成指标缺口/告警风暴 |
| 8 | NoWait 异步刷新 | scrape 永不阻塞 |
| 9 | 依赖门控：未就绪/未启用返回空 | 启动期、未配子系统不崩不报错 |
| 10 | 三 collector（cluster/node/bucket） | bucket 高基数单独管理 |

---

## 系列再收官：16 篇精读的全景

```
存储核心    01 写入 · 02 读取 · 03 xl.meta · 04 单盘 · 05 路由
分布式      06 grid · 07 dsync
安全        08 签名 · 09 IAM · 10 加密
数据服务    11 复制 · 12 扫描 · 13 自愈
外围补完    14 batch · 15 site-replication · 16 metrics
```

三篇外围补完后，一个有意思的观察是——**它们都在复用前面讲过的核心模式**：

- **batch（14）** 的检查点持久化 = 深读 04 的"临时区 + 落盘"思想；节流持久化 = 深读 11 MRF 的
  "进度 vs 写开销"权衡。
- **site-replication（15）** 的两阶段 heal 单 leader = 深读 12 scanner / 深读 06 之外所有后台任务
  共用的 `globalLeaderLock`；冲突解决 LWW 依赖时钟 = 深读 08 签名同样依赖时钟。
- **metrics（16）** 的单飞缓存 = 深读 09 IAM 的 singleflight；优雅降级 = 深读 01 globalObjectAPI
  判空的同一种"子系统未就绪"防御。

这正是读透一个成熟系统的最大收获：**核心抽象和模式会反复出现**。当你在第 16 篇里一眼认出
"这又是单飞 / 这又是 leader 锁 / 这又是临时区原子提交"，你就真正掌握了 MinIO 的设计语言——
而不只是记住了一堆函数。

接下来任何还没读的文件（`erasure-server-pool-decom.go`、`bucket-lifecycle.go`、`notify/` ……），
你都能用同样的方法独立读透：**追调用链、认出复用的模式、问为什么这么设计、找不变量。**
```
```
