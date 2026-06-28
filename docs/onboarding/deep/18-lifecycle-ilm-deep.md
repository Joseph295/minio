# 深读 18 · Bucket Lifecycle（ILM）逐层精读

> ILM（Information Lifecycle Management）让对象到期自动**过期删除**或自动**转储到便宜的远端 tier**。
> 本篇拆开规则求值引擎与守卫、过期/转储两个 worker 池、`WarmBackend` 统一 tier 抽象、转储与回读、
> 已转储对象的过期（`cmd/bucket-lifecycle.go`、`warm-backend.go`、`tier.go`）。

---

## 1. ILM 的两类动作

- **过期（Expiry）**：到期删除对象/版本/删除标记（`DeleteAction` 系列）。
- **转储（Transition）**：把冷数据搬到远端便宜存储（S3/Azure/GCS/MinIO tier），**本地只留指针**
  （深读 03 §5 的 tier 元数据），读时再从远端拉。

两类各有一个后台 worker 池：`expiryState`、`transitionState`。

---

## 2. 规则求值：`lc.Eval` + 守卫

ILM 不单独扫盘，而是**搭 data scanner 的车**（深读 12 §4）：scanner 扫到对象 → `applyActions` →
`evalActionFromLifecycle`。
```go
// data-scanner.go:1166
func evalActionFromLifecycle(ctx, lc lifecycle.Lifecycle, lr lock.Retention, rcfg *replication.Config, obj ObjectInfo) lifecycle.Event {
    event := lc.Eval(obj.ToLifecycleOpts())   // ★ 规则引擎求值，返回该对象的 Action
    switch event.Action {
    case DeleteAllVersionsAction, DelMarkerDeleteAllVersionsAction:
        if lr.LockEnabled { return NoneAction }                 // ★ 对象锁开启 → 不删（守护 retention）
    case DeleteVersionAction, DeleteRestoredVersionAction:
        if obj.VersionID == "" { return NoneAction }            // 防御
        if lr.LockEnabled && enforceRetentionForDeletion(ctx, obj) { return NoneAction }  // ★ 受保留期保护 → 不删
        if rcfg != nil && !obj.VersionPurgeStatus.Empty() && rcfg.HasActiveRules(...) { return NoneAction }  // ★ 复制删除未完成 → 不删
    }
    return event
}
```
`★ 守卫：ILM 不能违反更高约束 ───────────────────`
- **对象锁（Object Lock）retention 优先于 ILM**：被合规保留期锁定的对象/版本，ILM 到期也**不能删**
  （否则违反 WORM 合规）。`lr.LockEnabled && enforceRetentionForDeletion` 命中就返回 `NoneAction`。
- **复制未完成的删除不删**：一个版本的复制删除（`VersionPurgeStatus`）还没传播到目标站点时，
  本地不能先删——否则删除就丢了，没法复制过去（呼应深读 11 复制状态机）。
- ILM 是"最低优先级"的自动清理：它让位于对象锁和复制。理解这点能解释"配了过期规则但对象没被
  删"——多半是它被锁定了或在等复制。
`──────────────────────────────────────────`

求值结果按 action 分派（`applyActions`，深读 12）：
- `DeleteAction` → `applyExpiryRule` → `globalExpiryState` 入队删除。
- `TransitionAction` → `applyTransitionRule` → `globalTransitionState.queueTransitionTask`。
- `DeleteVersionAction` → 加入待删队列。

---

## 3. 过期 worker 池 `expiryState`

```go
// bucket-lifecycle.go：多种入队方式
enqueueByDays(oi, event, src)                  // 按"N 天后过期"规则删除
enqueueNoncurrentVersions(bucket, versions, events)  // 删非当前版本（批量）
enqueueFreeVersion(oi)                         // ★ 删 free-version（清理远端 tier 残留）
enqueueTierJournalEntry(je)                    // tier 删除日志
// :332 Worker 消费 expiryOp，执行实际删除
```
- **按哈希分通道**（`getWorkerCh(h)`）：同一对象的过期任务稳定落到同一 worker，避免乱序。
- **`enqueueFreeVersion`**：呼应深读 03 §2——删一个已转储对象时，本地立刻删，但远端 tier 的数据
  靠 free-version 标记异步清理。这个入队就是处理那些 free-version，去远端删数据。

---

## 4. 转储 `transitionObject` → `WarmBackend`

```go
// :688
func transitionObject(ctx, objectAPI, oi ObjectInfo, lae) error {
    opts := ObjectOptions{
        Transition: TransitionOptions{
            Status: lifecycle.TransitionPending,
            Tier:   lae.StorageClass,    // 目标 tier 名
            ETag:   oi.ETag,
        },
        VersionID: oi.VersionID, MTime: oi.ModTime, ...,
    }
    return objectAPI.TransitionObject(ctx, oi.Bucket, oi.Name, opts)   // ★ 交给 ObjectLayer
}
```
`TransitionObject` 内部：把对象数据 `Put` 到 `WarmBackend`，然后把本地 `xl.meta` 改成"已转储"
（记 tier 名、远端对象名、远端版本——深读 03 §5），**删除本地 part 数据**，只留指针。

### `WarmBackend`：统一 tier 抽象
```go
// warm-backend.go:39
type WarmBackend interface {
    Put(ctx, object, r, length) (remoteVersionID, error)
    PutWithMeta(ctx, object, r, length, meta) (remoteVersionID, error)
    Get(ctx, object, rv, opts) (io.ReadCloser, error)
    Remove(ctx, object, rv) error
    InUse(ctx) (bool, error)
}
// :134 按 tier 类型分派到具体驱动
func newWarmBackend(ctx, tier, probe) (WarmBackend, error) {
    switch tier.Type {
    case S3:    d = newWarmBackendS3(...)
    case Azure: d = newWarmBackendAzure(...)
    case GCS:   d = newWarmBackendGCS(...)
    case MinIO: d = newWarmBackendMinIO(...)
    }
    if probe { checkWarmBackend(ctx, d) }   // ★ 探测凭证权限
}
```
`★ WarmBackend 是又一个 StorageAPI 式的接口抽象 ──`
- **`WarmBackend` 把"远端冷存储"抽象成 Put/Get/Remove**，和深读 01 的 `StorageAPI`（一块盘）、
  深读 05 的 ObjectLayer 是同一种"面向接口编程"——上层的转储逻辑不关心冷存储是 S3 还是 Azure，
  底层换驱动即可。又一处 MinIO 用接口吃掉异构复杂度的例子。
- **`checkWarmBackend` 探测**（`:51`）：建 tier 时用 `probeObject` 做一次 Put→Get→Remove，**提前
  验证凭证有没有读/写/删权限**。少了哪个权限就报对应的 `tierPermErr{Op}`——配 tier 时立刻知道
  哪个权限缺了，而不是等真转储时才失败。
- **`InUse`**：删 tier 配置前检查"还有没有对象转储在这个 tier 上"，防止误删还在用的 tier。
`──────────────────────────────────────────`

### 转储的 worker 与节奏
```go
// :489 transitionState.worker
// 消费 transitionCh（容量 100000），调 transitionObject
// :429 queueTransitionTask：跳过 delete marker / 目录，入队
```
- `globalTierMetrics`（`:740`）记录每个 tier 的转储延迟/成功失败——tier 慢/不可用可被监控发现。

---

## 5. 回读已转储对象

```go
// bucket-lifecycle.go:752  getTransitionedObjectReader
// GET 命中已转储对象（深读 02 §1 的 objInfo.IsRemote() 分支）→ 从 tier 拉
d, _ := globalTierConfigMgr.getDriver(ctx, oi.TransitionedObject.Tier)  // tier.go:397
r, _ := d.Get(ctx, oi.TransitionedObject.Name, remoteVersionID, WarmBackendGetOpts{rs})  // 支持 range
```
- 读已转储对象**对客户端透明**：客户端不知道数据在远端，MinIO 从 tier 拉回来流式返回。支持 Range
  （`WarmBackendGetOpts` 带偏移），断点续传/分段下载照常工作。
- 代价是**延迟**：从远端冷存储拉比本地盘慢。所以转储是"用访问延迟换存储成本"的权衡。

---

## 6. 过期已转储对象：删两处

```go
// :609 expireTransitionedObject
// 删一个已转储对象，要删【本地指针】+【远端 tier 数据】两处
```
`★ 细节：转储对象的过期是两段删除 ───────────────`
- 一个对象先被转储（数据在远端、本地留指针），后来又到了过期时间。删它**必须删两处**：本地的
  `xl.meta` 指针 + 远端 tier 上的实际数据。
- **远端删除是异步的**（通过 free-version + `enqueueFreeVersion`，§3）：本地指针可以立刻删，远端
  数据靠 scanner 后续扫到 free-version 再去 `WarmBackend.Remove`。**"本地快、远端慢"的解耦**贯穿
  整个 tier 生命周期（呼应深读 03 §2 free-version 的设计初衷）。
`──────────────────────────────────────────`

---

## 7. 一页纸总结 ILM 的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 两类动作：过期删除 + 转储到 tier | 自动清理 + 冷数据降本 |
| 2 | 求值搭 scanner 的车（不单独扫） | 复用遍历省 IOPS |
| 3 | 对象锁 retention 优先于 ILM | 不违反 WORM 合规 |
| 4 | 复制未完成的删除不删 | 删除要先传播到目标站点 |
| 5 | ILM 让位于对象锁与复制 | 解释"配了过期却没删" |
| 6 | 过期/转储两个 worker 池，哈希分通道 | 同对象任务有序，互不堵 |
| 7 | WarmBackend 接口抽象 tier | 上层不关心 S3/Azure/GCS，换驱动即可 |
| 8 | checkWarmBackend 探测 Put/Get/Remove 权限 | 配 tier 时立刻发现权限缺失 |
| 9 | InUse 防误删在用的 tier | 删 tier 配置前检查 |
| 10 | 转储后本地留指针、删 part 数据 | 用访问延迟换存储成本 |
| 11 | 回读对客户端透明 + 支持 Range | 冷数据访问无感（但更慢） |
| 12 | 过期已转储对象删本地指针 + 远端数据（异步） | 本地快远端慢解耦，靠 free-version |
| 13 | globalTierMetrics 监控 tier 延迟/失败 | tier 不可用可被发现 |

下一篇深读：**事件通知体系**——event target 抽象、事件匹配规则、`internal/store` 持久化队列、
异步投递与重试。
