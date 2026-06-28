# 深读 07 · `internal/dsync` 分布式锁逐层精读

> MinIO 不依赖外部协调器（ZooKeeper/etcd）做互斥，而是用一套**基于 quorum 的分布式读写锁**。
> 本篇拆开两侧：客户端 `DRWMutex`（向 N 个节点请求锁并按 quorum 判定，`internal/dsync/drwmutex.go`）
> 与服务端 `localLocker`（每个节点在内存里持锁、过期回收，`cmd/local-locker.go`）。

---

## 1. 全局图：两侧 + Owner/UID 身份

```
   想锁 bucket/object 的协程
        │ GetLock
   ┌────▼─────────┐  DRWMutex (客户端)：把锁请求广播到所有 N 个节点
   │  DRWMutex    │  args = {UID, Owner, Resources, Quorum}
   └────┬─────────┘
        │ Lock(args) ×N（并行 RPC，远端走 grid）
   ┌────▼────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
   │localLckr│ │localLckr│ │localLckr│ │localLckr│   每个节点一个，内存里持锁
   │ node0   │ │ node1   │ │ node2   │ │ node3   │
   └─────────┘ └─────────┘ └─────────┘ └─────────┘
   收到 ≥ quorum 个 "granted" → DRWMutex 认为加锁成功
```
- **`UID`**：本次加锁请求的唯一 ID（每次 GetLock 生成）。解锁/续约都靠它定位。
- **`Owner`**：发起方节点的 UUID。解锁时要 UID **和** Owner 都匹配——防止"别的节点冒用 UID 解你的锁"。
- **`Quorum`**：本次要求的 quorum 数，随请求一起带给每个节点（节点记录下来，过期判断时用）。

---

## 2. Quorum 数学：tolerance、quorum、防脑裂 +1

```go
// drwmutex.go:217
tolerance := len(restClnts) / 2          // 能容忍多少节点挂
quorum    := len(restClnts) - tolerance  // 需要多少节点同意
if !isReadLock {
    if quorum == tolerance { quorum++ }  // ★ 写锁专属：tolerance 恰为半数时 +1，防脑裂
}
tolerance = len(restClnts) - quorum      // ★ 反算回 tolerance（与 quorum 配对）
```
`★ 为什么写锁要 +1 ───────────────────────────`
- 设 N=4：tolerance=2, quorum=2。但 `quorum==tolerance` → **写锁 quorum 提到 3**。
- 直觉：若读锁 quorum 和写锁 quorum 都是 2，网络分区成 {2 节点} + {2 节点} 时，两边可能**各自
  凑齐 2 个同意**，两个分区都以为自己拿到了写锁 = 脑裂。把写锁 quorum 提到 3（严格多数），
  保证任意两次"成功的写锁"在节点集合上**必有交集**，不可能两个分区同时写成功。
- **读锁不 +1**：多个读者本来就可并发，读读之间无需互斥，不存在这个脑裂问题。
`──────────────────────────────────────────`

---

## 3. 客户端加锁：`lockBlocking` 重试循环

```go
// drwmutex.go:237
for {
    select {
    case <-ctx.Done(): return false                  // 总超时
    default:
        if locked = lock(ctx, dm.clnt, &locks, ...); locked {
            // 成功：把各节点返回的 lockUID 存进 dm.writeLocks/readLocks
            copy(dm.writeLocks, locks)
            dm.startContinuousLockRefresh(lockLossCallback, id, source, quorum)  // ★ 起续约
            return true
        }
        switch {
        case opts.RetryInterval < 0: return false                 // ★ <0：试一次就走（非阻塞）
        case opts.RetryInterval > 0: time.Sleep(opts.RetryInterval)// 固定间隔重试
        default: attempt++; time.Sleep(lockRetryBackOff(rng, attempt))  // ★ 指数退避（默认）
        }
    }
}
```
- **`lockRetryMinInterval = 250ms`**（`drwmutex.go:47`，可由 `_MINIO_LOCK_RETRY_INTERVAL` 覆盖），
  退避是指数增长（`backoffWait`）。竞争激烈时退避避免把节点打爆。
- **`RetryInterval < 0` = 非阻塞 TryLock**：试一次拿不到立刻返回 false。某些场景（如能跳过就跳过的
  后台任务）用它避免阻塞。

---

## 4. 一次加锁尝试 `lock()`：广播、收集、回滚

```go
// drwmutex.go:~420
ch := make(chan Granted, len(restClnts))                  // 缓冲 = 节点数
args := LockArgs{Owner: owner, UID: id, Resources: names, Quorum: &quorum, ...}
ctx, cancel := context.WithTimeout(ctx, ds.Timeouts.Acquire)   // 单次尝试超时（默认 1s）

// 关键：NetLocker 调用用独立的 Background ctx，不套 acquire 超时（只传 trace）
netLockCtx := context.Background()
... // 注入 trace context

for index, c := range restClnts {                         // ★ 并行广播到所有节点
    go func(index int, c NetLocker) {
        locked, _ := c.Lock(netLockCtx, args)             // 或 RLock
        g := Granted{index: index}
        if locked { g.lockUID = args.UID }
        ch <- g
    }(index, c)
}

// 收集：直到 (a) 收齐 (b) 失败数 > tolerance（quorum 无望）(c) 超时
i, locksFailed, done := 0, 0, false
for ; i < len(restClnts); i++ {
    select {
    case grant := <-ch:
        if grant.isLocked() { (*locks)[grant.index] = grant.lockUID }
        else { locksFailed++; if locksFailed > tolerance { done = true } }   // ★ 提前放弃
    case <-ctx.Done():
        locksFailed++; if locksFailed > tolerance { done = true }
    }
    if done { break }
}

quorumLocked := checkQuorumLocked(locks, quorum) && locksFailed <= tolerance
if !quorumLocked {
    releaseAll(ctx, ds, tolerance, owner, locks, isReadLock, restClnts, names...)  // ★ 回滚已得锁
}

// ★ 异步释放"放弃后才到达的"被遗弃的锁
go func() {
    wg.Wait(); xioutil.SafeClose(ch)
    for grant := range ch {
        if grant.isLocked() { sendRelease(ctx, ds, restClnts[grant.index], owner, grant.lockUID, ...) }
    }
}()
```
`★ 这段代码的四个精妙点 ─────────────────────────`
- **失败数 > tolerance 就提前退出**：一旦确定"剩下的节点全同意也凑不齐 quorum"，立刻停止等待，
  不浪费时间在注定失败的尝试上。
- **加锁失败必须 `releaseAll` 回滚**：拿到了 2 个、需要 3 个 → 把那 2 个还回去。否则这 2 个锁
  挂在那里，**别人也凑不齐 quorum**，形成"谁都拿不到"的活锁。回滚是分布式锁正确性的命脉。
- **`netLockCtx = context.Background()`（不套 acquire 超时）**：单次尝试的 1s 超时只控制"收集
  循环等多久"，而**不直接掐断对各节点的 RPC**。为什么？因为 RPC 本身有自己的超时（grid 层），
  而且若用同一个 ctx，超时会让已经在途的加锁请求被取消，可能导致"节点其实加上了锁但客户端以为
  失败"的不一致。分离 ctx 让收集超时与 RPC 超时解耦。
- **异步释放"被遗弃的锁"**：客户端在 `locksFailed>tolerance` 时提前 break，但那些慢节点的加锁
  请求可能**之后才成功返回**（锁真的加上了）。这个 goroutine 等所有 RPC 完成后，把这些"主流程
  已经不要了"的锁逐个释放——否则它们会泄漏，直到过期。**这是极易被忽略但必须处理的边界。**
`──────────────────────────────────────────`

---

## 5. 锁续约：持锁者活着的证明

```go
// drwmutex.go:275
func (dm *DRWMutex) startContinuousLockRefresh(lockLossCallback func(), id, source string, quorum int) {
    go func() {
        refreshTimer := time.NewTimer(dm.refreshInterval)   // 默认 10s
        for {
            select {
            case <-ctx.Done(): return
            case <-refreshTimer.C:
                noQuorum, err := refreshLock(ctx, dm.clnt, id, source, quorum)
                if err == nil && noQuorum {
                    forceUnlock(ctx, dm.clnt, id)            // ★ 本地+远端强制清理
                    if lockLossCallback != nil { lockLossCallback() }  // ★ 通知调用方"锁丢了"
                    return
                }
                refreshTimer.Reset(dm.refreshInterval)
            }
        }
    }()
}
```
- `refreshLock`（`:339`）向所有节点发 `Refresh`，统计"还认账"的节点数。**不足 quorum → `noQuorum=true`**。
- **丢锁处理**：`forceUnlock` 广播 `ForceUnlock` 清理残留，并调 `lockLossCallback`。

`★ 续约为什么是分布式锁安全的核心 ─────────────────`
- 持锁的客户端可能**崩溃**。若锁永久有效，崩溃 = 这个资源永久死锁。续约把"我还活着"持续广播给
  各节点；节点侧 `expireOldLocks`（§6）会清理**太久没续约**的锁。两者配合实现"持锁者失联 →
  锁自动过期 → 别人能重新获取"。
- **`lockLossCallback` 的意义**：持锁者在临界区干活时若发现"锁已经因为多数节点失联而丢了"
  （比如自己被网络分区到少数派），必须**立刻停止对资源的操作**——因为此刻别的分区可能已经
  拿到了锁。回调就是给业务层"快收手"的信号。深读概览第 4 篇说的"不是线性一致的强锁"边界，
  正是靠这个回调缩小危险窗口。
`──────────────────────────────────────────`

---

## 6. 服务端 `localLocker`：内存持锁、过载拒绝、过期回收

每个节点跑一个 `localLocker`（`cmd/local-locker.go`），管理本节点视角的锁。

### 数据结构
```go
lockMap map[string][]lockRequesterInfo   // resource → 持锁者信息（写锁 1 个；读锁可多个）
lockUID map[string]string                // "UID+idx" → resource（反查，加速 Unlock/Refresh）
```

### `Lock`：过载拒绝 + 多资源原子
```go
// :99
func (l *localLocker) Lock(ctx, args) (bool, error) {
    if l.waitMutex.Load() > lockMutexWaitLimit {       // ★ 等待者过多 → 立刻拒绝（过载保护）
        l.locksOverloaded.Add(1); return false, nil
    }
    defer l.getMutex()()
    if !l.canTakeLock(args.Resources...) { return false, nil }   // ★ 任一资源已被锁 → 整体拒绝
    now := UTCNow()
    for i, resource := range args.Resources {          // ★ 一次性给所有资源加锁
        l.lockMap[resource] = []lockRequesterInfo{{
            Writer: true, Owner: args.Owner, UID: args.UID,
            Timestamp: now.UnixNano(), TimeLastRefresh: now.UnixNano(),
            Group: len(args.Resources) > 1, Quorum: *args.Quorum, idx: i,
        }}
        l.lockUID[formatUUID(args.UID, i)] = resource
    }
    return true, nil
}
```
`★ 服务端的三个讲究 ───────────────────────────`
- **`lockMutexWaitLimit` 过载拒绝**：等待获取内部 mutex 的请求太多时，直接返回 false（而非排队）。
  这把"锁服务过载"转化为"加锁失败"（客户端会退避重试），防止锁服务本身被打挂拖垮整个节点。
- **多资源 all-or-nothing**：`canTakeLock` 检查**所有** resource 都空闲才加；任一被占就整体拒绝。
  这避免"加了一半"的部分锁——批量操作（如多对象删除）要么全锁上、要么全不锁，从根上杜绝
  跨请求交叉加锁的死锁。
- **记录 `Quorum` 和时间戳**：节点存下这把锁的 quorum 要求和最后续约时间，供 `Refresh`/过期判断用。
`──────────────────────────────────────────`

### `Unlock`：UID + Owner 双匹配
```go
// :147 / removeEntry :172
// 只删 UID（且 Owner 匹配）的条目；写锁entity 才允许 Unlock
```
- 只有**同一 UID 且 Owner 匹配**的请求能解锁——防止 A 节点的请求误解 B 节点持的锁。

### `expireOldLocks`：清理失联持锁者
```go
// :407
func (l *localLocker) expireOldLocks(interval time.Duration) {
    // 遍历所有锁，TimeLastRefresh 早于 (now - interval) 的 → 删除
    delete(l.lockUID, formatUUID(lri.UID, lri.idx))
}
```
- 后台周期性调用。**持锁者崩溃后停止续约，它的锁 `TimeLastRefresh` 不再更新，超过阈值就被回收。**
  这是与客户端续约（§5）配对的另一半：客户端按时续约证明活着，服务端清理不续约的死锁。

---

## 7. 包装成 `RWLocker`：业务层看不到这些

业务代码不直接碰 `DRWMutex`，而是用统一接口（`cmd/namespace-lock.go`，深读概览第 4 篇）：
- `distLockInstance` 包 `dsync.DRWMutex`（分布式部署）。
- `localLockInstance` 包 `internal/lsync.LRWMutex`（单机部署，纯内存读写锁，无网络/quorum）。
- 工厂 `NewNSLock` 按 `isDistErasure` 决定用哪个。
- 典型用法：`lkctx, _ := nsLock.GetLock(ctx, timeout); defer nsLock.Unlock(lkctx)`。

**单机模式根本不走 dsync**——`LRWMutex` 就是个带超时和重试的本地读写锁，没有广播、没有 quorum、
没有续约。分布式的全部复杂度只在多节点时才付出。

---

## 8. 一页纸总结分布式锁的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 客户端 DRWMutex + 服务端 localLocker 两侧 | 请求方按 quorum 判定，各节点内存持锁 |
| 2 | UID + Owner 双身份 | 防冒用 UID 解别人的锁 |
| 3 | 写锁 quorum==tolerance 时 +1 | 防网络分区脑裂（两分区同时写成功） |
| 4 | 读锁不 +1 | 读读可并发，无脑裂问题 |
| 5 | 退避重试（250ms 起指数）；RetryInterval<0 = 非阻塞 | 竞争下不打爆节点；支持 TryLock |
| 6 | 失败数 > tolerance 提前放弃 | 不在注定失败的尝试上耗时 |
| 7 | 加锁失败必 releaseAll 回滚 | 否则半获取的锁让谁都拿不到（活锁） |
| 8 | NetLocker 调用用独立 Background ctx | 收集超时与 RPC 超时解耦，避免"加上了却以为失败" |
| 9 | 异步释放"放弃后才到达"的遗弃锁 | 防慢节点的成功加锁泄漏 |
| 10 | 续约 goroutine + noQuorum → forceUnlock + lockLossCallback | 持锁者崩溃可自动过期；分区时通知业务收手 |
| 11 | 服务端 lockMutexWaitLimit 过载拒绝 | 锁服务过载转为加锁失败，不拖垮节点 |
| 12 | 多资源 all-or-nothing 加锁 | 杜绝部分锁导致的交叉死锁 |
| 13 | expireOldLocks 清理失联持锁者 | 与客户端续约配对，无死锁 |
| 14 | 单机走 lsync.LRWMutex，不碰 dsync | 分布式复杂度只在多节点付出 |

下一篇深读：**Signature V4 签名校验**——canonical request 的逐字段构造、签名密钥派生、
streaming chunk 链式签名、presigned 与 STS 的差异、以及那些导致 `SignatureDoesNotMatch` 的坑。
