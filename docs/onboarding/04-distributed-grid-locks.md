# 04 · 分布式机制：节点通信与分布式锁

第 3 篇我们看到 `erasureObjects` 拿到的是一个 `[]StorageAPI`，里面混着本地盘和远端盘，
"一视同仁地并发调用"。本篇拆开这层魔法：远端盘的调用怎么走网络（`internal/grid`），
以及并发写同一个对象时怎么不打架（`internal/dsync` 分布式锁）。

## 4.1 节点拓扑：谁是本地，谁是远端

- `Endpoint`（`cmd/endpoint.go:71-76`）：每块盘对应一个 endpoint，带 `IsLocal`、
  `PoolIdx/SetIdx/DiskIdx`（它在拓扑里的坐标）。
- `IsLocal` 判定（`cmd/endpoint.go:116-124` `UpdateIsLocal`）：
  - 纯路径式端点（`/data/disk1`）→ 一定本地。
  - URL 式端点（`http://node2:9000/data/disk1`）→ 解析主机名/端口，是本机就 local。
- 启动时把所有 endpoint 解析成拓扑，决定每块盘用**本地 `xlStorage`** 还是
  **远端 `storageRESTClient`** 来实现 `StorageAPI`。

```
节点 A 视角：
  Set #0 的盘 = [ 本地disk1(xlStorage), 本地disk2, ..., 远端 B:disk1(RESTClient), 远端 C:disk1, ... ]
                  ↑ 直接读写本机                          ↑ 通过 grid/REST 代理到 B、C 节点执行
```

## 4.2 `internal/grid`：自研的节点间 RPC

MinIO 没用 gRPC 或 `net/rpc`，而是自研了 `grid`——基于**单条 WebSocket 连接 + 多路复用**的
消息框架。

### 为什么自研？相比 `net/rpc` 的优势
（`internal/grid/grid.go`、`connection.go`、`muxclient.go`、`muxserver.go`）

| 能力 | grid | net/rpc |
|------|------|---------|
| 连接复用 | 一对节点**一条** WebSocket，所有调用多路复用 | 每调用易起多连接 |
| 双向流 | 原生支持 streaming（如 NSScanner、ListDir） | 仅请求-响应 |
| 背压流控 | `outBlock` 令牌通道，防快producer压垮slow consumer | 无 |
| 消息合并 | `OpMerged` 一次发多条（`maxMergeMessages=50`） | 无 |
| 序列号 | `SendSeq/RecvSeq` 检测丢序/乱序 | 无 |
| 心跳保活 | ping/pong（`clientPingInterval=15s`） | 需自己实现 |

### 核心组件
- **`Manager`**（`internal/grid/manager.go:54`）：管理本节点到所有其它节点的连接
  （`targets map[host]*Connection`），本机不连自己。注册两类 handler：
  - `RegisterSingleHandler`（`:285`）：单请求-单响应（如 `DiskInfo`、`ReadAll`）。
  - `RegisterStreamingHandler`（`:330`）：双向流（如 `NSScanner` 扫描结果流式回传）。
- **`Connection`**（`internal/grid/connection.go:65`）：到某个远端的单条连接，内部维护：
  - `outgoing`：本端发起的多路复用流（`muxClient`）。
  - `inStream`：对端发起、本端处理的流（`muxServer`）。
  - `outQueue`：发送队列（缓冲 65535）。
  - 状态机：Unconnected → Connecting → Connected → (Error/Shutdown)。
- **`muxClient` / `muxServer`**：一次调用 = 一个 mux 流，靠 `MuxID` 在同一条 WebSocket 上区分。
  - `muxClient.roundtrip`（`muxclient.go:77`）：单请求往返，默认超时 1 分钟。
  - `muxServer.newMuxStream`（`muxserver.go:72`）：服务端为流创建处理 goroutine + 响应 goroutine，
    带出/入站容量做流控。

```
       节点 A                         一条 WebSocket                    节点 B
   ┌──────────────┐                                              ┌──────────────┐
   │ muxClient #1 │──MuxID=1──┐                          ┌──────▶│ muxServer #1 │
   │ muxClient #2 │──MuxID=2──┼── message{MuxID,Seq,Op}──┼──────▶│ muxServer #2 │
   │ muxClient #3 │──MuxID=3──┘   多路复用，互不阻塞      └──────▶│ muxServer #3 │
   └──────────────┘                                              └──────────────┘
```

`★ 设计洞察 ─────────────────────────────────`
- **一对节点一条连接**：大规模集群里，N 个节点两两通信若每次调用都开连接，会有 N² 量级的
  TCP/TLS 握手和文件描述符压力。grid 的多路复用把它压成"每对一条长连接"，握手成本一次性付清。
- **流式 + 背压**是为扫描、列举这类"结果可能海量"的场景设计的：服务端边产生边发，客户端
  消费慢就靠 `outBlock` 令牌自然减速，内存不爆。
`──────────────────────────────────────────`

### 存储调用如何走 grid（grid 优先，REST 后备）
- `storageRESTClient`（`cmd/storage-rest-client.go:160`）同时持有：
  - `restClient *rest.Client`（HTTP REST，后备）
  - `gridConn *grid.Subroute`（grid 连接，优先）
- 初始化时按盘路径取 grid 子路由（`:995` `gm.Connection(host).Subroute(path)`）。
- 存储 RPC 用 `grid.NewSingleHandler`/`NewStream` 定义（`storage-rest-client.go:59-77`），如
  `storageReadAllRPC`、`storageWriteMetadataRPC`、`storageRenameDataRPC`、`storageNSScannerRPC`。
- 调用示例：`DiskInfo()`（`:312`）走 `storageDiskInfoRPC.Call(ctx, client.gridConn, ...)`。

`★ 易错点 ─────────────────────────────────`
- **`diskID` 校验**：远端调用会带上期望的 `diskID`。如果远端那块盘被换过/格式化过，diskID
  对不上会拒绝操作——这防止"盘被搬到别的槽位"导致写错地方。调试"为什么远端调用报错 diskID
  mismatch"时，方向就在这。
`──────────────────────────────────────────`

## 4.3 `internal/dsync`：基于 Quorum 的分布式锁

多个客户端可能同时 PUT 同一个对象，或一边读一边删。MinIO 用分布式读写锁串行化这些操作。

### 核心：`DRWMutex`（分布式读写锁）
`internal/dsync/drwmutex.go:112-122`。
```go
type DRWMutex struct {
    Names           []string   // 要锁的资源（如 bucket/object）
    writeLocks      []string   // 每个节点上获得的锁 UID（写锁）
    readLocks       []string   // 读锁
    clnt            *Dsync     // 锁客户端（持有所有 NetLocker）
    refreshInterval time.Duration  // 续约间隔，默认 10s
}
```

### Quorum 计算（`drwmutex.go:217-234`）
```
tolerance = N / 2           // 能容忍多少节点挂
quorum    = N - tolerance   // 需要多少节点同意
if quorum == tolerance:     // 防脑裂
    quorum++
```
> 4 节点：tolerance=2, quorum=2，但 2==2 → quorum=3。即要 3 个节点都给锁才算获得。

### 加锁流程（`lock()` `:420-545`）
1. **广播**锁请求到所有 N 个节点（每个节点本地有个 `localLocker`）。
2. 收集响应：每个节点返回 `Granted{index, lockUID}`（成功有 UID，失败空）。
3. 等待直到：收齐全部 / 失败数超过 tolerance / 超时。
4. `checkQuorumLocked`（`:575`）：拿到锁的节点数 ≥ quorum 才算成功；否则**回滚**——
   把已经拿到的锁全部释放（`:524`），避免留下半获取的锁。

### 锁续约（`startContinuousLockRefresh` `:275` → `refreshLock` `:339`）
- 拿到锁后起一个 goroutine，每 10s 向所有节点 `Refresh`。
- 如果"刷新失败 + 锁不存在"的节点数超过 tolerance → 判定**锁已丢失**，触发
  `lockLossCallback`，`forceUnlock` 清理。

### 解锁（`Unlock` `:610`）
- 先停掉续约 goroutine，再**异步**向所有节点释放（不阻塞调用者，失败的锁会自然过期）。

```
   Client                node1   node2   node3   node4
     │   Lock("obj") ───▶  ✓       ✓       ✓       ✗(挂了)
     │   收到 3 个 grant ≥ quorum(3) → 获得锁
     │   每10s ─Refresh─▶  ✓       ✓       ✓
     │   ...临界区...
     │   Unlock ────────▶  释放    释放    释放   （异步，不等返回）
```

`★ 设计洞察 ─────────────────────────────────`
- **为什么要锁续约？** 持锁者可能崩溃。如果锁永久有效，崩溃就会死锁整个对象。续约 + 超时让
  "持锁者失联 → 锁自动过期 → 别人能抢" 成为可能，避免死锁。这是不依赖外部协调器还能安全的关键。
- **加锁失败要回滚**：拿到 2 个、需要 3 个时，必须把那 2 个还回去，否则别人也凑不齐 quorum，
  形成"谁都拿不到"的活锁。`checkQuorumLocked` 失败即 release 是必须的。
- **quorum 锁不是线性一致的强锁**，它是"实用的、能容忍少数节点故障的互斥"。极端网络分区下
  仍有理论上的边界情况，MinIO 用"写 quorum > 读 quorum 重叠"等手段把窗口压到很小。
`──────────────────────────────────────────`

## 4.4 `cmd` 层的统一锁接口：namespace lock

业务代码不直接用 `DRWMutex`，而是用统一的 `RWLocker` 接口，屏蔽"单机 vs 分布式"。

- 接口 `RWLocker`（`cmd/namespace-lock.go:40-45`）：`GetLock/Unlock/GetRLock/RUnlock`。
- 两个实现：
  - `distLockInstance`（`:157`）：包 `dsync.DRWMutex`，分布式模式用。
  - `localLockInstance`（`:221`）：包 `internal/lsync.LRWMutex`（纯内存读写锁），单机模式用。
- 工厂 `nsLockMap.NewNSLock`（`:231`）：分布式部署返回 `distLockInstance`，否则
  `localLockInstance`。

### 典型用法（你写 handler 时会反复见到）
```go
nsLock := er.NewNSLock(bucket, object)
lkctx, err := nsLock.GetLock(ctx, globalOperationTimeout)
if err != nil { return err }          // 超时/未获得
defer nsLock.Unlock(lkctx)
// ... 临界区：安全地读改写这个对象 ...
```
读多写少的场景用 `GetRLock/RUnlock`（多个读者可并发）。

`★ 易错点 ─────────────────────────────────`
- **锁的粒度与顺序**：批量删除多个对象时，对多个路径加锁要保证**全局一致的顺序**，否则两个
  并发批量操作交叉加锁会死锁。`localLockInstance.GetLock` 对多路径顺序加锁、任一失败就回滚
  已获得的（`cmd/namespace-lock.go:245-267`），就是为了这个。
- **`GetLock` 用 `dynamicTimeout`**：超时是自适应的——历史上获取越慢，超时给得越宽。别硬编码
  固定超时去替换它，会在高负载下误判超时。
- **`Unlock` 是异步释放**，调用返回不代表远端已释放完。所以不要"Unlock 后立刻假设别人能马上
  拿到锁"来写测试。
`──────────────────────────────────────────`

## 4.5 关键参数速查

| 组件 | 参数 | 默认 | 位置 |
|------|------|------|------|
| grid | clientPingInterval | 15s | grid.go |
| grid | 单请求超时 | 1m | grid.go |
| grid | maxMergeMessages | 50 | grid.go |
| Connection | outQueue 缓冲 | 65535 | connection.go |
| dsync | Acquire 超时 | 1s | drwmutex.go:71 |
| dsync | 续约间隔 | 10s | drwmutex.go:83 |
| dsync | Unlock 超时 | 30s | drwmutex.go:77 |
| lsync | 重试间隔 | 50ms | lrwmutex.go |

## 4.6 本篇要点回顾

- 远端盘 = `storageRESTClient`，把 `StorageAPI` 调用代理到目标节点；**grid 优先、REST 后备**。
- `grid` 用单条 WebSocket + 多路复用，支持双向流、背压、心跳，专为大集群 N² 通信优化。
- 分布式锁 `DRWMutex` 靠 **quorum + 续约 + 超时** 实现无外部协调器的互斥，加锁失败必回滚。
- 业务层用统一的 `RWLocker`/namespace lock，屏蔽单机与分布式差异；注意锁粒度、顺序、动态超时。

下一篇转向安全面：请求怎么验签、IAM 怎么存怎么评估、对象怎么加密。
