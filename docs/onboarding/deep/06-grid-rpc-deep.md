# 深读 06 · `internal/grid` 节点间 RPC 逐层精读

> 深读 04/05 里，远端盘的 `StorageAPI` 调用要"通过网络代理到目标节点"。承载这个代理的就是
> `internal/grid`——MinIO 自研的、基于**单条 WebSocket + 多路复用**的节点间消息框架。本篇拆开
> 连接状态机、握手与重连、mux 流的建立与拆除、双层心跳、背压与消息合并。

---

## 1. 为什么不用 gRPC：一对节点一条连接

大集群里 N 个节点两两通信。若每次 RPC 都开连接，会有 N² 量级的握手与 fd 压力。grid 的核心是
**每对节点只维持一条长连接，所有调用在上面多路复用**——握手成本一次性付清。

```go
// connection.go:65
type Connection struct {
    NextID   uint64                          // 下一个 MuxID（atomic 自增）
    LastPong int64                           // 最近一次连接级 pong 时间（atomic）
    state    State                           // 连接状态（atomic）
    outgoing *xsync.MapOf[uint64, *muxClient] // 本端发起的流（按 MuxID）
    inStream *xsync.MapOf[uint64, *muxServer] // 对端发起、本端处理的流
    outQueue chan []byte                     // 发送队列（缓冲 65535）
    ...
}
// :196
defaultOutQueue = 65535     // "kind of close to max open fds per user"
readBufferSize  = 32 << 10  // 32 KiB，注释：Linux 上最优
connPingInterval= 10 * time.Second
connWriteTimeout= 3 * time.Second
```
`★ 细节 ────────────────────────────────────`
- **一条连接里有两张 mux 表**：`outgoing`（我作为客户端发起的流）和 `inStream`（对端作为客户端
  发起、我作为服务端处理的流）。同一条 WebSocket 是**全双工**的——两个方向都能发起 RPC，靠 MuxID
  区分各自的流。用 `xsync.MapOf`（分片并发 map）避免锁竞争。
- **`outQueue` 缓冲 65535**：注释点出这个数接近"单用户最大打开 fd 数"的量级。所有出站消息先进
  这个 channel，由发送 goroutine 取出写 socket——把"业务 goroutine 调用"和"socket 写"解耦。
- **buffer 32 KiB**：注释"the most optimal on Linux"——经验值，平衡系统调用次数与内存。
`──────────────────────────────────────────`

---

## 2. 连接状态机（5 态）

```go
// :161
StateUnconnected      // 初始；第一条消息触发连接
StateConnecting       // 正在连
StateConnected        // 已建立且稳定（丢了会回到 Connecting）
StateConnectionError  // 连接尝试失败，停在此态直到重连成功
StateShutdown         // 服务器关闭
// :184
MaxDeadline = math.MaxUint32 ms ≈ 49 天   // deadline 用 uint32 毫秒编码的上限
```
- **`MaxDeadline ≈ 49 天`**：消息里 `DeadlineMS` 是 `uint32` 毫秒（深读概览第 4 篇），最大约 49 天。
  这不是业务超时，是协议字段的物理上限。

---

## 3. 握手与重连：`connect()` 死循环

```go
// :640
func (c *Connection) connect() {
    c.updateState(StateConnecting)
    for {                                              // 跑到服务器关闭为止
        if c.State() == StateShutdown { return }
        conn, err := c.dial(c.ctx, c.Remote)
        retry := func(err error) {                     // ★ 带抖动的退避重连
            sleep := defaultDialTimeout + rng.Int63n(defaultDialTimeout)  // 2s + [0,2s) 抖动
            ...
            c.updateState(StateConnectionError)
            time.Sleep(sleep)
        }
        if err != nil { retry(err); continue }

        // 发 OpConnect 握手
        req := connectReq{Host: c.Local, ID: c.id, Time: time.Now()}
        req.addToken(c.authFn)                          // ★ 带认证 token
        c.sendMsg(conn, message{Op: OpConnect}, &req)

        var r connectResp
        c.receive(conn, &r)
        if !r.Accepted { retry(...); continue }         // 对端拒绝（认证失败等）

        remoteUUID := uuid.UUID(r.ID)
        if c.remoteID != nil { c.reconnected() }        // ★ 之前连过 → 这是重连
        c.remoteID = &remoteUUID

        c.handleMessages(c.ctx, conn)                   // 进入消息收发循环，直到断开
        if c.State() == StateShutdown { conn.Close(); return }
        // 否则循环重连
    }
}
```
`★ 握手与重连的讲究 ───────────────────────────`
- **退避带随机抖动**：`2s + rand([0,2s))`。如果所有节点同时重连（比如某节点重启），无抖动会
  造成"惊群"同步重连。抖动把重连时刻打散。
- **`connectReq` 带认证 token**（`addToken(c.authFn)`）：节点间不是裸连——握手要过认证，对端
  `connectResp.Accepted=false` 就拒绝。防止未授权节点混入集群。
- **`remoteID` 跟踪对端实例身份**：对端的 UUID（`r.ID`）在它重启后会变。本端发现 `remoteID`
  变了，就知道"对端是新进程了"（不是网络抖动），据此决定是否清理旧状态。
- **`reconnected()` vs `disconnected()`**：见 §4。
`──────────────────────────────────────────`

---

## 4. 断开时的清理：取消所有在途流

```go
// :737
func (c *Connection) disconnected() {
    c.outgoing.Range(func(key uint64, client *muxClient) bool {
        if !client.stateless { client.cancelFn(ErrDisconnected) }   // ★ 唤醒所有等待响应的调用
        return true
    })
    c.outgoing.Clear()
    c.inStream.Range(func(key uint64, client *muxServer) bool {
        client.cancel()                                              // ★ 取消所有正在处理的服务端流
        return true
    })
    c.inStream.Clear()
}
```
`★ 这一步为什么关键 ───────────────────────────`
- 连接断了，所有"正在这条连接上等响应"的 `muxClient` 必须被**立即用 `ErrDisconnected` 唤醒**——
  否则它们会傻等到各自的超时（默认 1 分钟），调用方被无谓阻塞。`cancelFn(ErrDisconnected)` 让
  `roundtrip` 的 `select` 立刻从 `ctx.Done()` 返回。
- 服务端侧的 `muxServer`（正在为对端处理流）也要 `cancel()`，停止徒劳的工作（对端已经收不到了）。
- **`stateless` 流不取消**：无状态流（fire-and-forget）断开无所谓，不需要唤醒。
`──────────────────────────────────────────`

---

## 5. mux 多路复用：一次调用 = 一个 MuxID

### 客户端单请求往返 `roundtrip`
```go
// muxclient.go:77
func (m *muxClient) roundtrip(h HandlerID, req []byte) ([]byte, error) {
    m.singleResp = true
    msg := message{
        Op:         OpRequest,
        MuxID:      m.MuxID,
        Handler:    h,
        Flags:      m.BaseFlags | FlagEOF,             // ★ 单请求带 EOF：告诉对端"就这一条"
        Payload:    req,
        DeadlineMS: uint32(m.deadline.Milliseconds()),
    }
    ch := make(chan Response, 1)
    m.respWait = ch                                     // 响应会被投到这个 channel
    if msg.DeadlineMS == 0 {                            // 没设 deadline → 默认 1 分钟
        msg.DeadlineMS = uint32(defaultSingleRequestTimeout / time.Millisecond)
        ctx, cancel = context.WithTimeout(ctx, defaultSingleRequestTimeout); defer cancel()
    }
    m.send(msg)
    select {
    case v, ok := <-ch:  if !ok { return nil, ErrDisconnected }; return v.Msg, v.Err
    case <-ctx.Done():   return nil, context.Cause(ctx)   // 超时或断开
    }
}
```

### 发送 `sendLocked`：序列号、子路由、CRC
```go
// muxclient.go:148
func (m *muxClient) sendLocked(msg message) error {
    dst := GetByteBufferCap(msg.Msgsize())              // ★ 从池借 buffer
    msg.Seq = m.SendSeq                                 // ★ 序列号（检测丢序）
    msg.MuxID = m.MuxID
    m.SendSeq++
    dst, _ = msg.MarshalMsg(dst)
    if msg.Flags&FlagSubroute != 0 {                    // ★ 子路由：把目标 handler id 附在消息后
        hid := m.subroute.withHandler(msg.Handler)
        dst = append(dst, hid[:]...)
    }
    if msg.Flags&FlagCRCxxh3 != 0 {                     // ★ 可选 xxh3 CRC 校验
        h := xxh3.Hash(dst)
        dst = binary.LittleEndian.AppendUint32(dst, uint32(h))
    }
    return m.parent.send(m.ctx, dst)                    // 投进 outQueue
}
```
`★ mux 的几个关键设计 ─────────────────────────`
- **`FlagEOF` 区分单请求 vs 流**：单请求（`roundtrip`）带 `FlagEOF` 表示"发完就结束"；流式
  调用不带 EOF，可以持续发多条。同一套消息格式承载两种语义。
- **`SendSeq` 序列号**：每条消息自增。对端用它检测消息丢失/乱序（WebSocket 本身有序，但 grid
  在应用层再加一道，用于流控和重连恢复的正确性）。
- **`FlagSubroute` + handler id 后缀**：子路由（见 §6）的目标 handler（比如"node B 的 disk3"）
  编码进消息尾部，让同一条连接能路由到对端的不同处理器。
- **`FlagCRCxxh3` 可选 CRC**：对消息算 xxh3 附在尾部，防止传输损坏（WebSocket 有帧校验，这是
  额外一层，可配）。
- **`GetByteBufferCap`/`PutByteBuffer` 全程池化**：每条消息的序列化 buffer 都从池借还，热路径
  零分配。这是 grid 高吞吐的基础。
`──────────────────────────────────────────`

---

## 6. Subroute：一条连接路由到对端的多个处理器

```go
// connection.go:138
type Subroute struct {
    *Connection            // 嵌入连接
    route string           // 子路由路径（如磁盘路径）
    subID subHandlerID
}
```
- 深读 04 里 `storageRESTClient` 取 grid 连接是 `gm.Connection(host).Subroute(diskPath)`。一个节点
  上有多块盘，每块盘是一个"子路由"。`Subroute` 复用底层 `Connection`（嵌入），但把 `subID`
  绑定到具体磁盘路径。
- 发消息时带 `FlagSubroute`，对端据此把请求派发到"那块盘"的处理器。**一条物理连接服务一个节点上
  所有磁盘的所有 RPC**——这是"一对节点一条连接"能成立的关键：连接是节点级的，路由是磁盘级的。

---

## 7. 双层心跳：连接级 + 流级

```go
// connection.go:1491 handlePing
if m.MuxID == 0 {                          // ★ 连接级 ping（MuxID==0）
    c.queueMsg(m, &pongMsg{T: ping.T})     // 直接回 pong
    return
}
if v, ok := c.inStream.Load(m.MuxID); ok { // ★ 流级 ping（针对某个 mux）
    pong := v.ping(m.Seq); c.queueMsg(m, &pong)
} else {
    c.queueMsg(m, &pongMsg{NotFound: true, T: ping.T})  // 这个流已不存在
}
// :1466 handlePong
if m.MuxID == 0 {
    atomic.StoreInt64(&c.LastPong, time.Now().UnixNano())  // 更新连接活性
    c.lastPingDur.Store(int64(time.Since(pong.T)))         // 记录 RTT
    return
}
if v, ok := c.outgoing.Load(m.MuxID); ok { v.pong(pong) }  // 喂给对应的 muxClient
```
`★ 为什么要两层心跳 ───────────────────────────`
- **连接级心跳（MuxID==0）**：探测"这条 WebSocket 还活着吗"。`LastPong` 太久没更新 → 判定连接死
  → 触发重连。`lastPingDur` 顺便测 RTT 供监控。
- **流级心跳（MuxID!=0）**：探测"这个长流的对端还在处理吗"。一个流可能跑很久（如全量扫描结果
  流式回传），中途对端崩了，靠流级 ping/pong 及时发现并取消，不必等连接级超时。
- **`NotFound: true`**：流级 ping 打到一个已经不存在的 mux，回 NotFound，让对端清理本地残留的
  muxClient（呼应 handlePong 里"流不在就发 OpDisconnectClientMux"）。
`──────────────────────────────────────────`

---

## 8. 背压与消息合并（概览回顾 + 落点）

- **背压**：服务端 `muxServer` 用 `outBlock` 令牌通道（深读概览第 4 篇）。客户端消费慢 → 不回
  令牌 → 服务端 `sendResponses` 阻塞 → 不再从处理器抽数据 → 自然减速。流式大结果不爆内存。
- **消息合并 `OpMerged`**：发送 goroutine 从 `outQueue` 一次取多条消息（上限 `maxMergeMessages=50`）
  打包成一条 `OpMerged` 发出，减少 socket 写系统调用次数。高频小消息场景（如大量元数据读）受益明显。

---

## 9. 一页纸总结 grid 的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 一对节点一条 WebSocket，全双工双向发起 | 大集群 N² 通信只付一次握手成本 |
| 2 | outgoing/inStream 两张 mux 表（xsync 分片 map） | 同连接区分双向流，低锁竞争 |
| 3 | outQueue 缓冲 65535 解耦业务与 socket 写 | 发送不阻塞业务 goroutine |
| 4 | 5 态状态机 + MaxDeadline 49 天 | 连接生命周期管理，协议字段上限 |
| 5 | 握手带认证 token，对端可拒绝 | 防未授权节点入集群 |
| 6 | 重连退避带随机抖动 | 防节点同时重连惊群 |
| 7 | remoteID 跟踪对端实例 UUID | 区分"网络抖动"与"对端重启" |
| 8 | disconnected 立即用 ErrDisconnected 唤醒所有在途流 | 调用方不傻等超时 |
| 9 | FlagEOF 区分单请求/流 | 一套消息格式两种语义 |
| 10 | SendSeq 序列号 | 应用层检测丢序 |
| 11 | Subroute：连接节点级、路由磁盘级 | 一条连接服务一节点所有盘的 RPC |
| 12 | 双层心跳（连接级 + 流级） | 连接死与长流对端死分别及时发现 |
| 13 | 全程 byte buffer 池化 + OpMerged 合并 | 热路径零分配、减少 socket 写 |
| 14 | 背压令牌（outBlock） | 流式大结果不爆内存 |

下一篇深读：**`internal/dsync` 分布式锁**——quorum 加锁的广播与收集、锁续约的 goroutine、
丢锁回调、加锁失败的回滚、以及 namespace lock 如何把它包装成 `RWLocker`。
