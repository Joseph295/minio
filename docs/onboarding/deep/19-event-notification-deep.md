# 深读 19 · 事件通知体系逐层精读

> S3 bucket notification：对象被 PUT/DELETE 等操作时，向外部系统（webhook/Kafka/NATS/Redis/
> AMQP/MQTT/NSQ/PostgreSQL/MySQL/Elasticsearch）投递事件。本篇拆开事件匹配规则、target 抽象、
> `internal/store` 持久化队列的 store-and-forward、同步/异步双模式与背压
> （`internal/event/` + `internal/store/`）。

---

## 1. 事件匹配：三层映射

通知配置是"什么事件 + 什么对象 → 发给哪些 target"。匹配结构是三层（`internal/event/`）：
```go
// rulesmap.go:21
type RulesMap map[Name]Rules              // 事件名（s3:ObjectCreated:Put...）→ Rules
// rules.go:50
type Rules    map[string]TargetIDSet      // 对象名 pattern（前缀/后缀通配）→ target 集合
```
```go
// rules.go:68  按对象名匹配出 target 集合
func (rules Rules) Match(objectName string) TargetIDSet {
    targetIDs := NewTargetIDSet()
    for pattern, targetIDSet := range rules {
        if wildcard.MatchSimple(pattern, objectName) {   // ★ 通配匹配
            targetIDs = targetIDs.Union(targetIDSet)
        }
    }
    return targetIDs
}
```
`★ 匹配流程 ───────────────────────────────────`
- 一个事件发生（如 `s3:ObjectCreated:Put`，对象 `photos/cat.jpg`）：
  1. 用**事件名**查 `RulesMap` → 得到该事件的 `Rules`。
  2. 用**对象名**在 `Rules` 里通配匹配（如 `photos/*.jpg` → target X）→ 得到 `TargetIDSet`。
  3. 把事件发给这些 target。
- **`TargetIDSet`（集合 + Union）**：一个对象可能匹配多条 pattern，各自指向不同 target，Union
  合并去重。一个事件可同时发给多个目标。
- **ARN（`arn.go`）标识 target**：`arn:minio:sqs::<id>:<type>`，配置里用 ARN 引用 target。
`──────────────────────────────────────────`

---

## 2. Target 抽象：统一所有外部系统

```go
// targetlist.go:40
type Target interface {
    ID() TargetID
    IsActive() (bool, error)         // target 是否可达
    Save(Event) error                // ★ 保存事件（持久化或直发）
    SendFromStore(store.Key) error   // ★ 从持久化队列回放一条事件
    Close() error
    Store() TargetStore              // 返回它的持久化队列（可能 nil）
}
```
- `internal/event/target/` 下每种外部系统（webhook/kafka/nats/...）实现这个接口。**上层只管
  `Save(event)`，不关心底层是 HTTP POST 还是 Kafka produce**——又一处接口抽象吃掉异构（呼应深读
  01 StorageAPI、深读 18 WarmBackend）。

---

## 3. Store-and-Forward：持久化队列 + 回放

这是事件系统最关键的设计。以 webhook 为例（`target/webhook.go`）：
```go
// :147 Save：有 store → 落盘；无 store → 直发
func (target *WebhookTarget) Save(eventData event.Event) error {
    if target.store != nil {
        _, err := target.store.Put(eventData)   // ★ 持久化到磁盘队列，立即返回
        return err
    }
    // 无 store：同步直发，失败即返回
    return target.send(eventData)
}
// :212 SendFromStore：回放一条存储的事件
func (target *WebhookTarget) SendFromStore(key store.Key) error {
    eventData, _ := target.store.Get(key)       // 从磁盘读出
    // send；成功 → 删除该条；失败（target down）→ 保留待重试
}
// :165 send：真正投递（HTTP POST + 认证 + 2xx 检查）
func (target *WebhookTarget) send(eventData event.Event) error {
    data, _ := json.Marshal(event.Log{...})
    req, _ := http.NewRequest(POST, endpoint, data)
    // Authorization: Bearer <token>
    resp, _ := target.httpClient.Do(req)
    if resp.StatusCode in 2xx { return nil }
    // 否则报错（403 提示检查 token）
}
```
`★ Store-and-Forward 的全部价值 ─────────────────`
- **两种模式**：配了 `store`（持久化队列）→ **异步耐久**：`Save` 只把事件写盘就返回，不阻塞前台
  请求，由后台回放循环慢慢投递。没配 store → **同步直发**：投递失败立刻让前台请求知道。
- **耐久性**：事件先落盘（`QueueStore.Put`），**即使 MinIO 重启、即使 target（如 webhook 服务）
  宕机，事件不丢**。target 恢复后，回放循环把积压的事件逐条 `SendFromStore` 投出去。这就是
  "store and forward"——存下来，转发出去。
- **回放 + 删除**：`SendFromStore` 成功才删除磁盘上那一条；失败就留着下次重试。**至少一次投递**
  语义（at-least-once）——target 端要能处理重复事件（幂等）。
- **网络抖动转 `ErrNotConnected`**（`:157`）：投递时网络/主机不通 → 返回 `store.ErrNotConnected`，
  回放循环据此知道"target 还没好，稍后再试"，而非把事件当作处理失败丢弃。
`──────────────────────────────────────────`

---

## 4. 持久化队列 `QueueStore`

```go
// store/queuestore.go:56  泛型文件队列
func NewQueueStore[I any](directory string, limit uint64, ext string) *QueueStore[I]
// :186 Put
func (store *QueueStore[I]) Put(item I) (Key, error) {
    if len(store.entries) >= store.entryLimit { return Key{}, errLimitExceeded }  // ★ 队列上限
    uid, _ := uuid.NewRandom()
    key := Key{Name: uid.String(), Extension: store.fileExt}
    return key, store.write(key, item)   // 写一个 UUID 命名的文件
}
// :253 Get：读回；读失败（损坏/不存在）→ 删除该条
```
`★ 细节 ────────────────────────────────────`
- **每个事件 = 一个 UUID 命名的文件**，存在 target 专属目录。简单、崩溃安全（写一个文件是原子的）、
  重启后目录还在 = 队列还在。
- **`entryLimit` 上限 → `errLimitExceeded`**：队列不能无限堆。target 长时间宕机、事件堆到上限，
  新事件被拒（背压）。防止一个挂掉的 target 把磁盘撑爆。
- **读损坏自动删**（`GetRaw` 的 defer）：读到损坏/空文件就删掉该条，不让一条坏事件卡住回放。
- 支持 **s2 压缩**（`key.Compress`）：事件 JSON 可压缩存储省空间。
`──────────────────────────────────────────`

---

## 5. 同步 vs 异步发送 + 背压

```go
// targetlist.go:262
func (list *TargetList) Send(event, targetIDset, sync bool) {
    if sync { list.sendSync(...) } else { list.sendAsync(...) }
}
// :270 sendSync：每个 target 一个 goroutine 并行 Save，等全部完成
func (list *TargetList) sendSync(event, targetIDset) {
    for id := range targetIDset { go func(){ target.Save(event); ... }() }
    wg.Wait()
}
// :299 sendAsync：投进 list.queue（非阻塞）
func (list *TargetList) sendAsync(event, targetIDset) {
    select {
    case list.queue <- asyncEvent{ev: event, targetSet: targetIDset.Clone()}:
    case <-list.ctx.Done(): list.eventsSkipped.Add(...); return
    default:                                            // ★ 队列满 → 丢弃 + 计数 + 警告
        list.eventsSkipped.Add(1)
        // "concurrent target notifications exceeded ... target is too slow"
    }
}
```
`★ 多层背压 ───────────────────────────────────`
- 通知系统有**三道背压闸**，层层防止"慢 target 拖垮前台"：
  1. **`sendAsync` 的 `list.queue` 满** → 丢弃事件 + `eventsSkipped` 计数 + 警告"target 太慢"。
  2. **`QueueStore.entryLimit` 满** → `errLimitExceeded`，新事件存不进。
  3. **`maxConcurrentAsyncSend`** 并发投递上限。
- **设计取舍：宁可丢事件，不拖垮请求**。一个 PUT 触发通知，绝不能因为下游 webhook 慢就让 PUT
  变慢。所以满了就丢 + 计数（运维可监控 `eventsSkipped`），保护前台 SLA。这与深读 11 复制、
  深读 16 metrics 的"宁可降级不阻塞"是同一种工程价值观。
- **sync 模式**（如某些必须确认的场景）才等投递完成——但默认大多异步。
`──────────────────────────────────────────`

---

## 6. 事件从哪触发

- handler（PUT/DELETE 等）完成后调 `sendEvent`（`cmd/notification.go`），它用 `globalNotificationSys`
  按 bucket 的通知配置（`RulesMap`，从 `bucket-notification-handlers.go` 加载的 XML 配置解析而来）
  匹配出 target，调 `TargetList.Send`。
- 事件 `Event`（`event.go:78`）含 EventName、S3 桶/对象信息、时间、source IP、user identity 等——
  下游能据此做审计、触发后续处理（如缩略图生成、索引更新）。

---

## 7. 一页纸总结事件通知的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | RulesMap[事件名]→Rules[对象pattern]→TargetIDSet 三层匹配 | 精确"什么事件+什么对象→哪些 target" |
| 2 | 通配 Match + TargetIDSet Union | 一对象匹配多 pattern，一事件发多 target |
| 3 | Target 接口统一所有外部系统 | 上层只管 Save，不关心 HTTP/Kafka/... |
| 4 | Save：有 store 落盘异步、无 store 同步直发 | 耐久异步 vs 快速失败两模式 |
| 5 | QueueStore = UUID 文件持久化队列 | 重启/target 宕机事件不丢 |
| 6 | SendFromStore 回放，成功才删 | at-least-once，target 需幂等 |
| 7 | ErrNotConnected 区分"target 没好"vs"处理失败" | 不把可重试当作丢弃 |
| 8 | entryLimit + 读损坏自删 | 队列有界、坏事件不卡回放 |
| 9 | 三道背压（queue/store/并发上限） | 慢 target 不拖垮前台请求 |
| 10 | 宁可丢事件 + eventsSkipped 计数 | 保护前台 SLA，运维可监控 |
| 11 | 默认异步、sync 可选 | 多数场景不阻塞写路径 |

下一篇深读（系列三度收官）：**S3 Select 查询下推**——SQL 解析、列式/行式读取、表达式求值、
压缩/加密流的下推处理。
