# 深读 15 · Site Replication 站点级全量同步逐层精读

> Site Replication 让多个 MinIO 部署（站点）成为**对等的镜像**——不只复制对象数据，还复制
> IAM、bucket 配置、生命周期规则等"一切"。本篇拆开对等拓扑与状态、变更 hook 的广播传播、
> 时间戳冲突解决、两阶段 heal 自愈（`cmd/site-replication.go`）。

---

## 1. 与 Bucket Replication 的本质区别

| | Bucket Replication（深读 11） | Site Replication |
|---|---|---|
| 粒度 | bucket → bucket | 整个部署 → 整个部署 |
| 内容 | 仅对象数据 | **IAM + bucket 配置 + 对象 + ILM 规则 + 一切** |
| 方向 | 单/双向 | 多向对等（n-way） |
| 配置 | bucket 复制规则 | admin API 一次建立 |
| 目标 | 数据容灾 | 多数据中心多活 |

Site Replication = "把整个集群（含配置和身份）镜像到其它站点"。对象数据的复制**底层仍复用
bucket replication**（建站点时自动给每个 bucket 配好复制规则），但**配置/IAM 的同步是 site
replication 独有的**——这是本篇重点。

---

## 2. 状态：对等 Peer 拓扑

```go
// :198
type SiteReplicationSys struct {
    enabled bool
    state   srState           // 持久化的多站点状态
    iamMetaCache srIAMCache
}
// :213
type srStateV1 struct {
    Name  string
    Peers map[string]madmin.PeerInfo  // ★ 按 deploymentID 索引的所有对等站点
    ServiceAccountAccessKey string     // ★ 跨站点认证用的服务账号
    UpdatedAt time.Time
}
```
`★ 细节 ────────────────────────────────────`
- **`Peers` 按 deploymentID 索引**（深读 05：每个集群的终生唯一 ID）。每个站点都持有完整的 peer
  列表——**对等而非主从**，没有"主站点"。任何站点改了 IAM/配置，都向所有其它 peer 广播。
- **`ServiceAccountAccessKey`**：站点间互相调用 admin API 要认证。建立 site replication 时创建一个
  专用服务账号，各站点用它的凭证互信。这避免用 root 凭证跨站点传输。
- **状态持久化**（`saveToDisk`/`loadFromDisk`，存 `.minio.sys`）：拓扑信息本身也要落盘，重启
  后能恢复"我和谁组成了 site replication"。
`──────────────────────────────────────────`

`Init`（`:230`）：起 `startHealRoutine` + 带重试地 `loadFromDisk`（加载失败退避重试，因为对象层
可能还没完全就绪——又一处启动顺序依赖）。

---

## 3. 变更传播：hook + concDo 广播

核心模式：**本地先应用变更 → 向所有 peer 广播 → peer 应用（带冲突检测）**。

### 发起侧：hook + `concDo`
```go
// :1208 IAMChangeHook（改了 IAM 后调用）
func (c *SiteReplicationSys) IAMChangeHook(ctx, item madmin.SRIAMItem) error {
    // 注释：item 已经应用到本地集群，这里只需推给所有远端 peer
    cerr := c.concDo(nil, func(d string, p madmin.PeerInfo) error {
        admClient, _ := c.getAdminClient(ctx, d)
        return c.annotatePeerErr(p.Name, replicateIAMItem, admClient.SRPeerReplicateIAMItem(ctx, item))
    }, replicateIAMItem)
}
```
```go
// :2282 concDo：并行对所有 peer 执行 peerActionFn
func (c *SiteReplicationSys) concDo(selfActionFn, peerActionFn, actionName) error {
    for i := range depIDs {
        go func(i int) {
            if depIDs[i] == globalDeploymentID() { errs[i] = selfActionFn() }   // 自己
            else { errs[i] = peerActionFn(depIDs[i], peers[depIDs[i]]) }         // 远端 peer
        }(i)
    }
    wg.Wait()
    // ★ 网络不通的 peer 标记为 offline
    for i, depID := range depIDs {
        if errs[i] != nil && minio.IsNetworkOrHostDown(errs[i]) { globalBucketTargetSys.markOffline(epURL) }
    }
}
```
`★ 传播设计 ───────────────────────────────────`
- **本地先应用、再广播**（注释 `:1209` "already been applied to the local cluster"）：改 IAM 的
  那个站点先在本地生效，然后异步推给所有 peer。本地立即可用，远端最终一致。
- **`concDo` 并行广播 + offline 标记**：同时推给所有 peer，不串行等。某个 peer 网络不通 → 标记
  offline，**不阻塞其它 peer 的传播**。offline 的 peer 之后靠 heal routine（§5）补齐。
- **有非常多种 hook**：`IAMChangeHook`、`BucketMetaHook`、`MakeBucketHook`、`DeleteBucketHook`……
  覆盖用户/组/策略/SA/STS + bucket 的 versioning/objectlock/SSE/policy/tags/quota/LC——**几乎每种
  配置变更都有对应 hook 往 peer 广播**。这就是"复制一切配置"的实现：每个配置写入点都挂了广播。
`──────────────────────────────────────────`

### 接收侧：应用 + 时间戳冲突解决
```go
// :1233 PeerAddPolicyHandler（peer 收到策略变更）
func (c *SiteReplicationSys) PeerAddPolicyHandler(ctx, policyName, p, updatedAt) error {
    // ★ 冲突解决：本地版本比对方新 → 忽略对方的（防止旧覆盖新）
    if !updatedAt.IsZero() {
        if doc, err := globalIAMSys.store.GetPolicyDoc(policyName); err == nil && doc.UpdateDate.After(updatedAt) {
            return nil   // 本地更新，跳过
        }
    }
    if p == nil { globalIAMSys.DeletePolicy(...) } else { globalIAMSys.SetPolicy(...) }
}
```
`★ 时间戳 last-writer-wins 冲突解决 ───────────────`
- 对等多活下，两个站点可能"同时"改同一个策略。每条变更带 `updatedAt` 时间戳；peer 收到时
  **若本地版本的时间戳更新，就忽略对方的**（`doc.UpdateDate.After(updatedAt)` → return nil）。
  这是 **last-writer-wins（最后写入者胜）** 的冲突解决——按时间戳取最新。
- 这意味着 site replication 的一致性是**最终一致 + LWW**，不是强一致。两站点并发改同一对象的
  极端竞态下，时间戳较早的那次更新会被丢弃。理解这点对运维多活很重要——它不保证"两次并发写
  都保留"，而是"收敛到时间戳最新的那个"。
- **依赖站点间时钟**：LWW 靠时间戳，所以各站点 NTP 同步很重要（呼应深读 08 签名也依赖时钟）。
`──────────────────────────────────────────`

---

## 4. 加入站点（建立拓扑）

`AddPeerClusters`（`:397`）建立 site replication：
- 收集各站点信息（`getSiteStatuses`）、校验身份提供方一致（`validateIDPSettings`——各站点的
  OpenID/LDAP 配置必须兼容，否则身份无法互通）。
- `PeerJoinReq`（`:614`）让每个站点把彼此加进 `Peers`。
- 把现有的 IAM、bucket、配置**全量同步**给新加入的站点（初始全量，之后增量靠 hook）。
- 给每个 bucket 自动配好双向复制规则（对象数据走 bucket replication）。

---

## 5. 两阶段 heal：补齐漏掉的变更

广播可能因 peer offline 而漏传，所以需要周期性 heal 兜底。
```go
// :4265
func (c *SiteReplicationSys) startHealRoutine(ctx, objAPI) {
    ctx, cancel := globalLeaderLock.GetLock(ctx)   // ★ 单 leader
    healTimer := time.NewTimer(siteHealTimeInterval)  // 30s
    for {
        select {
        case <-healTimer.C:
            if enabled {
                c.healIAMSystem(ctx, objAPI)   // ★ 阶段 1：先 heal IAM
                c.healBuckets(ctx, objAPI)     // ★ 阶段 2：再 heal bucket
                waitForLowIO(...)              // 低 I/O 时才跑，不抢前台
            }
            healTimer.Reset(siteHealTimeInterval)
        }
    }
}
```
`★ 为什么"IAM 先于 bucket"───────────────────────`
- **顺序不可换**：bucket 数据的访问权限依赖 IAM（用户/策略）。如果先 heal bucket（对象到了）
  但 IAM 还没 heal（对应的用户/策略不存在），就会出现"对象在但没人有权访问"的窗口。**先把
  身份补齐，再补数据**，保证任何时刻权限都先于数据就位。
- **单 leader（globalLeaderLock）**：全集群只有一个节点跑 heal，避免重复对 peer 发起同步。
- **30s 周期 + waitForLowIO**：每 30s 检查一次各站点是否一致、补齐差异；且只在 I/O 空闲时跑，
  不和用户请求抢资源。
- `healBuckets`（`:4444`）逐 bucket 比对：versioning → object-lock → SSE → replication → policy →
  tags → quota → LC，哪项在某 peer 缺了/旧了就补/更新。`healBucketReplicationConfig`（`:5170`）
  专门修复 bucket 间的复制配置。
`──────────────────────────────────────────`

---

## 6. 一页纸总结 Site Replication 的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 复制一切：IAM+配置+对象+ILM | 整个集群镜像，多数据中心多活 |
| 2 | Peers 按 deploymentID 对等索引 | 无主从，任一站点变更广播全员 |
| 3 | ServiceAccountAccessKey 跨站点互信 | 不用 root 凭证传输 |
| 4 | 本地先应用、再 concDo 广播 | 本地即时、远端最终一致 |
| 5 | concDo 并行广播 + offline 标记 | 不串行、不被故障 peer 阻塞 |
| 6 | 每种配置变更都有 hook 广播 | "复制一切配置"的实现 |
| 7 | updatedAt 时间戳 LWW 冲突解决 | 多活并发改同一项收敛到最新 |
| 8 | 依赖站点间时钟同步 | LWW 靠时间戳，NTP 重要 |
| 9 | AddPeerClusters 校验 IDP 一致 | 身份提供方必须兼容才能互通 |
| 10 | 加入时全量同步、之后增量 hook | 初始对齐 + 增量传播 |
| 11 | heal routine 单 leader + 30s + 低 I/O | 兜底补齐漏传，不抢前台 |
| 12 | **IAM 先于 bucket** heal | 权限先于数据就位，防"对象在但无人有权" |

下一篇深读（系列再收官）：**Metrics V2 指标体系**——指标描述符与采集器架构、缓存与 TTL、
按需采集、Prometheus 导出、label 设计。
