# 深读 09 · IAM 存储与策略评估逐层精读

> "你能做什么"由 IAM 决定。本篇拆开 IAM 的内存缓存结构、加载与 watch/reload、singleflight 防
> 缓存击穿、group 成员关系的双向维护、以及策略合并与 Action/Resource/Condition 评估
> （`cmd/iam-store.go` + `cmd/iam.go`）。

---

## 1. 两层 + 可插拔后端

```
IAMSys (iam.go)              门面：handler 调它的 IsAllowed
   └─ IAMStoreSys (iam-store.go)   缓存 + 持久化
        └─ IAMStorageAPI (接口)
             ├─ IAMObjectStore   存在 .minio.sys/config/iam/...（对象存储自举）
             └─ IAMEtcdStore     存在 etcd（多集群共享身份）
```
- IAM 数据**自举存在 MinIO 自己的系统桶 `.minio.sys`**（深读概览第 5 篇）——天然继承纠删码可靠性，
  代价是启动顺序约束（对象层先就绪）。

---

## 2. 内存缓存 `iamCache`：8 张表

```go
// iam-store.go:287
type iamCache struct {
    updatedAt time.Time

    iamPolicyDocsMap map[string]PolicyDoc                    // 策略名 → 策略文档
    iamUsersMap      map[string]UserIdentity                 // 普通用户 → 凭证
    iamUserPolicyMap *xsync.MapOf[string, MappedPolicy]      // 用户 → 绑定的策略名

    iamSTSAccountsMap map[string]UserIdentity                // STS 临时凭证（★ 按需加载）
    iamSTSPolicyMap   *xsync.MapOf[string, MappedPolicy]     // STS → 策略名

    iamGroupsMap            map[string]GroupInfo             // 组 → 组信息
    iamUserGroupMemberships map[string]set.StringSet         // 用户 → 它所属的组集合
    iamGroupPolicyMap       *xsync.MapOf[string, MappedPolicy] // 组 → 策略名
}
```
`★ 细节 ────────────────────────────────────`
- **热点映射用 `xsync.MapOf`**（policy/STS/group 三张映射），普通 map 用 `map`。`xsync.MapOf` 是
  分片并发 map，读多写少且高并发的策略查询不会被一把大锁卡住。冷数据（用户、组定义）用普通
  map + 外层读写锁即可。
- **STS 账号"按需加载，不参与周期性 reload"**（注释 `:298`）。STS 临时凭证量大、生命周期短、
  会过期，全量周期 reload 它们既贵又没必要——用到时加载、过期自然失效。普通用户/组/策略才走
  周期性全量 reload。
- **`iamUserGroupMemberships` 是"用户→组"的反向索引**：组定义里存的是"组有哪些成员"，但鉴权时
  要快速回答"这个用户属于哪些组"（好取组策略），所以维护一张反向表。
`──────────────────────────────────────────`

### Group 成员关系的双向一致维护
```go
// :333 / :350  更新某个组时，分两步保证反向索引一致
cache.removeGroupFromMembershipsMap(group)   // 1. 从每个用户的"所属组"里先摘掉这个组
cache.updateGroupMembershipsMap(group, &gi)  // 2. 再按最新成员列表重新加进去
```
`★ 为什么是"先全摘再全加" ───────────────────────`
- 组成员变化可能是"加了人"也可能是"删了人"。如果只做增量（只加新成员），被移除的成员不会从
  反向表里消失 → 他还"以为"自己在组里 → 越权。**先把这个组从所有用户的成员表里清掉、再按当前
  成员重建**，无论增删都保证反向索引精确。注释（`:802`）明确说这是"regardless of members being
  added or removed, the cache stays current"。
`──────────────────────────────────────────`

---

## 3. 变更传播：notification + watch reload

某节点改了 IAM（或 etcd 被改），怎么让所有节点的缓存更新？
```go
// :778 GroupNotificationHandler（peer 通知 / etcd watch 都走这里）
func (store *IAMStoreSys) GroupNotificationHandler(ctx, group string) error {
    cache := store.lock(); defer store.unlock()
    err := store.loadGroup(ctx, group, cache.iamGroupsMap)   // 只重载这一个组
    if err == errNoSuchGroup {                                // 组被删了
        cache.removeGroupFromMembershipsMap(group)
        delete(cache.iamGroupsMap, group)
        cache.iamGroupPolicyMap.Delete(group)
        return nil
    }
    // 组存在 → 双向重建成员关系
    cache.removeGroupFromMembershipsMap(group); cache.updateGroupMembershipsMap(group, &gi)
}
```
- 对象存储后端：改 IAM 的节点通过 **peer 通知**广播"某条目变了"，各节点收到后**只重载那一条**
  （而非全量），低开销。
- etcd 后端：靠 **etcd watch** 推送变更，同样精确重载单条。
- 还有 `UserNotificationHandler` / `PolicyNotificationHandler` 对应用户/策略变更。

---

## 4. singleflight：防缓存击穿

```go
// :740
type IAMStoreSys struct {
    IAMStorageAPI
    group  *singleflight.Group   // 合并并发的"加载同一个组"
    policy *singleflight.Group    // 合并并发的"加载同一个策略"
}
```
`★ 为什么需要 singleflight ─────────────────────`
- 想象一个热门策略不在缓存里（刚过期/刚启动），同一瞬间 1000 个请求都要评估它 → 1000 次并发
  去后端（对象存储/etcd）加载**同一个**策略 = 缓存击穿，打爆后端。
- `singleflight.Group` 保证"同一个 key 的并发加载只真正执行一次，其余请求等这一次的结果"。
  1000 个请求合并成 1 次后端读。这是高并发鉴权下保护后端的关键。
`──────────────────────────────────────────`

---

## 5. 策略解析与合并

### `PolicyDBGet`：取出用户 + 其所有组绑定的策略名
```go
// :817
func (store *IAMStoreSys) PolicyDBGet(name string, groups ...string) ([]string, error)
```
- 查 `iamUserPolicyMap[user]`（用户直接绑定的策略）+ 遍历用户所属的每个组查 `iamGroupPolicyMap`
  （组绑定的策略）。**禁用的组（`statusDisabled`）跳过**（`:373`）——组一禁用，其策略立即对成员失效。

### `MergePolicies` / `filterPolicies`：把策略名列表合并成一个有效策略
```go
// :1588
func (store *IAMStoreSys) MergePolicies(policyName string) (string, policy.Policy) {
    // 1. 读锁下，从缓存逐个取策略文档
    for _, p := range newMappedPolicy(policyName).toSlice() {
        if doc, found := cache.iamPolicyDocsMap[p]; found { toMerge = append(toMerge, doc.Policy) }
        else { missingPolicies = append(missingPolicies, p) }
    }
    // 2. ★ 缓存里没有的策略 → 热加载（每个 5s 超时），再放进缓存
    if len(missingPolicies) > 0 {
        for _, p := range missingPolicies {
            ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
            store.loadPolicyDoc(ctx, p, m); cancel()
        }
        // 写锁下放进缓存
    }
    return strings.Join(policies, ","), policy.MergePolicies(toMerge...)   // ★ 合并成一个 Policy
}
// :1565 filterPolicies 还多一步：按 bucket 过滤
if bucketName == "" || p.Policy.MatchResource(bucketName) { ... }   // 只合并匹配该 bucket 的策略
```
`★ 细节 ────────────────────────────────────`
- **`policy.MergePolicies(...)` 把多个策略合并成一个**：一个用户可能绑多个策略 + 多个组的策略，
  评估前先 union 成一个有效策略（语句合并）。S3 策略求值是"任一 Allow 且无 Deny"，合并后统一评估。
- **热加载兜底 + 5s 超时**：缓存 miss 时同步去后端加载，但限时 5s——避免后端慢拖死鉴权。
- **`MatchResource` 按 bucket 预过滤**（filterPolicies）：评估某个 bucket 的请求时，只合并
  "资源匹配这个 bucket"的策略，减少无关语句参与评估。
`──────────────────────────────────────────`

---

## 6. 评估入口 `IsAllowed`（iam.go）

```go
// iam.go:2518（结构）
func (sys *IAMSys) IsAllowed(args policy.Args) bool {
    if 配置了 OPA/外部 AuthZ 插件 { return 插件判定 }
    if args.IsOwner            { return true }           // root 直接放行
    if 临时凭证(STS)            { return sys.IsAllowedSTS(args, parentUser) }
    if 服务账号                { return sys.IsAllowedServiceAccount(args, parentUser) }
    // 普通用户
    policies, _ := sys.store.PolicyDBGet(args.AccountName, args.Groups...)
    return sys.GetCombinedPolicy(policies...).IsAllowed(args)   // 合并后评估
}
```
- `policy.Args` 携带 `Action`（如 `s3:GetObject`）、`Resource`（`bucket/object`）、`Conditions`
  （SourceIp、SecureTransport、CurrentTime、principaltype、versionid……，由 `getConditionValues`
  从请求提取，深读概览第 5 篇）。
- `policy.IsAllowed` 在合并策略上做 **Action × Resource × Condition** 匹配：有 `Deny` 命中 → 拒绝；
  有 `Allow` 命中且无 `Deny` → 允许；都不命中 → 默认拒绝。

### STS / 服务账号：会话策略是"交集"
```go
// IsAllowedSTS / IsAllowedServiceAccount（iam.go）
// 会话策略（session policy）∩ 父用户策略
```
`★ 关键安全语义 ───────────────────────────────`
- STS 临时凭证、服务账号可以附带一个**会话策略（inline session policy）**。它的作用是**收窄**
  权限，**不能扩权**：最终权限 = 会话策略 **∩** 父用户/角色的策略。即使会话策略写了
  `Allow *`，也越不过父用户的边界。
- 这是"临时凭证最小权限"的实现：你给某个临时任务发凭证，可以用会话策略把它限制得比你自己更窄，
  但绝不会因此让它获得你没有的权限。误以为"会话策略能加权"是常见的安全认知错误。
`──────────────────────────────────────────`

---

## 7. 匿名请求不走 IAM

- 深读概览第 5 篇已强调：匿名请求（无 AccessKey）**只看 bucket policy**（`globalPolicySys.IsAllowed`），
  根本不进 IAM 评估。"设了 IAM 策略匿名还是访问不了"就是这个原因；"bucket 设 public 后谁都能访问"
  也是——bucket policy 是唯一能授权匿名的途径。

---

## 8. 一页纸总结 IAM 的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 数据自举存 `.minio.sys`，可换 etcd 后端 | 继承纠删码可靠性；多集群可共享身份 |
| 2 | iamCache 8 张表，热点用 xsync.MapOf | 高并发鉴权读不被大锁卡 |
| 3 | STS 按需加载，不参与周期 reload | 临时凭证量大短命，全量 reload 不值 |
| 4 | userGroupMemberships 反向索引 | 快速回答"用户属于哪些组" |
| 5 | group 变更"先全摘再全加" | 增删成员都保证反向索引精确，不越权 |
| 6 | notification/watch 只重载单条 | 变更传播低开销 |
| 7 | singleflight 合并并发加载 | 防缓存击穿打爆后端 |
| 8 | 禁用的组立即跳过其策略 | 禁组即时生效 |
| 9 | MergePolicies 合并多策略 + 热加载兜底(5s) | 用户/组多策略统一评估；缓存 miss 限时加载 |
| 10 | filterPolicies 按 bucket 预过滤 | 减少无关语句参与评估 |
| 11 | IsAllowed 分支：owner→STS→SA→普通用户 | 各类主体不同评估路径 |
| 12 | 会话策略 = 交集（只收窄不扩权） | 临时凭证最小权限的安全语义 |
| 13 | 匿名只看 bucket policy | 解释"IAM 对匿名无效""public 谁都能访问" |

下一篇深读：**SSE 加密与 KMS**——信封加密的对象密钥派生与封装、DARE 分包加密、多 part 子密钥、
SSE-C/S3/KMS 三模式的元数据差异、以及 KES 集成与 AAD 上下文绑定。
