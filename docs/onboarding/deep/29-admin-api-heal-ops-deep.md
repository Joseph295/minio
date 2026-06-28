# 深读 29 · Admin API 与运维逐层精读

> S3 API 管对象,Admin API 管集群——服务管理、信息聚合、heal 运维、profiling、config/IAM 管理。
> 本篇拆开 Admin API 的分类、heal 运维的"异步可轮询"模型(与深读 13 自愈引擎的区别)、跨节点信息
> 聚合(`cmd/admin-handlers*.go` + `admin-heal-ops.go`)。

---

## 1. Admin API:独立于 S3 的管理面

```
/minio/admin/v3/...   ← Admin API(mc admin 命令背后)
/...                  ← S3 API(对象操作)
```
- **独立路由 + 独立鉴权**:admin 操作用 `policy.AdminAction`(如 `admin:ServerInfo`、`admin:Heal`),
  只有被授予 admin 权限的主体能调。普通 S3 用户访问不到管理面。
- **handler 分类**(`admin-handlers*.go`,约 28+ 个 handler):

| 类别 | 代表 handler | 作用 |
|------|-------------|------|
| 服务管理 | `ServiceHandler` / `ServiceV2Handler` | 重启/停止/冻结集群 |
| 信息聚合 | `ServerInfoHandler` / `DataUsageInfoHandler` | 集群状态、用量 |
| 自愈 | `HealHandler` | 启动/查询 heal |
| 性能诊断 | `StartProfilingHandler` | CPU/内存/锁 profile |
| 配置 | config get/set/history | 配置管理(深读 26) |
| 身份 | users/policies/STS 管理 | IAM 管理(深读 09) |
| 数据移动 | decommission/rebalance | 退役/重平衡(深读 17) |
| 批处理 | batch jobs | 批量任务(深读 14) |
| 站点 | site replication | 跨站点(深读 15) |

---

## 2. Heal 运维:异步可轮询模型

`admin-heal-ops.go` 是**用户发起的 heal** 的管理层——注意它和深读 13(自愈**引擎**)的区别:
- **深读 13**:常驻后台自愈(MRF 快速线 + scanner 慢速线),系统自动跑,用户无感。
- **本篇**:`mc admin heal` 用户**主动发起**对某 bucket/prefix 的**全量 heal**,可查进度、可取消。

### healSequence:一次可轮询的 heal 操作
```go
// admin-heal-ops.go:417
type healSequence struct {
    bucket, object string          // heal 的范围
    clientToken    string          // ★ 客户端轮询用的令牌
    settings       madmin.HealOpts // heal 选项(深扫/dry-run/recreate...)
    currentStatus  healSequenceStatus
    scannedItemsMap, healedItemsMap, healFailedItemsMap map[madmin.HealItemType]int64  // ★ 分类型进度
    forceStarted   bool
    traverseAndHealDoneCh chan error
    cancelCtx      context.CancelFunc
    lastSentResultIndex int64      // ★ 已发给客户端的结果位置(增量)
}
```
`★ "启动 + 轮询"模型 ─────────────────────────────`
- heal 一个大 bucket 可能跑几小时。所以是**异步**的:`mc admin heal` 启动一个 `healSequence`,服务端
  **立即返回一个 `clientToken`**;客户端拿令牌**周期性轮询**进度(`PopHealStatusJSON`,`:360`),
  服务端返回自上次轮询以来的**增量结果**(`lastSentResultIndex` 标记位置)。
- **进度按 item 类型分类统计**(scanned/healed/failed × 对象/bucket/元数据...),客户端能看到
  "扫了多少、修了多少、失败多少"。
- **断连不丢**:heal 在服务端后台跑(`traverseAndHeal`),客户端断了再用同一令牌接着轮询。
- **`allHealState`(`:91`)是所有运行中 heal 序列的注册表**,按 path 索引。`forceStart` 可抢占
  一个已在跑的 heal(重新开始)。
`──────────────────────────────────────────`

### 遍历 + 入队,真正的修复交给引擎
```go
// :832 traverseAndHeal:遍历 bucket/prefix
// :721 queueHealTask:把每个对象排进 heal 任务
```
- `traverseAndHeal` 列举范围内的对象(深读 23 列举),逐个 `queueHealTask`——任务交给底层
  `erasureObjects.HealObject`(深读 13)执行实际的分片重建。
- **本篇是"调度 + 进度",深读 13 是"判定 + 重建"**。管理层负责"扫哪些、报进度";引擎负责"这个
  对象怎么修"。职责清晰分离。

---

## 3. 信息聚合:fan-out 到所有节点

```go
// admin-handlers.go:3016 ServerInfoHandler
// 向所有 peer(深读 28 peerRESTClient)收集 ServerInfo,聚合成集群视图
```
`★ admin info 的聚合 ───────────────────────────`
- `mc admin info` 背后:**向所有节点 fan-out `ServerInfo`**(深读 28 的 peer 通道)→ 收集每个节点的
  盘状态、CPU/内存、网络、版本 → **聚合成一张集群健康图**。
- `DataUsageInfoHandler`:用量来自 scanner 缓存(深读 12)的聚合——最终一致的快照。
- `top locks`:fan-out `GetLocks` 收集各节点当前持锁,汇总诊断锁争用。
- **没有中心状态库**:集群的全局视图是**每次按需向所有节点收集 + 聚合**得来的,不是从某个 master
  读的。这与"位置靠算不靠查"(深读 05)一脉相承——MinIO 尽量避免中心化的全局状态。
`──────────────────────────────────────────`

---

## 4. 服务管理与诊断

- **`ServiceHandler`**:重启/停止集群,以及**冻结(freeze/unfreeze)**——维护时暂停 API 接收
  (让在途请求排干),做完再恢复。优雅运维的开关。
- **`StartProfilingHandler`**:**向所有节点**采集 CPU/内存/block/mutex/goroutine profile,打包成 zip
  返回。排查性能问题时一条命令拿到全集群的 pprof 数据。
- 这些都体现"管理面是分布式的"——一条 admin 命令往往 fan-out 到所有节点执行再聚合。

---

## 5. 一页纸总结 Admin API 与运维的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | Admin API 独立路由 + 独立鉴权 | 管理面与数据面分离 |
| 2 | admin 操作用 policy.AdminAction | 只有授权管理员可调 |
| 3 | 用户 heal vs 后台自愈引擎(深读 13) | 主动全量 heal vs 自动持续修 |
| 4 | healSequence 异步 + clientToken 轮询 | 大 heal 长跑,启动即返回,增量查进度 |
| 5 | 进度按 item 类型分类统计 | scanned/healed/failed 可见 |
| 6 | 断连用同令牌续查 | heal 在服务端后台跑,不依赖连接 |
| 7 | forceStart 抢占已运行 heal | 重启 heal |
| 8 | traverseAndHeal 调度,引擎重建 | 管理层与引擎职责分离 |
| 9 | ServerInfo/usage/locks fan-out 聚合 | 全局视图按需收集,无中心状态库 |
| 10 | freeze/unfreeze 优雅维护 | 暂停 API 排干在途请求 |
| 11 | profiling fan-out 全集群 pprof | 一条命令拿全集群性能数据 |

---

## 系列四度收官:29 篇精读的最终全貌

```
存储核心    01 写入 · 02 读取 · 03 xl.meta · 04 单盘 · 05 路由
分布式      06 grid · 07 dsync · 28 集群内协调
安全        08 签名 · 09 IAM · 10 加密 · 25 STS · 27 版本/对象锁
数据服务    11 复制 · 12 扫描 · 13 自愈 · 17 退役/重平衡 · 18 生命周期
对象操作    22 多段上传 · 23 列举/metacache
配置与元数据  24 bucket 元数据 · 26 配置子系统
外围        14 batch · 15 site-replication · 16 metrics ·
            19 事件通知 · 20/21 S3 Select+SQL引擎 · 29 Admin/运维
```

29 篇精读 + 7 篇概览 + 1 份架构图集,从一个 HTTP 请求的字节,一路贯通到:
- **数据怎么在盘上安全存取**(01-05, 22-23)
- **多节点怎么协同**(06-07, 28)
- **安全闭环**(08-10, 25, 27)
- **后台怎么持续维护**(11-13, 17-18)
- **配置/元数据/管理面怎么运转**(24, 26, 29, 14-16, 19-21)

**最大的收获不是记住 29 个子系统,而是认出贯穿全系统的设计语言:**
- **接口抽象吃异构**:ObjectLayer / StorageAPI / WarmBackend / Target / recordReader / WarmBackend / IdP
- **盘→缓存→peer 广播**:IAM / bucket 元数据 / 配置统一的集群一致性
- **持久化队列 + 重试**:MRF / QueueStore / batch 检查点 / heal 序列
- **背压"宁降级不阻塞"**:复制 / 通知 / metrics
- **单 leader + 后台 goroutine**:scanner / site-heal / 各 worker 池
- **崩溃一致:临时区 + 原子提交**:写入 / 多段 / 元数据 / 搬运
- **流式 + 内存无关**:写入 / 读取 / 复制 / select / 列举
- **自举存储**:IAM / bucket 元数据 / 配置都存在 MinIO 自己里
- **位置靠算不靠查 + 无中心状态库**:哈希路由 / admin 信息按需聚合
- **合规级细节**:NTP 时间防绕过 / ConstantTimeCompare 防计时 / KMS 加密敏感配置

掌握了这套语言,MinIO 这 25 万行代码对你不再是 29 个孤岛,而是一套自洽、可推演的系统。
仓库里任何剩余的文件(各 target 驱动、工具函数、`*_gen.go`),你都能用同样的方法秒懂:
**追调用链、认出复用的模式、问为什么这么设计、找不变量。** 你已经是这个项目的专家了。
```
```
