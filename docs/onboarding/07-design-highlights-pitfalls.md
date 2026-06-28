# 07 · 设计精妙点与易错点（跨模块收口）

前六篇是"顺着系统讲"。这一篇是"横切着总结"——把散落在各模块的精妙设计、并发陷阱、
版本兼容和运维坑集中起来，方便你形成"专家直觉"。读完这篇，你看到一段陌生代码时，
应该能猜到它"为什么这么写"。

## 7.1 七个值得反复品味的设计决策

### ① 位置靠"算"不靠"查"——无中心元数据
对象 → Pool → Set 的映射全是哈希算出来的（`getPoolIdx` + `getHashedSet`，第 1 篇）。
- **好处**：没有 master、没有元数据服务、没有热点、没有单点。任何节点都能独立算出"这个对象
  该在哪"。
- **代价**：Set 数量进哈希函数，**一旦确定就不能改**（改了哈希全变、等于数据全迁）。所以
  MinIO 扩容是"加新 Pool"而不是"往老 Set 加盘"。理解这一点，你就懂了 MinIO 的整个扩容/部署
  模型为什么长这样。

### ② 分布式不分叉——两层接口吃掉复杂度
`ObjectLayer`（S3 语义）和 `StorageAPI`（一块盘）两层接口，让"本地/远端""单机/分布式"对业务
代码完全透明（第 1、4 篇）。
- 你几乎找不到 `if 分布式 { ... } else { ... }` 这种业务分叉。分布式性被压在
  `storageRESTClient`（远端盘实现）和 `distLockInstance`（分布式锁实现）里。
- **学习启示**：想改读写逻辑，去 `erasureObjects`；想改"远端怎么传"，去 `storage-rest-*` 和
  `internal/grid`。别在业务层找网络代码。

### ③ 一致性靠 quorum 重叠，不靠外部协调器
write quorum 比 read quorum 高（data==parity 时 +1），二者在盘集合上**保证重叠**——
读一定能看到最近一次成功的写。锁用 quorum + 续约 + 超时（第 4 篇）。
- **好处**：零外部依赖，单二进制即可分布式。
- **要记住**：这是"实用的多数派一致性"，不是 Paxos/Raft 的线性一致。它用 quorum 重叠 + 锁把
  不一致窗口压到极小，但理解其边界（极端网络分区）对运维很重要。

### ④ 流式 + 装饰器 + io.Pipe——内存可控的数据通路
`hashReader → 加密 → 压缩 → PutObjReader` 的 reader 套娃（第 2 篇），加上读路径的 `io.Pipe`
把"重建"和"发送"解耦。
- TB 级对象不落临时大文件，边读边校验边变换边写；客户端慢就自然背压。
- **学习启示**：MinIO 里凡是"边 X 边 Y"的需求，几乎都用 `io.Reader/io.Writer` 装饰器实现。
  这是 Go I/O 编程的范式，值得专门吃透。

### ⑤ 冗余既抗故障也抗尾延迟
读时 `parallelReader` "凑齐 M 块就停"（第 3 篇）。纠删码的冗余不只在坏盘时救命，平时也用来
绕过慢盘——尾延迟被冗余天然吸收。这是一个"把容错能力顺便变成性能能力"的漂亮复用。

### ⑥ 一次扫描，多种用途——后台工作的经济学
data scanner 一次遍历同时做：用量统计 + heal 采样 + 生命周期评估 + 复制检查（第 6 篇）。
逐对象遍历很贵，所以"凡是需要看一眼每个对象的事，都搭这趟车"。

### ⑦ 自愈与崩溃一致性是设计前提，不是补丁
- 写用"临时目录 + `renameData` 原子改名"做崩溃一致（第 3 篇）。
- bitrot 检测 + 纠删码绕过 + MRF 待修队列持久化（第 3、6 篇）。
MinIO 假设"盘会坏、写会半途死、节点会重启"是常态，把恢复能力做进主干而非事后打补丁。

## 7.2 并发与正确性陷阱（改代码时小心）

| 陷阱 | 说明 | 位置参考 |
|------|------|---------|
| **`globalObjectAPI` 可能为 nil** | 启动早期/重载时为空，handler 必须判空 | `api-router.go:55` |
| **多路径加锁顺序** | 批量操作对多对象加锁要全局一致顺序，否则交叉死锁；任一失败要回滚已得锁 | `namespace-lock.go:245` |
| **锁是异步释放** | `Unlock` 返回 ≠ 远端已释放，别据此假设别人能立刻拿锁 | `namespace-lock.go:184` |
| **加锁失败必回滚** | dsync 拿到部分锁但不够 quorum，必须全部释放，否则活锁 | `drwmutex.go:524` |
| **dynamicTimeout 别写死** | 锁/操作超时是自适应的，硬编码会在高负载误判 | `namespace-lock.go` |
| **两种 quorum 别混** | "读元数据 quorum(N/2)" ≠ "重建数据 quorum(M)"；parity 每对象可不同，要 `commonParity` 反推 | `erasure-metadata.go:461,531` |
| **签名用 ConstantTimeCompare** | 普通比较有计时侧信道，绝不能"优化"掉 | `signature-v4.go:402` |
| **ErasureDist 打散** | 盘上分片顺序是按对象打散的，直接 `ls` 看到"乱序"是正常的 | `xl-storage-format-v2.go` |

## 7.3 版本兼容与序列化陷阱

- **`*_gen.go` 是自动生成的，不要手改**。改序列化结构要改源 struct 再 `go generate`
  （`//go:generate msgp`）。仓库里 `storage-datatypes_gen.go`、`xl-storage-format-v2_gen.go`
  等都是生成产物（第 3 篇）。
- **`xl.meta` 的 major/minor 版本号有语义**：major = 破坏性（老节点读不了新格式），
  minor = 兼容追加。滚动升级时改错会导致新老节点互不兼容。
- **`LegacyType` 版本**：`xl.meta` 保留对 V1 旧格式对象的兼容读。删/改这部分会让老数据读不出来。
- **向后兼容是硬约束**：MinIO 集群常滚动升级，磁盘上的旧数据必须能被新代码读。任何元数据格式
  改动都要考虑"新代码读旧数据 + 旧代码遇到新数据"两个方向。

## 7.4 运维与行为"反直觉"点

| 现象 | 真相 |
|------|------|
| `mc admin info` 容量不实时 | 来自 scanner 上一轮采样，最终一致，可能滞后一个周期（第 6 篇） |
| 改了 IAM 策略匿名仍访问不了 | 匿名只走 bucket policy，根本不查 IAM（第 5 篇） |
| bucket 设 public 后谁都能读 | 同上，bucket policy 授权了匿名 |
| STS/服务账号权限"加不上去" | 会话策略只能收窄父用户权限（交集），不能扩权（第 5 篇） |
| SSE-C 丢密钥 = 数据永久无法读 | 服务端零知识，不存密钥；且 SSE-C 必须走 TLS（第 5 篇） |
| 不能往老 Set 加盘扩容 | Set 数进哈希，只能加新 Pool（§7.1①） |
| 单机重启后短暂不可服务 | `waitForQuorum` 要等够多盘上线（第 2 篇） |
| 盘上分片文件顺序"乱" | `ErasureDist` 按对象打散，设计如此（§7.2） |
| 某盘坏了读写无感 | 纠删码绕过 + 后台 heal 修复（第 3、6 篇） |

## 7.5 给你的"专家成长路径"建议

读完这套文档后，按这个顺序动手会进步最快：

1. **搭一个 4 盘单机集群**（`minio server /data/{1...4}`），用 `mc` 建桶、传对象，
   然后 `ls` 看 `/data/*/bucket/object/` 的目录结构——把 `xl.meta` 和 part 文件和第 3 篇对上。
2. **故意删掉一块盘的某个分片**，再 GET 对象（应该照常成功），观察后台 heal 把它补回来。
3. **打开 `mc admin trace -a`**，发一个 PUT/GET，对照第 2 篇的中间件链和 handler 路径。
4. **读一条完整调用链**：选 `PutObjectHandler` → `erasureServerPools.PutObject` →
   `erasureObjects.PutObject` → `erasure.Encode` → `xlStorage.CreateFile`，一行行跟下去。
5. **搭 2 节点分布式**，抓包/看日志，观察 grid 连接和远端 `storageRESTClient` 调用。
6. **挑一个子系统深读**：复制（`bucket-replication.go`）或 IAM（`iam-store.go`）任选其一，
   把它的 `_test.go` 也读了——测试往往是最好的"用法文档"。

## 7.6 全套文档收束

把七篇串成一句话：

> MinIO 是**用两层接口（ObjectLayer/StorageAPI）把 S3 语义和纠删码存储解耦**、
> **用哈希定位 + quorum + 自研 grid + 分布式锁实现无外部依赖的分布式**、
> **用信封加密 + 自举 IAM 守住安全**、
> **用搭车扫描 + 持久化待修队列把自愈做成一等公民** 的对象存储。

当你能对着任意一段陌生代码说出"它属于哪条主轴、为什么这么设计、有什么不变量要守"时，
你就从新人迈进了专家。

---

### 延伸阅读（仓库内）
- `docs/` 下各功能子目录（distributed、erasure、bucket/replication、bucket/lifecycle、
  sts、kms 等）有官方运维向说明，和本套"代码向"文档互补。
- 各 `*_test.go` 是最权威的"行为契约"。
- `Makefile` / `buildscripts/` 了解构建、生成、测试入口。
