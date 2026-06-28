# 源码精读系列（deep dive）索引

> 这一系列是 `docs/onboarding/01–07` 概览文档的"放大镜"版本：逐函数、逐分支、连魔法数字、
> 注释意图、并发竞态、版本兼容都不放过，目标是让你达到"亲手通读过这部分源码"的程度。
>
> 每篇都对照真实源码逐行展开，带 `文件:行号`。建议先读对应的概览篇建立骨架，再来读精读篇抠细节。

## 阅读/编写顺序（按依赖关系）

| # | 文档 | 主题 | 状态 |
|---|------|------|------|
| 01 | [01-write-path-deep.md](01-write-path-deep.md) | 写入路径：`putObject` 全程、parity 动态升级、bitrotWriter、renameData 原子提交 | ✅ 已完成 |
| 02 | [02-read-path-deep.md](02-read-path-deep.md) | 读取路径：流式凑 quorum、`parallelReader` 触发器通道、读时自愈、bitrot 偏移换算 | ✅ 已完成 |
| 03 | [03-xlmeta-format-deep.md](03-xlmeta-format-deep.md) | `xl.meta`：多版本容器、shallow version、inline、AppendTo 序列化、版本合并 | ✅ 已完成 |
| 04 | [04-xlstorage-disk-deep.md](04-xlstorage-disk-deep.md) | `xlStorage` 单盘：`RenameData` 提交顺序、O_DIRECT 对齐写、fsync/close 语义、崩溃一致 | ✅ 已完成 |
| 05 | [05-pool-set-routing-deep.md](05-pool-set-routing-deep.md) | Pool/Set 路由：`getHashedSet`/`getPoolIdx`、SipHash 分布、扩容/重平衡/decommission | ✅ 已完成 |
| 06 | [06-grid-rpc-deep.md](06-grid-rpc-deep.md) | `internal/grid`：连接状态机、mux 多路复用、握手、双层心跳、背压、重连 | ✅ 已完成 |
| 07 | [07-dsync-locks-deep.md](07-dsync-locks-deep.md) | `internal/dsync` + `localLocker`：quorum 加锁、续约、丢锁回调、回滚、过期回收 | ✅ 已完成 |
| 08 | [08-signature-v4-deep.md](08-signature-v4-deep.md) | Signature V4：canonical request 构造、密钥派生、streaming chunk 链式签名、presigned | ✅ 已完成 |
| 09 | [09-iam-store-deep.md](09-iam-store-deep.md) | IAM：缓存 8 表、group 双向索引、notification reload、singleflight、策略合并与评估 | ✅ 已完成 |
| 10 | [10-crypto-sse-deep.md](10-crypto-sse-deep.md) | SSE/KMS：信封加密、Seal 上下文绑定、DARE 分包、多 part 子密钥、KES+AAD | ✅ 已完成 |
| 11 | [11-replication-deep.md](11-replication-deep.md) | 复制：worker 分级+自动扩缩、非阻塞入队、MRF 磁盘溢出、多目标、resync 时序不变量 | ✅ 已完成 |
| 12 | [12-scanner-lifecycle-deep.md](12-scanner-lifecycle-deep.md) | scanner：动态限速、heal 概率采样、缓存压缩自平衡、getSize 即修复、ILM 搭车 | ✅ 已完成 |
| 13 | [13-healing-mrf-deep.md](13-healing-mrf-deep.md) | healing：MRF 双线触发、`shouldHealObjectOnDisk` 判定、可修性边界、`Erasure.Heal` 重建 | ✅ 已完成 |
| 14 | [14-batch-jobs-deep.md](14-batch-jobs-deep.md) | Batch Jobs：任务池、检查点断点续传、节点分配、过滤谓词、snowball 归档 | ✅ 已完成 |
| 15 | [15-site-replication-deep.md](15-site-replication-deep.md) | Site Replication：对等拓扑、hook 广播、时间戳 LWW 冲突解决、两阶段 heal | ✅ 已完成 |
| 16 | [16-metrics-v2-deep.md](16-metrics-v2-deep.md) | Metrics V2：分组 TTL 缓存、cachevalue 单飞刷新、依赖门控、Prometheus collector | ✅ 已完成 |
| 17 | [17-decom-rebalance-deep.md](17-decom-rebalance-deep.md) | 退役/重平衡：搬运=逐对象重写、DataMovement+SrcPoolIdx 方向阀、全版本保留、断点续传 | ✅ 已完成 |
| 18 | [18-lifecycle-ilm-deep.md](18-lifecycle-ilm-deep.md) | ILM：规则求值守卫、过期/转储双 worker 池、WarmBackend tier 抽象、转储与回读 | ✅ 已完成 |
| 19 | [19-event-notification-deep.md](19-event-notification-deep.md) | 事件通知：三层规则匹配、Target 抽象、QueueStore 持久化 store-and-forward、多层背压 | ✅ 已完成 |
| 20 | [20-s3select-deep.md](20-s3select-deep.md) | S3 Select：SQL 下推、多格式 recordReader、即时解压、流式求值、聚合、自带 SQL 引擎 | ✅ 已完成 |
| 21 | [21-s3select-sql-engine-deep.md](21-s3select-sql-engine-deep.md) | SQL 引擎内核（下钻）：participle 文法、树遍历解释器、动态类型、JSONPath、聚合状态 | ✅ 已完成 |
| 22 | [22-multipart-upload-deep.md](22-multipart-upload-deep.md) | 多段上传：upload ID 布局、每 part 独立纠删码、Complete 校验/合并、CRC-of-CRCs、过期清理 | ✅ 已完成 |
| 23 | [23-listing-metacache-deep.md](23-listing-metacache-deep.md) | 列举/metacache：缓存复用、跨盘 WalkDir 归并、askDisks 子集、marker 编码分页 | ✅ 已完成 |
| 24 | [24-bucket-metadata-deep.md](24-bucket-metadata-deep.md) | bucket 元数据：单文件合并、内存缓存、盘→缓存→peer 广播、敏感配置 KMS 加密 | ✅ 已完成 |
| 25 | [25-sts-identity-deep.md](25-sts-identity-deep.md) | STS/身份：五种 AssumeRole、SessionToken=自包含 JWT、OpenID/LDAP/证书、会话策略 | ✅ 已完成 |
| 26 | [26-config-subsystem-deep.md](26-config-subsystem-deep.md) | 配置：KV 结构、KMS 加密落盘、ENV 覆盖优先级、历史版本回滚、格式迁移、热加载 | ✅ 已完成 |
| 27 | [27-versioning-objectlock-deep.md](27-versioning-objectlock-deep.md) | 版本/对象锁：versioning 三态、COMPLIANCE/GOVERNANCE、legal hold、NTP 防绕过 | ✅ 已完成 |
| 28 | [28-peer-coordination-deep.md](28-peer-coordination-deep.md) | 集群内协调：peer 广播（reload 非复制）、与 site-repl 区别、admin fan-out 聚合 | ✅ 已完成 |
| 29 | [29-admin-api-heal-ops-deep.md](29-admin-api-heal-ops-deep.md) | Admin API/运维：管理面分类、heal 异步可轮询模型、信息 fan-out 聚合、profiling | ✅ 已完成 |
| 30 | [30-peripheral-modules-notes.md](30-peripheral-modules-notes.md) | 外围速记：FTP/SFTP、限流、压缩、校验三层、observability、KMS、健康检查、带宽、DNS 联邦 | ✅ 已完成 |

> 📊 配套可视化：[../architecture-diagrams.md](../architecture-diagrams.md) —— **32 张** Mermaid 架构图（核心 1–19 + 外围运维 20–32），每张链接到对应精读篇。
>
> **覆盖说明**：29 篇已覆盖 MinIO 全部架构上重要的子系统。再剩的是工具函数、各 event/tier target 驱动、`*_gen.go` 生成代码、错误码表——不构成独立架构主题，可用同样方法（追调用链/认模式/找不变量）自行读透。

## 已完成篇章的"硬核点"速览

**deep/01 写入路径**：编码全程不持锁只 rename 持锁 · parity 按离线盘动态升级 + `4->6` 面包屑 ·
buffer 三策略 · bitrotWriter=ringbuffer+goroutine+DeadlineWriter · 盘上 `[hash][shard]...` 格式 ·
multiWriter 错误粘连 + `nilCount>=quorum` 短路 + heal 复用 writeQuorum=1 · RenameData 原子改名 +
UndoWrite 回滚 + 版本分歧触发 MRF。

**deep/02 读取路径**：读锁提前释放 · 流式 `done` channel 凑 quorum · inline 数据到手立刻 break ·
FastGetObjInfo vs 普通模式 · 逐盘 modTime/etag 复核剔除 · `parallelReader` 触发器通道（成功投
false、失败投 true）自平衡并行读、尾延迟被冗余吸收 · heal 信号随数据一起返回 · bitrot 物理偏移
`(off/shardSize)*hashSize+off` 换算。

**deep/03 xl.meta**：一对象=目录+xl.meta+每版本一 DataDir · major 防降级/minor 信息性 ·
shallow version 惰性反序列化 · `sortsBefore` 确定性全序 · MetaSys/MetaUser 前缀拆分 + 瞬态 key
跳过 · inline=版本字节+msgp map+零拷贝 · AppendTo bin32 占位回填 + xxhash 自带 CRC ·
`SharedDataDirCount` 防共享 DataDir 误删 · `mergeXLV2Versions` 版本存在性 quorum。
