# MinIO 深度上手文档（新人 → 专家）

> 这一套文档的目标：读完之后，你能在脑子里画出"一个 S3 请求从网卡到磁盘"的完整路径，
> 理解 MinIO 每个核心子系统的职责、数据结构与设计取舍，并知道哪些地方藏着精妙设计、
> 哪些地方是容易踩的坑。所有关键论断都带 `文件:行号`，方便你边读边对照源码。

## 阅读顺序

| 篇 | 文件 | 你将获得 |
|----|------|---------|
| 1 | [01-overview-architecture.md](01-overview-architecture.md) | 全局观：MinIO 是什么、`ObjectLayer` 抽象、Pool→Set→Disk 三层、设计哲学 |
| 2 | [02-request-lifecycle.md](02-request-lifecycle.md) | 一个请求的完整生命周期：进程启动 → 路由 → 中间件 → handler → ObjectLayer → 磁盘。逐步追 PUT / GET |
| 3 | [03-erasure-storage.md](03-erasure-storage.md) | 纠删码引擎、`xl.meta` 元数据格式、bitrot、inline 小对象、quorum |
| 4 | [04-distributed-grid-locks.md](04-distributed-grid-locks.md) | `grid` 节点间 RPC、`dsync` 分布式锁、namespace lock、集群拓扑 |
| 5 | [05-iam-security.md](05-iam-security.md) | Signature V4 校验、IAM 存储与缓存、策略评估、SSE 加密与 KMS |
| 6 | [06-data-services.md](06-data-services.md) | bucket/site 复制、生命周期 ILM、data-scanner、healing/MRF、后台 goroutine 全景 |
| 7 | [07-design-highlights-pitfalls.md](07-design-highlights-pitfalls.md) | 跨模块的精妙设计、并发陷阱、版本兼容、运维易错点 |

> 📊 **架构图集**：[architecture-diagrams.md](architecture-diagrams.md) —— 19 张 Mermaid 图把全系统关键流程可视化（GitHub/VS Code 可直接渲染）。
> 🔬 **源码精读**：[deep/00-index.md](deep/00-index.md) —— 21 篇逐行精读（核心 5 + 分布式 2 + 安全 3 + 数据服务 3 + 外围 8），达到"亲手通读过源码"的深度。

## 给"Go 与分布式存储都还较新"的你：5 个先验概念

在进入正文前，先建立 5 个贯穿全篇的概念，后面不再赘述：

1. **对象存储 ≠ 文件系统**。S3 的世界里只有"桶（bucket）"和"对象（object）"两级，
   没有真正的目录树——`a/b/c.txt` 里的 `/` 只是对象名的一部分。MinIO 内部确实用文件系统的
   目录来落盘，但对外语义是扁平的 key→value（value 可以是 TB 级）。

2. **纠删码（Erasure Coding, EC）是 RAID 的泛化**。把一个对象切成 `N` 份数据块 + `K` 份校验块，
   写到 `N+K` 块盘上；任意丢失 ≤ `K` 块仍能用 Reed-Solomon 数学重建原数据。这是 MinIO
   省空间又抗故障的根基（对比三副本：EC(8,4) 只多花 50% 空间却能扛 4 盘故障）。

3. **Quorum（法定多数）是分布式一致性的货币**。"写成功"不等于"写到所有盘"，而是"写到
   足够多的盘（write quorum）"；"能读"意味着"能凑齐足够的盘（read quorum）"。MinIO 不依赖
   外部协调器（没有 ZooKeeper/etcd 强依赖），一致性靠 quorum + 锁来保证。

4. **Go 的 `interface` 是解耦的核心手法**。看到 `type XxxAPI interface {...}` 就理解为
   "契约"：上层只依赖契约，底层可以换不同实现（本地盘 / 远端盘 / 测试 mock）。MinIO 最重要的
   两个接口是 `ObjectLayer`（S3 语义）和 `StorageAPI`（单块盘的读写）。

5. **Go 的 `goroutine` + `channel` 是并发骨架**。MinIO 大量使用"启动一批 goroutine 并行
   读写 N 块盘，用 channel/WaitGroup 汇总，按 quorum 判定成败"的模式。后台任务（扫描、复制、
   自愈、生命周期）也都是常驻 goroutine + 任务队列。

## 仓库地图（先记住这张表）

```
minio/
├── main.go                # 进程入口，仅一行：minio.Main(os.Args)
├── cmd/                   # 服务端本体（~18 万行）
│   ├── server-main.go     # 启动编排：配置→IAM→ObjectLayer→后台任务→HTTP
│   ├── routers.go         # 全局中间件链 + 路由组装
│   ├── api-router.go      # S3 API 路由注册 + globalObjectAPI
│   ├── object-handlers.go # GetObject/PutObject 等 HTTP handler
│   ├── object-api-interface.go    # ObjectLayer 接口定义（核心契约）
│   ├── erasure-server-pool.go     # 顶层 ObjectLayer 实现（多 pool）
│   ├── erasure-sets.go / erasure.go / erasure-object.go  # 纠删码三层
│   ├── xl-storage.go              # 单块本地盘的读写实现（StorageAPI）
│   ├── xl-storage-format-v2.go    # xl.meta 元数据格式
│   ├── auth-handler.go / signature-v4.go   # 认证签名
│   ├── iam*.go                    # 身份与访问管理
│   ├── bucket-replication.go / site-replication.go  # 复制
│   ├── bucket-lifecycle.go / data-scanner.go        # 生命周期 + 扫描
│   └── *-healing.go / mrf.go      # 自愈
└── internal/             # 可复用基础库（~30 个子包）
    ├── grid/             # 自研节点间 RPC（WebSocket 多路复用）
    ├── dsync/ lsync/     # 分布式锁 / 本地锁
    ├── crypto/ kms/      # 加密 / 密钥管理
    ├── hash/             # bitrot / 校验
    └── ...
```

> 约定：正文里 `cmd/xxx.go:123` 这种写法在你的编辑器里可直接点击跳转。
> 行号基于撰写时的 `master`（commit `fb3f67a59` 附近），如有小幅漂移以函数名为准。
