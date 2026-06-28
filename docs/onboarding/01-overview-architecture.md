# 01 · 总览与分层架构

## 1.1 MinIO 一句话定位

MinIO 是一个**用 Go 写的、S3 API 兼容的、分布式纠删码对象存储**。它的设计目标可以浓缩成三句话：

- **简单部署**：单个静态二进制，无外部依赖（不需要 ZooKeeper/etcd 来跑核心存储）。
- **高密度可靠**：用纠删码而非多副本，在省空间的前提下抗多盘/多节点故障。
- **协议兼容**：对外说"纯正的 S3 方言"，让海量 AWS S3 生态工具开箱即用。

理解 MinIO 的关键，是抓住它的**两条主轴**：
1. **垂直主轴（一个请求怎么落盘）**：HTTP → handler → `ObjectLayer` → 纠删码 → `StorageAPI` → 物理盘。
2. **水平主轴（数据怎么分布）**：对象 → 哈希到某个 Pool → 哈希到某个 Erasure Set → 条带化到 Set 内所有盘。

本篇讲清楚这两条主轴的骨架与抽象，后续每篇再深入一段。

## 1.2 核心抽象：`ObjectLayer` 接口

整个 S3 语义被收敛到一个 Go 接口里：

- 定义：`cmd/object-api-interface.go:242-315`（`type ObjectLayer interface`）

它就是"对象存储该会做的所有事"的契约，方法大致分几组：

```go
type ObjectLayer interface {
    // —— Bucket 级 ——
    MakeBucket / ListBuckets / GetBucketInfo / DeleteBucket
    ListObjects / ListObjectsV2 / ListObjectVersions / Walk

    // —— Object 级（最核心）——
    GetObjectNInfo(ctx, bucket, object, rs *HTTPRangeSpec, h, opts) (*GetObjectReader, error)  // :274
    GetObjectInfo(...)            // 只取元数据
    PutObject(ctx, bucket, object, data *PutObjReader, opts) (ObjectInfo, error)               // :276
    CopyObject / DeleteObject / DeleteObjects
    TransitionObject / RestoreTransitionedObject   // 生命周期分层

    // —— Multipart 大文件分片上传 ——
    NewMultipartUpload / PutObjectPart / CompleteMultipartUpload / AbortMultipartUpload ...

    // —— 运维 ——
    StorageInfo / Health / HealObject / NSScanner ...
}
```

### 为什么这个接口是整个系统的支点？

`★ 设计洞察 ─────────────────────────────────`
- **HTTP handler 只依赖接口，不关心底下是单机还是分布式**。`object-handlers.go` 里的代码
  调用的是 `objectAPI.PutObject(...)`，它完全不知道数据最终被切成几块、写到几台机器。
  这让"单机模式 / 分布式模式 / 测试 mock"可以无缝替换。
- **接口让"装饰器/中间层"成为可能**。比如加密、压缩、配额、复制标记，都可以在调用
  `ObjectLayer` 前后用统一的 `ObjectOptions` 传递，而不污染接口本身。
`──────────────────────────────────────────`

### 它有几个实现？三层套娃

`ObjectLayer` 不是只有一个实现，而是**三层都实现了它**，层层包裹：

```
ObjectLayer (interface)
   │
   └─ erasureServerPools     cmd/erasure-server-pool.go:52   ← 顶层：管理多个 Pool
        │   serverPools []*erasureSets                          决定"对象落哪个 Pool"
        │
        └─ erasureSets        cmd/erasure-sets.go:51          ← 中层：一个 Pool 内多个 Set
             │   sets []*erasureObjects                          决定"对象落哪个 Set"
             │
             └─ erasureObjects cmd/erasure.go:47               ← 底层：一个 Set 的纠删码读写
                    getDisks() []StorageAPI                      真正切块、算校验、并行读写盘
```

- **`erasureServerPools`**（顶层）：你扩容时新加的"一组盘"叫一个 **Pool**。多 Pool 用于水平
  扩展。它负责把对象路由到某个 Pool（见 1.4）。
- **`erasureSets`**（中层）：一个 Pool 内部按固定大小切成若干 **Erasure Set**（典型 set 大小
  4/8/16 盘）。它负责把对象哈希到某个 Set。
- **`erasureObjects`**（底层）：一个 Set 才是真正做纠删码的最小单元——切块、算 parity、
  并行写它管辖的那几块盘。

这三层都满足同一个 `ObjectLayer` 接口，所以顶层方法常常是"算出该去哪个下层，然后转发调用"。
这是一种**组合式的责任链**：路由逻辑在上层，I/O 逻辑在底层。

## 1.3 另一个核心抽象：`StorageAPI`（一块盘）

如果说 `ObjectLayer` 是"对象存储的契约"，那 `StorageAPI` 就是"一块盘的契约"：

- 它把"读一个文件、写一个文件、写元数据、列目录、重命名"等磁盘操作抽象成接口。
- **本地盘**的实现是 `xlStorage`（`cmd/xl-storage.go`），直接操作本机文件系统（带 O_DIRECT 优化）。
- **远端盘**的实现是 `storageRESTClient`（`cmd/storage-rest-client.go`），把这些调用通过网络
  代理到拥有那块盘的节点上执行。

`★ 设计洞察 ─────────────────────────────────`
- **"本地盘"和"远端盘"对上层完全透明**。`erasureObjects` 拿到的 `getDisks()` 是一个
  `[]StorageAPI`，里面混着本地盘和远端盘，它一视同仁地并发调用。网络代理这件事被压在
  `StorageAPI` 这层抽象之下。这是 MinIO "分布式但代码不分叉"的关键——分布式不是在业务逻辑里
  写 if/else，而是在接口实现里换一个"会走网络"的实现。
- 远端调用**优先走 `internal/grid`（WebSocket 多路复用），REST 作后备**，详见第 4 篇。
`──────────────────────────────────────────`

## 1.4 水平主轴：对象如何被分布

一个对象 `PUT bucket/a/b/c.txt` 进来后，要经历两次哈希定位：

### 第一跳：选 Pool（`erasure-server-pool.go`）
- 入口：`PutObject`（`cmd/erasure-server-pool.go:1084` 附近）。
- 单 Pool 时直接用；多 Pool 时调用 `getPoolIdx()`：
  - **先查对象是否已存在**于某个 Pool（覆盖写要落回原 Pool）；
  - 不存在则选**剩余空间最充足**的 Pool（`getAvailablePoolIdx()`）。

### 第二跳：选 Set（`erasure-sets.go`）
- `getHashedSet(object)` → `getHashedSetIndex(object)`（`cmd/erasure-sets.go:695`）。
- 哈希算法由 `distributionAlgo` 决定（`cmd/erasure-sets.go:663-692`）：
  - **SipHash（V3，当前默认）**：以 `deploymentID` 为密钥对对象名做 SipHash，再 `% set数`。
    用部署 ID 当密钥能抗哈希碰撞攻击，分布也更均匀。
  - **CRC32（V2，旧版兼容）**：`crc32(key) % set数`。
- 结果是一个**确定性映射**：同一个对象名永远落到同一个 Set。这点至关重要——读的时候不需要
  查"目录"，直接重算哈希就知道去哪个 Set 找。

### 第三跳：条带化到 Set 内所有盘
- 选定 Set 后，`erasureObjects` 把对象数据切成 `dataBlocks` 份 + `parityBlocks` 份，
  **一一对应**写到 Set 内的每一块盘（第 3 篇详解）。

```
              PUT bucket/a/b/c.txt
                       │
              ┌────────▼─────────┐   getPoolIdx()      （已存在? 否则选最空的）
              │   选 Pool         │
              └────────┬─────────┘
                       │
              ┌────────▼─────────┐   getHashedSet()    （SipHash(name, deploymentID) % setCount）
              │   选 Erasure Set  │
              └────────┬─────────┘
                       │
              ┌────────▼─────────┐   Reed-Solomon 编码
              │ 条带化到 N+K 块盘 │   并行写，达 write quorum 即成功
              └──────────────────┘
```

`★ 设计洞察 ─────────────────────────────────`
- **无中心元数据服务**：对象→位置 的映射是"算出来的"（哈希），不是"查出来的"（没有 master
  维护一张大表）。好处是没有单点、没有元数据热点；代价是 Set 数量一旦固定就不能随意变（哈希
  会变），所以扩容是"加新 Pool"而非"往老 Set 塞盘"。这是理解 MinIO 扩容模型的关键。
`──────────────────────────────────────────`

## 1.5 全局对象层句柄：`globalObjectAPI`

handler 怎么拿到 `ObjectLayer`？通过一个被读写锁保护的全局变量：

- `cmd/api-router.go:53-65`
  ```go
  var globalObjectAPI ObjectLayer
  func newObjectLayerFn() ObjectLayer { /* RLock 后返回 globalObjectAPI */ }
  func setObjectLayer(o ObjectLayer)  { /* Lock 后赋值 */ }
  ```
- **设置时机**：`newErasureServerPools()` 里 `defer setObjectLayer(z)`（`cmd/erasure-server-pool.go:201`），
  也就是对象层一构造好就挂到全局。
- **handler 用法**：每个 handler 通过 `objectAPI := api.ObjectAPI()` 动态取当前实例
  （`cmd/api-router.go:255`）。

`★ 易错点 ─────────────────────────────────`
- 这个全局变量**可能为 nil**（启动早期、或正在重载）。所以几乎每个 handler 开头都有
  `objectAPI := api.ObjectAPI(); if objectAPI == nil { writeErrorResponse(...ErrServerNotInitialized) }`。
  你新增 handler 时若忘了这个判空，会在启动竞态或测试里偶发 panic。
`──────────────────────────────────────────`

## 1.6 设计哲学小结（这套系统"信什么"）

| 信条 | 体现 |
|------|------|
| **接口解耦，分布式不分叉** | `ObjectLayer` / `StorageAPI` 两层接口，让单机/分布式/远端共用一套业务代码 |
| **位置靠算不靠查** | 对象位置由哈希确定，无中心元数据服务，无单点 |
| **一致性靠 quorum + 锁** | 不强依赖外部协调器，写够 quorum 即成功，读够 quorum 即可读 |
| **空间靠纠删码不靠副本** | EC(N,K) 比三副本省空间且抗故障更强 |
| **自愈是一等公民** | 后台 scanner + heal + MRF 持续修复 bitrot 和缺失块 |
| **静态二进制，零依赖** | 单文件部署，运维简单 |

读到这里，你已经有了"骨架地图"。下一篇我们顺着**垂直主轴**，把一个真实请求从 `main()`
一路追到磁盘。
