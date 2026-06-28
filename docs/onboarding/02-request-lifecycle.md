# 02 · 一个请求的完整生命周期

本篇顺着**垂直主轴**，把 MinIO 从进程启动到一个 S3 请求落盘的全过程走一遍。建议你打开
编辑器，对照每个 `文件:行号` 跳进去看。

## 2.1 进程启动：从 `main()` 到 HTTP 监听

启动是一条"严格有序"的编排链。MinIO 用 `bootstrapTrace(...)` 把每一步包起来，这样启动慢在
哪一步可以被追踪到。

```
main()                                  main.go:29
  └─ minio.Main(os.Args)                cmd/main.go:201
       └─ (CLI 解析) serverCmd.Action → serverMain   cmd/main.go:141
            └─ serverMain(ctx, cli)     cmd/server-main.go:743
```

`serverMain`（`cmd/server-main.go:743-1174`）的关键步骤，**按真实顺序**：

| 步 | 做什么 | 位置 |
|----|--------|------|
| 1 | 初始化控制台日志 | `:755` newConsoleLogger |
| 2 | 解析命令行/磁盘布局（哪些盘、几个 Pool） | `:782` serverHandleCmdArgs |
| 3 | 处理环境变量、根凭证 | `:793` `:797` |
| 4 | 自检：bitrot / erasure / 压缩 算法自测 | `:800-804` |
| 5 | 初始化 KMS 配置 | `:807` |
| 6 | **初始化所有子系统**（IAM、bucket metadata、通知等的"壳"） | `:813` initAllSubsystems |
| 7 | **初始化 grid**（节点间 RPC）和 lock grid | `:860` `:865` |
| 8 | **启动 HTTP server**（路由先就位，但对象层还没好） | `:870-903` configureServerHandler |
| 9 | **构造 ObjectLayer**（`newErasureServerPools`） | `:920` newObjectLayer |
| 10 | **等待读 quorum 就绪**（够多盘上线才算可服务） | `:937` waitForQuorum |
| 11 | 初始化服务器配置、IAM、Console/FTP/SFTP | `:954-1016` |
| 12 | **拉起后台任务**：scanner、复制、生命周期、heal、通知…… | `:1018-1099` |

`★ 设计洞察 ─────────────────────────────────`
- **HTTP 路由先于 ObjectLayer 就绪**（步骤 8 在 9 之前）。这样进程能尽早接收连接，但在
  对象层 nil 期间，handler 会返回 `ServerNotInitialized`。这是"快速开始监听 + 优雅地告诉
  客户端还没好"的折中。
- **步骤 10 的 `waitForQuorum`** 是分布式启动的精髓：单台机器重启时不能立刻服务，要等集群里
  足够多的盘/节点上线，凑齐 read quorum，避免"半个集群"对外提供不一致的视图。
`──────────────────────────────────────────`

## 2.2 路由组装：三层路由 + 全局中间件

HTTP 处理器在 `configureServerHandler`（`cmd/routers.go:84-115`）里组装。它创建一个
gorilla `mux.Router`，然后**按顺序注册各路由组**：

```go
router := mux.NewRouter().SkipClean(true).UseEncodedPath()  // 不清洗路径——对象名可含特殊字符
registerDistErasureRouters(...)   // 分布式：节点间内部接口（storage/lock/peer REST）
registerAdminRouter(...)          // :95   /minio/admin/...
registerHealthCheckRouter(...)    // :98   健康检查
registerMetricsRouter(...)        // :101  Prometheus 指标
registerSTSRouter(...)            // :104  临时凭证 STS
registerKMSRouter(...)            // :107  KMS 管理
registerAPIRouter(router)         // :110  ← S3 对象 API（最常走的路）
router.Use(globalMiddlewares...)  // :112  全局中间件应用到所有路由
```

`★ 易错点 ─────────────────────────────────`
- **`SkipClean(true).UseEncodedPath()`** 是有意为之：S3 对象名可以包含 `.`、`..`、`//`、
  URL 编码字符。如果让路由器"规整"路径，`a//b` 会被改写成 `a/b`，对象名就错了。改路由时
  千万别"顺手"去掉这两个设置。
`──────────────────────────────────────────`

### S3 对象路由注册

`registerAPIRouter`（`cmd/api-router.go:253-392`）把每个 S3 动作映射到 handler。对象级路由
都挂在 `/{object:.+}` 上（`.+` 贪婪匹配，让对象名能带 `/`）：

| S3 动作 | 路由 | handler | 位置 |
|---------|------|---------|------|
| GET Object | `GET /{object:.+}` | `GetObjectHandler` | `object-handlers.go:715` |
| PUT Object | `PUT /{object:.+}` | `PutObjectHandler` | `object-handlers.go:1747` |
| HEAD Object | `HEAD /{object:.+}` | `HeadObjectHandler` | `api-router.go:300` |
| DELETE Object | `DELETE /{object:.+}` | `DeleteObjectHandler` | `api-router.go:395` |
| UploadPart | `PUT ...?partNumber&uploadId` | `PutObjectPartHandler` | `api-router.go:314` |
| CompleteMultipart | `POST ...?uploadId` | `CompleteMultipartUploadHandler` | `api-router.go:322` |

每个 handler 都被 `s3APIMiddleware()` 包一层（`cmd/api-router.go:210-250`），加上 trace、
gzip、限流（maxClients）、API 统计。

### 全局中间件链（执行顺序 = 注册顺序）

`globalMiddlewares`（`cmd/routers.go:54-81`）从外到内：

```
1. addCustomHeadersMiddleware     :56   注入 x-amz-request-id 等响应头
2. httpTracerMiddleware           :60   全链路 trace（mc admin trace 看到的就是它）
3. setAuthMiddleware              :66   解析鉴权类型（不是完整鉴权，是分流）
4. setBrowserRedirectMiddleware   :69   浏览器访问重定向到 Console
5. setCrossDomainPolicyMiddleware :71
6. setRequestLimitMiddleware      :73   请求体大小上限
7. setRequestValidityMiddleware   :75   请求合法性（非法 header / 路径穿越等）
8. setUploadForwardingMiddleware  :77   站点复制：把上传转发到正确站点
9. setBucketForwardingMiddleware  :79   多站点：转发到拥有该 bucket 的节点
```

`★ 设计洞察 ─────────────────────────────────`
- **中间件顺序即安全顺序**：先注入 request-id（出错也能追踪）→ 再 trace → 再鉴权分流。
  注意这里 `setAuthMiddleware` 只做"判断这是 V4 签名 / presigned / STS / 匿名"的**分流**，
  真正的"签名对不对、有没有权限"是在每个 handler 内部针对具体 bucket/object/action 再做的
  （见 2.4 与第 5 篇）——因为权限依赖于"对哪个资源做什么操作"，必须等路由解析出 bucket/object
  才能判定。
`──────────────────────────────────────────`

## 2.3 追一个 PUT：从 handler 到磁盘

以 `PutObjectHandler`（`cmd/object-handlers.go:1747`）为例，主干流程：

```
PutObjectHandler(w, r)                                  object-handlers.go:1747
  ├─ 取出 bucket / object（从 mux vars）
  ├─ objectAPI := api.ObjectAPI(); 判空 ServerNotInitialized
  ├─ 鉴权：checkRequestAuthType(...)（验签 + 鉴权，见第5篇）
  ├─ 解析 Content-MD5 / x-amz-content-sha256 / 元数据 / SSE 头
  ├─ 包装请求体：
  │    hashReader（校验大小+MD5+SHA256）→ PutObjReader（可能套加密/压缩）
  ├─ putObject := objectAPI.PutObject                    :1834
  └─ objInfo, err := putObject(ctx, bucket, object, pReader, opts)   :2063
```

注意这里的 reader 套娃，是 MinIO 处理"边读边校验边变换"的标准手法：

```
网络字节流
  → hashReader        校验客户端声明的 size / MD5 / SHA256（不符立即报错）
  → (EncryptReader)   若启用 SSE，边读边 DARE 加密
  → (compressReader)  若启用压缩，边读边压
  → PutObjReader      交给 ObjectLayer 的统一入口
```

`★ 设计洞察 ─────────────────────────────────`
- **流式处理，不落临时大文件**：一个 TB 级对象不会先存盘再处理，而是用 `io.Reader` 装饰器
  链边读边算边写。`hashReader` 在流读完的瞬间就能判定"客户端给的 MD5 对不对"，错了就让整个
  写入失败。这是内存占用可控的关键。
`──────────────────────────────────────────`

进入 `objectAPI.PutObject` 后，沿三层下钻：

```
erasureServerPools.PutObject     erasure-server-pool.go:1084   选 Pool（getPoolIdx）
  └─ erasureSets.PutObject        erasure-sets.go               选 Set（getHashedSet）
       └─ erasureObjects.PutObject  erasure-object.go:1244      真正的纠删码写
```

`erasureObjects.PutObject` 内部（细节见第 3 篇）：
1. 算 `parityDrives`（可被 storage class 或"可用性优化"调整）—— `:1287`
2. 算 `writeQuorum = dataDrives`（若 data==parity 则 +1）—— `:1323`
3. 建 Reed-Solomon 编码器，为**每块盘**建一个 `bitrotWriter` —— `:1360`
4. `erasure.Encode(...)` 流式编码并**并行写所有盘** —— `:1414`
5. 小对象走 inline（数据塞进 `xl.meta`），大对象单独落 part 文件 —— `:1388`
6. `renameData(...)` 把临时目录原子改名到最终位置 —— `:1543`

```
                       erasureObjects.PutObject
                                │
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                       ▼
   disk[0] (data)          disk[1] (data)   ...    disk[N+K-1] (parity)
   bitrotWriter            bitrotWriter            bitrotWriter
   写 part + xl.meta        写 part + xl.meta        写 part + xl.meta
        └───────────────────────┴───────────────────────┘
              达到 write quorum 个成功 → PutObject 返回成功
              不足 → reduceWriteQuorumErrs 返回 errErasureWriteQuorum
```

## 2.4 追一个 GET：定位、读元数据、并行重建

`GetObjectHandler`（`cmd/object-handlers.go:715`）→ 核心 `getObjectHandler`（`:312`）：

```
getObjectHandler                                  object-handlers.go:312
  ├─ 鉴权
  ├─ 解析 Range（断点续传 / 分段下载）
  ├─ getObjectNInfo := objectAPI.GetObjectNInfo     :359
  └─ reader, err := getObjectNInfo(ctx, bucket, object, rs, header, opts)  :404
        └─ 把 reader 的字节流拷到 http.ResponseWriter（边读边发）
```

下钻到底层 `erasureObjects.GetObjectNInfo`（`cmd/erasure-object.go:202`）：

1. `getObjectFileInfo(...)`（`:238`）：**并行**从各盘读 `xl.meta`，凑够 read quorum 个一致的
   元数据，确定对象的 size / 版本 / 分布 / 各盘 checksum。
2. 建一个 `io.Pipe`（`:291`）：一个 goroutine 在写端解码，handler 在读端消费——**边重建边发送**。
3. `getObjectWithFileInfo`（`:309`）：
   - 按编码顺序重排盘（`shuffleDisksAndPartsMetadataByIndex`，`:310`）——因为写时的盘顺序
     被打散过（见第 3 篇 `ErasureDist`），读时要还原。
   - 对每块盘建 `bitrotReader`（`:368`）：读的同时校验 highwayhash，发现 bitrot 立刻报错。
   - `erasure.Decode(...)`（`:389`）：只要凑齐 `dataBlocks` 块有效分片，用 Reed-Solomon
     **重建**出原始数据（哪怕部分盘坏了或读到的是 parity 块）。

`★ 设计洞察 ─────────────────────────────────`
- **读也是"凑 quorum"而非"读全部"**：`parallelReader` 只要先到的 `dataBlocks` 块就够重建，
  慢盘/坏盘不拖累整体（`prefer` 参数还会优先本地盘）。这是 MinIO 读延迟稳定的原因——尾延迟
  被纠删码的冗余天然吸收掉了。
- **`io.Pipe` 解耦"重建"与"发送"**：重建在一个 goroutine，HTTP 发送在另一个，背压自然形成。
  客户端慢就自然减速读盘，不会把重建结果堆在内存里。
`──────────────────────────────────────────`

## 2.5 一张端到端时序图（PUT）

```
Client                  HTTP/Router        Middlewares          PutObjectHandler        ObjectLayer(3层)         Disks(StorageAPI)
  │  PUT /bucket/obj  →     │                                                                                       
  │                        │  match route  →  reqid/trace/auth分流/limit/validity  →  │                            
  │                        │                                                          │  鉴权(验签+授权)            
  │  body stream      ───────────────────── hashReader→(enc/compress)→PutObjReader ──→│                            
  │                        │                                                          │ getPoolIdx → getHashedSet  
  │                        │                                                          │       │ erasure.Encode    
  │                        │                                                          │       └──并行写N+K块──────→ bitrotWriter ×(N+K)
  │                        │                                                          │  达 write quorum 成功      ← renameData(原子改名)
  │  200 OK + ETag    ←───────────────────────────────────────────────────────────  │                            
```

## 2.6 本篇要点回顾

- 启动是**严格有序**的编排，HTTP 先于 ObjectLayer 就绪，`waitForQuorum` 守住"可服务"门槛。
- 路由用 `SkipClean+EncodedPath` 保护对象名；全局中间件链先 reqid/trace 再鉴权分流。
- **真正的细粒度鉴权在 handler 内部**做（依赖具体 bucket/object/action）。
- PUT/GET 都走 `ObjectLayer` 三层下钻，最终在 `erasureObjects` 做**并行多盘 I/O + quorum 判定**。
- reader 装饰器链 + `io.Pipe` 实现**流式、可校验、有背压**的数据通路。

下一篇深入第 3 层：纠删码到底怎么切块、`xl.meta` 长什么样、bitrot 怎么防、quorum 怎么算。
