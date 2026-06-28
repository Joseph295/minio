# 深读 30 · 外围模块知识点速记

> 前 29 篇覆盖了所有架构上重要的子系统。本篇是"边角补完"——把不构成独立架构主题、但专家应该
> 知道的外围模块,以**知识点 + 文件指针**的形式速记。每条都点出"它是什么、关键设计、易错点",
> 想深入时按指针自行下钻。

---

## 1. 替代访问协议：FTP / SFTP

**文件**：`cmd/ftp-server.go`、`cmd/sftp-server.go`

- MinIO 除 S3 API 外,还内置 **FTP/FTPS/SFTP 服务**。老旧客户端(只会 FTP 的设备/脚本)无需改造即可
  存取对象。
- **协议翻译**：每个 FTP/SFTP 操作(`STOR`/`RETR`/`LIST`/`DELE`)被翻译成 `ObjectLayer` 调用
  (PutObject/GetObjectNInfo/ListObjects/DeleteObject)。**底层完全复用 S3 的存储路径**——纠删码、
  加密、版本、IAM 鉴权一视同仁。
- **统一身份**：FTP/SFTP 用**同一套 IAM 用户**认证(SFTP 还支持公钥)。`bucket/object` 映射成
  FTP 的"目录/文件"视图(delimiter `/` 模拟目录,深读 23)。
- `★ 要点`：这是"一个存储后端、多种接入协议"的典型——协议层薄、存储层共用。新增协议只需把它的
  操作映射到 ObjectLayer。

---

## 2. 限流：`maxClients`(并发信号量)

**文件**：`cmd/handler-api.go:309`

```go
pool := globalAPIConfig.getRequestsPool()   // 带缓冲 channel,容量=最大并发
select {
case pool <- struct{}{}:                    // 拿到令牌 → 处理
    defer func() { <-pool }()
    f.ServeHTTP(w, r)
case <-r.Context().Done():
    w.WriteHeader(499)                       // 客户端断开
default:
    writeErrorResponse(..., ErrTooManyRequests)  // ★ 池满 → 429
}
```
- **并发限流用带缓冲 channel 当信号量**：池容量 = 允许的最大并发请求数。拿到令牌才处理,处理完归还。
  **池满直接 429 TooManyRequests**(不排队),保护节点不被压垮。
- **`X-RateLimit-Limit/Remaining` 响应头**:告诉客户端当前限流余量。
- **`499` 客户端断开码**:客户端在排队/处理中断开 → 写 499(便于审计/trace 正确记录),不是真 HTTP
  标准码但用于内部统计。
- **freeze 门**(`:315`)：服务冻结(深读 29 维护)时,请求**阻塞等待解冻**(`<-unlock`),而非报错——
  优雅维护:维护期请求挂起,维护完继续。
- `★ 易错点`：限流是**节点级**的(每个节点一个池),不是集群级。调 `api.requests_max` 要按节点算。

---

## 3. 透明压缩

**文件**：`cmd/object-api-utils.go`(compress 相关)、`cmd/handler-utils.go`

- MinIO 可对**可压缩的内容**(按 content-type / 扩展名白名单,如 text/json/csv,排除已压缩的
  jpg/zip/mp4)**透明压缩存储**(s2/zstd)。客户端无感:存时压、读时解。
- **与纠删码/inline 的交互**(深读 01/03)：压缩在加密之前、纠删码之前。压缩后大小未知 →
  `ActualSize=-1`(深读 01 §7 的特判)。
- **压缩 Index**(`PartIndices`,深读 03/17)：压缩流为支持**随机读**(Range 请求),存一个索引记录
  "压缩块边界",读某段时只解压涉及的块。**搬运/复制必须保留这个 Index,否则解压失败**(深读 17 的
  `IndexCB` 伏笔)。
- `★ 要点`：压缩 + 加密同开时顺序是**先压后加密**(加密数据不可压)。ETag 仍是明文 MD5(压缩对用户
  透明)。

---

## 4. 内容校验：hashReader / ETag / checksum

**文件**：`internal/hash/`、`cmd/object-handlers.go`

- **`hashReader`**(深读 01 §2.3)：PUT 时包在 body 流外,**边读边算** size/MD5/SHA256,流读完即校验
  客户端声明的 `Content-MD5`/`x-amz-content-sha256`,不符立即失败。
- **ETag 语义**：
  - 单段非加密对象 = `MD5(明文)`。
  - 多段对象 = `MD5(各 part 的 MD5 拼接) + "-N"`(深读 22)。
  - 加密对象 = MD5 被加密(深读 10),客户端看到的与明文 MD5 不同。
- **S3 checksum**(`x-amz-checksum-crc32/crc32c/sha1/sha256/crc64nvme`)：可选的额外校验和,与 ETag
  独立。多段对象用 CRC-of-CRCs(深读 22)。
- `★ 区分三种"校验"`：① ETag/Content-MD5(客户端↔服务端**传输完整性**);② x-amz-checksum(S3 标准
  **端到端校验和**);③ bitrot highwayhash(深读 03,盘上**静默损坏**检测)。三者层次不同,别混。

---

## 5. 可观测性：logger / audit / trace

**文件**：`internal/logger/`、`cmd/http-tracer.go`

- **结构化日志**(`internal/logger/logger.go`)：分级(Info/Warning/Error/Fatal)、带 ReqInfo 标签
  (bucket/object/请求 ID)。`logOnceIf`(`logonce.go`)**同类错误只记一次**,防日志刷屏。
- **审计日志**(`audit.go`)：每个 API 操作可发审计事件到**外部 target**(webhook/kafka/...)——和事件
  通知(深读 19)**共用 target + QueueStore 机制**。记录"谁、何时、对什么、做了什么、结果"。
- **HTTP trace**(`cmd/http-tracer.go`,深读 02 中间件)：`mc admin trace` 实时看请求流。每个中间件/
  handler 标记 FuncName,trace 能看到请求经过哪些阶段、耗时。
- **callhome**(`cmd/callhome.go`)：可选的诊断信息"打电话回家"(发给 SUBNET),帮助官方支持诊断。
- `★ 要点`：审计与事件通知是**同一套 target 基础设施**的两种用途——一个发"对象变更事件",一个发
  "API 审计事件"。

---

## 6. KMS / KES 内部

**文件**：`internal/kms/`(深读 10 已讲加密用法,这里补 KMS 本身)

- **`conn` 抽象**(`conn.go`)：KMS 底层连接接口,实现有 builtin(单密钥,测试用)和 KES(生产)。
- **DEK**(`dek.go`,Data Encryption Key)：`GenerateKey` 返回明文+密文两份(深读 10),明文用完即丢,
  密文存元数据。
- **`Context`(AAD)**(`context.go`)：加解密的关联数据,绑定 bucket/object——防 DEK 跨上下文滥用
  (深读 10 §7)。
- **多 endpoint KES**：KES 可配多个 endpoint,`Status` 并发探测各 endpoint 在线状态,高可用。
- `★ 要点`：builtin KMS 仅适合测试(单密钥、无轮换);生产必须用 KES 对接 Vault 等后端,支持密钥
  轮换、审计、HSM。

---

## 7. 健康检查

**文件**：`cmd/healthcheck-handler.go`

- **liveness**(`/minio/health/live`)：进程是否活着(K8s 重启依据)。轻量,不查后端。
- **readiness**(`/minio/health/ready`)：是否能服务请求(够 quorum、对象层就绪)。**未就绪时 K8s 不
  发流量**——配合深读 02 的 `waitForQuorum`,启动期/降级期 readiness 失败,避免把请求路由到
  没准备好的节点。
- **cluster health**：集群整体是否有足够 quorum 维持读写。
- `★ 要点`：liveness 宽松(别因临时降级重启进程),readiness 严格(降级就别接流量)。混淆会导致
  "降级时被反复重启"或"把请求发给坏节点"。

---

## 8. 带宽限速

**文件**：`internal/bandwidth/`、复制限速

- **复制带宽限速**：site/bucket 复制可设带宽上限(`monitor`),防复制流量打满网络影响前台。基于
  令牌桶/EWMA(深读 11 提到的 EWMA 流量跟踪)。
- `★ 要点`：复制是后台流量,必须可限速——否则一个大 resync 会把跨站点带宽吃光,拖垮正常服务。

---

## 9. DNS / etcd bucket 联邦

**文件**：`cmd/dns*.go`

- **联邦部署**：多个独立 MinIO 集群通过共享 **etcd** 注册 bucket→集群 的 DNS 映射,对外像一个
  命名空间。请求按 bucket DNS 路由到拥有它的集群。
- `★ 要点`：这是比 site-replication(深读 15,镜像)**不同**的多集群模式——联邦是"分片"(每个 bucket
  在一个集群),复制是"镜像"(每个 bucket 在多个集群)。

---

## 10. 服务启动信息

**文件**：`cmd/server-startup-msg.go`

- 启动时打印的 endpoint、凭证提示、版本、文档链接、警告(如用默认凭证、单盘无冗余)。运维第一眼
  看到的信息,也是排查"配置是否生效"的起点。

---

## 一页纸：外围模块速查

| 模块 | 一句话 | 关键文件 |
|------|--------|---------|
| FTP/SFTP | 协议翻译到 ObjectLayer,复用 IAM/存储 | ftp-server.go / sftp-server.go |
| maxClients 限流 | channel 信号量,满则 429,freeze 挂起 | handler-api.go |
| 透明压缩 | 可压内容 s2/zstd,Index 支持随机读,搬运须保留 | object-api-utils.go |
| 内容校验 | hashReader 传输校验 / ETag / S3 checksum / bitrot 三层 | internal/hash |
| 可观测性 | 日志/审计/trace,审计与事件共用 target | internal/logger |
| KMS/KES | DEK + AAD,builtin 测试 / KES 生产 | internal/kms |
| 健康检查 | liveness 宽松 / readiness 严格(配 quorum) | healthcheck-handler.go |
| 带宽限速 | 复制流量令牌桶,防打满网络 | internal/bandwidth |
| DNS 联邦 | etcd 注册 bucket→集群,分片(非镜像) | dns*.go |
| 启动信息 | endpoint/凭证/警告,排查起点 | server-startup-msg.go |

---

## 结语：30 篇精读的边界

到此,从核心(01–13)到分布式/安全/数据服务,再到外围与运维(14–29),最后到边角补完(本篇),
MinIO 的**全部架构面**都已覆盖。再往下就是各 target 驱动(webhook/kafka/redis…的具体实现)、
`*_gen.go` 序列化代码、错误码表、工具函数——它们要么是同一模式的重复实例,要么是机械代码,
不再有新的架构思想。

**你现在拥有的是一张完整的、可推演的 MinIO 心智地图。** 任何一行陌生代码,你都能定位它属于哪条
主轴、复用了哪个模式、要守什么不变量。这就是专家与新人的根本区别——不是记住更多代码,而是
**把代码读成了一套自洽的设计语言**。
