# 深读 24 · bucket 元数据系统逐层精读

> 一个 bucket 有十来种配置：policy、versioning、object-lock、加密、生命周期、复制、通知、标签、
> 配额、复制目标。本篇拆开它们如何**合并存一个文件**、内存缓存、写盘+缓存+peer 通知三步走、
> 敏感配置 KMS 加密、site-replication 的时间戳基础（`cmd/bucket-metadata.go` + `-sys.go`）。

---

## 1. 一个结构体装下 bucket 的全部配置

```go
// bucket-metadata.go:69
type BucketMetadata struct {
    Name    string
    Created time.Time
    // ★ 每种配置：原始字节（XML/JSON）
    PolicyConfigJSON        []byte
    NotificationConfigXML   []byte
    LifecycleConfigXML      []byte
    ObjectLockConfigXML     []byte
    VersioningConfigXML     []byte
    EncryptionConfigXML     []byte
    TaggingConfigXML        []byte
    QuotaConfigJSON         []byte
    ReplicationConfigXML    []byte
    BucketTargetsConfigJSON []byte
    // ★ 每种配置一个 UpdatedAt 时间戳
    PolicyConfigUpdatedAt      time.Time
    VersioningConfigUpdatedAt  time.Time
    ... // 每个配置一个
    // ★ 未导出：解析后的强类型配置（原子替换）
    policyConfig      *policy.BucketPolicy
    versioningConfig  *versioning.Versioning
    lifecycleConfig   *lifecycle.Lifecycle
    objectLockConfig  *objectlock.Config
    replicationConfig *replication.Config
    ...
}
```
`★ "原始字节 + UpdatedAt + 解析指针"三件套 ─────────`
- **每种配置存三份信息**：① 原始字节（持久化的 XML/JSON）；② `UpdatedAt` 时间戳；③ 解析后的强类型
  指针（运行时用）。
- **原始字节是权威持久化形式**：存盘的是 XML/JSON 原文（S3 配置就是 XML/JSON），保证与客户端提交
  的字节一致、可原样返回。
- **解析指针是运行时缓存**：`parseAllConfigs`（`:271`）在加载时把字节解析成 `*versioning.Versioning`
  等，handler 直接用解析好的对象，不必每次重新解析 XML。
- **`UpdatedAt` 是 site-replication 的命脉**（深读 15）：每个配置独立的时间戳，跨站点 LWW 冲突解决
  靠它判"谁更新"。
`──────────────────────────────────────────`

---

## 2. 单文件合并存储：`.metadata.bin`

```go
// bucket-metadata.go:52
bucketMetadataFile = ".metadata.bin"
// 存储路径：.minio.sys/buckets/<bucket>/.metadata.bin
```
`★ 一个文件装所有配置（而非每配置一文件）───────────`
- **所有配置合并存进一个 `.metadata.bin`**（msgp 序列化整个 `BucketMetadata`）。老版本是每种配置
  一个单独文件（policy.json、lifecycle.xml……），`convertLegacyConfigs`（`:408`）做迁移。
- **为什么合并**：
  - **更少对象**：一个 bucket 一个元数据对象，而非十几个。海量 bucket 时显著减少元数据对象数。
  - **近原子的多配置保存**：保存是写一个文件（复用对象写的原子改名，深读 04），不会出现"policy
    更新了但 lifecycle 没更新"的撕裂中间态。
  - **site-replication 同步简单**：同步一个文件即同步全部配置（深读 15 的 healBuckets 逐项比对就基于
    这一个文件）。
- 它本身也是个 MinIO 对象（存在系统桶），所以**天然继承纠删码可靠性**——bucket 配置和 IAM 一样
  自举存在 MinIO 自己里（深读 09）。
`──────────────────────────────────────────`

---

## 3. 内存缓存与读取

```go
// bucket-metadata-sys.go:46
type BucketMetadataSys struct {
    sync.RWMutex
    metadataMap map[string]BucketMetadata   // ★ 所有 bucket 配置的内存缓存
    ...
}
// :497 Init：启动时并发加载所有 bucket 的元数据
func (sys *BucketMetadataSys) Init(ctx, buckets, objAPI) error {
    sys.concurrentLoad(ctx, buckets)   // 并发读盘填充缓存
}
// :259 各配置有专门的 getter（返回解析后的强类型 + UpdatedAt）
GetVersioningConfig / GetBucketPolicy / GetLifecycleConfig / GetObjectLockConfig / GetSSEConfig / GetReplicationConfig / GetQuotaConfig ...
```
- **启动时全量加载进内存**（`concurrentLoad` 并发读所有 bucket）。之后 handler 读配置走内存缓存——
  鉴权、生命周期评估、复制判定都高频读 bucket 配置，必须内存命中。
- **专用 getter**：`GetReplicationConfig` 返回 `*replication.Config` + UpdatedAt。注释强调返回的对象
  **不可修改**（共享指针，改了影响所有读者）——要改得原子替换整个指针。

---

## 4. 写路径：三步走（盘 → 缓存 → peer）

```go
// :101 updateAndParse
meta, _ := loadBucketMetadataParse(ctx, objAPI, bucket, parse)   // 读当前
updatedAt = UTCNow()
switch configFile {
case bucketVersioningConfig: meta.VersioningConfigXML = configData; meta.VersioningConfigUpdatedAt = updatedAt
case bucketReplicationConfig: meta.ReplicationConfigXML = configData; meta.ReplicationConfigUpdatedAt = updatedAt
case bucketTargetsFile:
    meta.BucketTargetsConfigJSON, _, _ = encryptBucketMetadata(ctx, meta.Name, configData, kms.Context{...})  // ★ 加密
    ...
}
return updatedAt, sys.save(ctx, meta)
// :166 save
meta.Save(ctx, objAPI)                                   // ① 写 .metadata.bin（原子）
sys.Set(meta.Name, meta)                                 // ② 更新内存缓存
globalNotificationSys.LoadBucketMetadata(bgCtx, meta.Name)  // ③ 通知所有 peer 重载
```
`★ 写路径是 IAM/site-repl 同款三步 ───────────────`
- **盘 → 缓存 → peer 广播**：和 IAM（深读 09）、site-replication（深读 15）完全一致的模式。改一个
  bucket 配置：先落盘、再更本地缓存、再通知集群内所有节点重载这个 bucket 的元数据（保证各节点缓存
  一致）。
- **更新只改一个配置字段 + 它的 UpdatedAt**：`updateAndParse` 加载完整元数据，只覆盖被改的那一项
  和它的时间戳，其余保持。所以"改 lifecycle"不会动 policy 的时间戳——site-replication 才能精确判断
  "只有 lifecycle 变了"。
- **`LoadBucketMetadata` 用 `bgContext`**（不用调用方 ctx）：注释明说——peer 通知是后台传播，不能因为
  原请求 ctx 取消就中断集群同步。
`──────────────────────────────────────────`

---

## 5. 敏感配置 KMS 加密

```go
// :150 bucketTargets（复制目标，含远端凭证）→ 加密存储
meta.BucketTargetsConfigJSON, meta.BucketTargetsConfigMetaJSON, err =
    encryptBucketMetadata(ctx, meta.Name, configData, kms.Context{bucket, bucketTargetsFile})
```
`★ 含凭证的配置加密落盘 ─────────────────────────`
- **复制目标配置（`bucketTargets`）含远端站点的 AccessKey/SecretKey**——明文存盘是泄密风险。所以它
  用 KMS（`encryptBucketMetadata`，信封加密同深读 10）加密后才存进 `.metadata.bin`。读时解密。
- 其余配置（policy/lifecycle 等）是公开语义，不加密。**只对含密钥/凭证的配置做加密**——精准而非
  一刀切，平衡安全与性能。
`──────────────────────────────────────────`

---

## 6. 派生判断

```go
// bucket-metadata.go:169
func (b BucketMetadata) Versioning() bool {
    return b.LockEnabled || b.versioningConfig.Enabled() || b.objectLockConfig.Enabled()
}
func (b BucketMetadata) ObjectLocking() bool {
    return b.LockEnabled || b.objectLockConfig.Enabled()
}
```
- **object lock 强制 versioning**：开了对象锁就必然开版本控制（WORM 需要版本不可变）。这个派生关系
  在这里编码——`Versioning()` 把"对象锁开启"也算作版本控制开启。深读 27 会展开对象锁。

---

## 7. 一页纸总结 bucket 元数据的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 一个 struct 装下全部 bucket 配置 | 统一管理十余种配置 |
| 2 | 每配置"原始字节 + UpdatedAt + 解析指针" | 持久化原文 + LWW 时间戳 + 运行时强类型 |
| 3 | 合并存一个 `.metadata.bin` | 少对象、近原子保存、site-repl 同步简单 |
| 4 | 元数据本身是 MinIO 对象（自举） | 继承纠删码可靠性 |
| 5 | 启动并发全量加载进内存缓存 | 高频配置读必须内存命中 |
| 6 | 专用 getter 返回不可改的解析对象 | 共享指针，改需原子替换 |
| 7 | 写路径：盘→缓存→peer 广播 | IAM/site-repl 同款一致性模式 |
| 8 | 更新只改一项 + 其 UpdatedAt | site-repl 精确判断哪项变了 |
| 9 | peer 通知用 bgContext | 集群同步不被原请求 ctx 中断 |
| 10 | bucketTargets 含凭证 → KMS 加密落盘 | 防复制目标密钥泄露 |
| 11 | object lock 强制 versioning | WORM 需版本不可变 |

下一篇深读：**STS 与身份提供方**——AssumeRole、WebIdentity(OpenID)、LDAP、证书 STS、
临时凭证的派生与会话策略。
