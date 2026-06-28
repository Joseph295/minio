# 深读 26 · 配置子系统逐层精读

> MinIO 的运行配置（通知目标、身份提供方、加密、压缩、扫描器、复制限速…）统一存在一个结构化
> 配置里。本篇拆开 config.json 的存储、KMS 加密落盘、ENV 覆盖优先级、历史版本与回滚、格式迁移、
> 热加载（`cmd/config*.go` + `internal/config/`）。

---

## 1. 结构化 KV 配置

MinIO 的配置是**分层的键值结构**（`internal/config`）：
```
config
 └── subsystem（如 notify_webhook / identity_openid / scanner / compression）
      └── target（如 "1"、"primary"，支持同一子系统多实例）
           └── key=value（如 endpoint、auth_token、enable）
```
- `mc admin config set minio notify_webhook:1 endpoint=... auth_token=...` 设置的就是这棵树的一个节点。
- 每个子系统有**带校验的强类型配置**（`internal/config/<subsystem>/`），加载时把 KV 解析成结构体
  并校验（如 webhook endpoint 必须是合法 URL）。

---

## 2. 存储：config.json，又一个自举对象

```go
// config.go:132 saveServerConfig
data, _ := json.Marshal(cfg)
configFile := path.Join(minioConfigPrefix, minioConfigFile)   // .minio.sys/config/config.json
if GlobalKMS != nil {
    data, _ = config.EncryptBytes(GlobalKMS, data, kms.Context{...})   // ★ KMS 加密
}
return saveConfig(ctx, objAPI, configFile, data)
// config-common.go:83 saveConfig → PutObject 用 MaxParity
func saveConfig(...) { return saveConfigWithOpts(..., ObjectOptions{MaxParity: true}) }
```
`★ 配置存储的三个讲究 ─────────────────────────`
- **config.json 是一个 MinIO 对象**（存 `.minio.sys/config/`）——和 IAM（深读 09）、bucket 元数据
  （深读 24）一样**自举存在 MinIO 自己里**，继承纠删码可靠性。
- **`MaxParity`（最高冗余）**：配置用 `opts.MaxParity=true`（深读 01 §2：parity 取 N/2）。**配置丢了
  整个集群就配错了**，所以用最高冗余度写——比普通对象更耐故障。IAM/bucket 元数据等关键系统数据
  也都用 MaxParity。
- **KMS 加密落盘**：配了 KMS 就**加密整个 config.json**。因为配置里含**密钥/凭证**（通知目标的
  auth_token、复制目标的 SecretKey、IdP 的 client secret…）。`decryptData`（`config.go:165`）读时解密。
  没配 KMS 则明文存（依赖盘加密/物理安全）。
`──────────────────────────────────────────`

---

## 3. ENV 覆盖：优先级 ENV > 文件 > 默认

```go
// config.go:211 initConfig
srvCfg, _ := readConfigWithoutMigrate(GlobalContext, objAPI)   // 读 config.json
lookupConfigs(srvCfg, objAPI)                                  // ★ ENV 覆盖
globalServerConfig = srvCfg
```
`★ 三层优先级 ───────────────────────────────────`
- **`lookupConfigs` 让环境变量覆盖存储的配置**。优先级：**ENV 变量 > config.json > 编译默认值**
  （`srvCfg.Merge()` 填默认）。
- 为什么这样：容器/编排环境里用 ENV 注入配置最方便（不可变基础设施）；持久化配置走 config.json；
  没配的走默认。**同一个配置项 ENV 设了就以 ENV 为准**——这让 K8s 等环境能用 `MINIO_*` 环境变量
  覆盖任何持久配置，无需改 config.json。
- 这也解释了运维困惑"我 `mc admin config set` 改了没生效"——多半是有个 `MINIO_*` ENV 变量在覆盖它。
`──────────────────────────────────────────`

---

## 4. 历史版本与回滚

```go
// config.go:116 saveServerConfigHistory
uuidKV := mustGetUUID() + kvPrefix
historyFile := pathJoin(minioConfigHistoryPrefix, uuidKV)   // .minio.sys/config-history/<uuid>.kv
```
`★ 配置即版本化 ───────────────────────────────`
- **每次改配置都存一个历史版本**（UUID 命名）。`mc admin config history` 列出、`mc admin config
  restore <id>` 回滚到某个历史版本。
- 配错了能回退——配置变更是高风险操作（改错通知/复制/加密配置可能影响生产），版本化 + 回滚是
  运维安全网。历史版本同样 KMS 加密。
`──────────────────────────────────────────`

---

## 5. 格式迁移

```
config-versions.go  各历史版本的结构定义
config-migrate.go   migrateConfig：把旧版本配置逐步升级到当前版本
```
- MinIO 版本演进中配置格式会变。启动时 `migrateConfig` 检测旧格式 → **逐版本升级**到当前格式。
  这保证**老部署升级后旧配置能被读懂**（呼应深读 03 xl.meta 的版本兼容——向后兼容是滚动升级的
  硬约束，配置也不例外）。

---

## 6. 热加载与集群传播

- 改配置（`mc admin config set`）→ 校验 → `saveServerConfig` 落盘 → 通知所有 peer 重载（
  `globalNotificationSys`）→ 各节点重新 `lookupConfigs` 应用。**无需重启**即生效（大部分子系统支持
  热加载）。
- 这是 IAM/bucket 元数据**同款的"盘→缓存→peer 广播"模式**（深读 09/24）。MinIO 的所有"配置类
  状态"都用这一套传播机制保持集群一致。
- `globalServerConfig` 受 `globalServerConfigMu` 保护，重载时原子替换。

---

## 7. 一页纸总结配置子系统的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 分层 KV：subsystem→target→key | 统一结构，支持多实例子系统 |
| 2 | 每子系统强类型配置 + 校验 | 配置错误加载时即报 |
| 3 | config.json 是自举 MinIO 对象 | 继承纠删码可靠性 |
| 4 | MaxParity 最高冗余 | 配置丢=集群配错，比普通对象更耐故障 |
| 5 | KMS 加密落盘 | 配置含密钥/凭证，防泄露 |
| 6 | ENV > config.json > 默认 | 容器化注入；解释"改了没生效" |
| 7 | 每次改配置存历史版本 | history/restore 回滚，变更安全网 |
| 8 | migrateConfig 逐版本升级 | 老部署升级后旧配置可读 |
| 9 | 盘→缓存→peer 广播热加载 | IAM/元数据同款，集群一致无需重启 |
| 10 | globalServerConfig 原子替换 | 重载并发安全 |

下一篇深读：**版本控制与对象锁**——versioning 状态机、object lock（WORM）、retention 模式、
legal hold、合规与治理。
