# 深读 27 · 版本控制与对象锁（WORM）逐层精读

> 版本控制让对象保留历史版本；对象锁（Object Lock / WORM）让对象在保留期内**不可删除、不可
> 覆盖**——满足金融/医疗等合规要求（SEC 17a-4 等）。本篇拆开 versioning 状态机、两种 retention
> 模式、legal hold、NTP 时间防绕过（`cmd/bucket-versioning.go` + `bucket-object-lock.go` +
> `internal/bucket/object/lock/`）。

---

## 1. 版本控制状态机

```go
// bucket-versioning.go
Enabled(bucket)    // 启用：每次写产生新 VersionID，删除产生删除标记
Suspended(bucket)  // 暂停：写覆盖 null 版本，但已有版本保留
// 未设置 = 禁用：无版本概念
// ★ MinIO 扩展：按前缀
PrefixEnabled(bucket, prefix)    // 某些前缀启用版本控制
PrefixSuspended(bucket, prefix)  // 排除某些前缀
```
`★ 三态 + MinIO 前缀扩展 ───────────────────────`
- **Enabled**：每次 PUT 产生新版本（新 VersionID），DELETE 产生删除标记（深读 03：`DeleteType`），
  旧版本全保留。这是"对象的完整历史"。
- **Suspended**：新写覆盖 `null` 版本，但**之前 Enabled 期间产生的历史版本仍保留**。暂停 ≠ 删除历史。
- **MinIO 前缀扩展**（注释 `:39`）：S3 标准是 bucket 级开关，MinIO 扩展为**可排除某些前缀**——比如
  对 `tmp/` 前缀不做版本控制（临时文件不必留历史），省空间。这是 MinIO 对 S3 的增强。
- 版本如何存：深读 03 的 `xl.meta` 多版本容器，每个版本一项，按 modtime 降序，最新在前。
`──────────────────────────────────────────`

---

## 2. 对象锁 = WORM，必须配版本控制

`★ 为什么对象锁需要版本控制 ─────────────────────`
- WORM（Write Once Read Many）= 对象一旦写入，保留期内不可改不可删。这要求**版本不可变**——
  所以**开对象锁强制开版本控制**（深读 24 §6 `Versioning()` 把 object lock 也算作 versioning）。
- 没有版本不可变，"覆盖"就会改变内容，WORM 无从谈起。
`──────────────────────────────────────────`

### 两种 retention 模式
```go
// bucket-object-lock.go:104 enforceRetentionBypassForDelete
switch ret.Mode {
case objectlock.RetCompliance:
    // ★ COMPLIANCE：保留期内任何人(含 root)都不能删/覆盖
    if !ret.RetainUntilDate.Before(now) { return ObjectLocked{} }
case objectlock.RetGovernance:
    // ★ GOVERNANCE：有 BypassGovernanceRetention 权限 + bypass 头才能越过
    byPassSet := IsObjectLockGovernanceBypassSet(r.Header)
    if !byPassSet {
        if !ret.RetainUntilDate.Before(now) { return ObjectLocked{} }
    } else {
        // 需 s3:BypassGovernanceRetention 权限
        if checkRequestAuthType(..., policy.BypassGovernanceRetentionAction, ...) != ErrNone { ... }
    }
}
```
| 模式 | 谁能在保留期内删/改 | 用途 |
|------|---------------------|------|
| **COMPLIANCE** | **没人**（包括 root），模式不可改、期限不可缩短 | 强合规（SEC 17a-4 等），最强 WORM |
| **GOVERNANCE** | 有 `s3:BypassGovernanceRetention` 权限 + `x-amz-bypass-governance-retention:true` 头的用户 | 一般保护，可受控豁免；也用于测试保留期设置 |

`★ COMPLIANCE 是"连 root 都拦"的硬墙 ───────────`
- COMPLIANCE 模式下，**保留期内连 root 用户都不能删/覆盖**，模式不能降级、期限只能延长不能缩短。
  这是真正的合规级 WORM——监管要求"数据 N 年不可篡改",COMPLIANCE 保证连管理员都做不到篡改。
- GOVERNANCE 是"软"一档：默认拦截大多数用户，但授权用户带 bypass 头可越过。适合"防误删但留逃生
  口"的场景,或在正式上 COMPLIANCE 前测试保留策略。
`──────────────────────────────────────────`

---

## 3. Legal Hold：与日期无关的锁

```go
// :59 enforceRetentionForDeletion
lhold := objectlock.GetObjectLegalHoldMeta(objInfo.UserDefined)
if lhold.Status == objectlock.LegalHoldOn { return true }   // ★ 法律保留 ON → 锁定（无视日期）
```
`★ Legal Hold vs Retention ─────────────────────`
- **Legal Hold 没有过期时间**：只有 ON/OFF。ON 时对象**无限期不可删**，直到有权限的用户显式
  关掉它（`s3:PutObjectLegalHold` 权限）。
- 用途：**诉讼保全**——案件期间证据不能销毁,但案子多久不知道,所以用无期限的 legal hold 而非
  固定 retention。案结再解除。
- **Legal Hold 与 Retention 独立并存**：一个对象可以同时有 retention（到某日期）和 legal hold（ON）。
  **任一生效就锁定**——retention 到期了但 legal hold 还 ON,依然删不了。两道独立的锁。
`──────────────────────────────────────────`

---

## 4. NTP 时间：防"改时钟绕过保留期"

```go
// :66 / :114
t, err := objectlock.UTCNowNTP()   // ★ 用 NTP 同步时间,不用本地时钟
if ret.RetainUntilDate.After(t) { return true /*locked*/ }
```
`★ 为什么用 NTP 而非本地时钟 ─────────────────────`
- 保留期判断 `RetainUntilDate > now` 用的是 **NTP 同步时间**(`UTCNowNTP`),**不是本地系统时钟**。
- 因为本地时钟可被管理员**改快**——如果用本地时钟,管理员把系统时间调到保留期之后,就能删除本该
  锁定的对象,WORM 形同虚设。用外部 NTP 源,**改本地时钟无效**,保留期边界不可被时间欺骗绕过。
- 这是合规级设计的细节:合规不能依赖"可被管理员操纵的本地状态"。NTP 拿不到时,保守地判为锁定
  (`return true`)——宁可误锁也不误放。
`──────────────────────────────────────────`

---

## 5. 设置 retention/legal hold

```go
// :245 checkPutObjectLockAllowed —— PUT 时可设置锁
// - 请求头带 x-amz-object-lock-mode / retain-until-date → 需 s3:PutObjectRetention 权限
// - x-amz-object-lock-legal-hold → 需 s3:PutObjectLegalHold 权限
// - bucket 配了 DefaultRetention → 新对象自动套用默认保留
```
- **PUT 时可直接设锁**(带相应权限),也可后续用 `PutObjectRetention`/`PutObjectLegalHold` 单独设。
- **bucket 默认保留**(`DefaultRetention`:mode + days/years):配了之后**所有新对象自动套用**,无需
  每次请求显式设——保证"这个 bucket 的所有数据默认就受保护"。

---

## 6. 与生命周期、复制的交互

- **ILM 让位于对象锁**(深读 18 §2):被锁定的对象,生命周期到期也不删(`evalActionFromLifecycle`
  的 `lr.LockEnabled` 守卫)。合规优先于自动清理。
- **复制保留锁状态**:retention/legal hold 元数据随对象复制到目标站点——目标端同样受保护(WORM
  不能因为复制就丢失)。

---

## 7. 一页纸总结版本控制与对象锁的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 版本控制三态:Enabled/Suspended/禁用 | 历史版本管理 |
| 2 | 删除产生删除标记而非真删 | 版本可恢复(deep/03) |
| 3 | MinIO 前缀级版本控制扩展 | 排除临时前缀省空间 |
| 4 | 对象锁强制版本控制 | WORM 需版本不可变 |
| 5 | COMPLIANCE:连 root 都不能删/改 | 强合规级 WORM |
| 6 | GOVERNANCE:授权用户带 bypass 头可越过 | 防误删 + 受控逃生口 |
| 7 | Legal Hold:无期限,显式关闭才解锁 | 诉讼保全 |
| 8 | Retention 与 Legal Hold 独立并存,任一生效即锁 | 两道独立的锁 |
| 9 | 用 NTP 时间判保留期 | 防改本地时钟绕过 WORM |
| 10 | bucket DefaultRetention 自动套用新对象 | 默认即保护 |
| 11 | ILM 让位、复制保留锁状态 | 合规优先,跨站点不丢保护 |

下一篇深读:**集群内节点协调**——peer REST、配置/IAM/元数据变更如何在集群内广播,与 site
replication 的区别。
