# 深读 23 · 对象列举与 metacache 逐层精读

> ListObjects 看似简单，实则是 MinIO 最微妙的子系统之一——因为"列举"要遍历所有盘，而分页又不能
> 每页重扫。本篇拆开 S3 列举语义、metacache 缓存复用、跨盘 WalkDir 流式归并、askDisks 子集优化、
> marker 编码分页（`cmd/metacache-*.go`）。

---

## 1. S3 列举语义：prefix / marker / delimiter

```go
// metacache-server-pool.go:60 listPath 入口处理语义
// delimiter="/" → 非递归，返回"公共前缀"(目录式)；否则递归扁平列举
if (o.Separator == slashSeparator || o.Separator == "") && !o.Recursive {
    o.Recursive = o.Separator != slashSeparator   // 有 / 分隔符 → 非递归
    o.Separator = slashSeparator
} else {
    o.Recursive = true                            // 无分隔符 → 递归列全部
}
```
- **`prefix`**：只列以它开头的对象。**`marker`**：从哪个 key 之后继续（分页）。**`delimiter`**：
  通常是 `/`，把 `a/b/c` 在第一个 `/` 处截断成公共前缀 `a/`（模拟"目录"）。**`maxKeys`**：每页上限。
- **S3 是扁平的**（深读 00 §先验概念）：delimiter 只是"按 `/` 把 key 分组"的视图，底层没有真目录。

---

## 2. 核心难题与 metacache 方案

`★ 为什么需要 metacache ─────────────────────────`
- **列举要遍历所有盘**：一个 bucket 的对象散落在所有 set 的所有盘上，列举要把它们 WalkDir 出来归并。
  这很贵（大量 IOPS）。
- **分页若每页重扫，灾难**：客户端列 100 万对象，每页 1000 个 = 1000 次请求。如果每次都从头扫到
  当前页，总成本是 O(页数²)。
- **metacache 的解法**：**第一次列举时做一次完整扫描，把结果缓存（流式存盘），后续每页从缓存
  流式读取**。marker 里编码了"缓存 ID + 读到哪了"。1000 页 = 1 次扫描 + 1000 次缓存流读。
`──────────────────────────────────────────`

`metacache` 结构（`metacache.go:58`）就是一次缓存列举的元信息：
```go
type metacache struct {
    id, bucket, root, filter string   // 缓存 ID、桶、前缀根、过滤器
    started, ended, lastHandout, lastUpdate time.Time
    status    scanStatus              // 进行中 / 成功 / 失败
    recursive bool
    fileNotFound bool
}
```
- **缓存的实际条目不在内存**，而是 `saveMetaCacheStream`（`:815`）**分块流式存到 `.minio.sys`**
  下的特殊对象。百万级列举不撑爆内存——边扫边存、边读边发。

---

## 3. 三种列举路径

`listPath`（`metacache-server-pool.go:60`）按 marker 里有无缓存 ID 分三种：
```
1) 冷列举（无 ID）         → listAndSave：扫描 + 存缓存 + 返回首页
2) 续页（有 ID，本节点造缓存）→ 同上但带 ID
3) 续页（有 ID，缓存已存在） → streamMetadataParts：从缓存流式读这一页
```
```go
// :119 有 ID → 先问"谁有这个缓存"
if o.ID != "" && !o.Transient {
    rpc := globalNotificationSys.restClientFromHash(pathJoin(o.Bucket, o.Prefix))  // ★ 按前缀哈希定位缓存所有者节点
    if rpc == nil { c = localMetacacheMgr...findCache(*o) }      // 本节点
    else          { c, err = rpc.GetMetacacheListing(...) }      // RPC 到所有者节点
    ...
    go c.keepAlive(ctx, rpc)   // ★ 持续续约，告诉所有者"客户端还在翻页"
}
```
`★ 分布式列举：缓存有归属节点 ───────────────────`
- **一个 (bucket, prefix) 的列举缓存归属一个节点**（`restClientFromHash` 按前缀哈希定位）。无论客户端
  连到哪个节点翻页，都 RPC 到这个"缓存所有者"节点取页。这避免每个节点各存一份缓存。
- **`keepAlive` 续约**：客户端还在翻页时持续告诉所有者"别丢这个缓存"。客户端走了（`lastHandout`
  超时），缓存被回收(`worthKeeping`，`:82`：未完成+停更超时丢弃、完成后保留 15 分钟、失败 5 分钟后删)。
- **`Transient` 列举**：保留桶/无效桶或一次性列举不建持久缓存，直接列。
`──────────────────────────────────────────`

---

## 4. 跨盘归并：`listPathRaw`

真正的扫描在 `listPathRaw`（`metacache-set.go:986`）：
```go
for i := range disks {
    r, w := io.Pipe()
    readers[i] = newMetacacheReader(r)
    go func() {
        werr := d.WalkDir(ctx, WalkDirOptions{Bucket, BaseDir, Recursive, FilterPrefix, ForwardTo}, w)  // ★ 每盘流式 WalkDir
        for { fd := fallback(werr); if fd == nil { break }; werr = fd.WalkDir(...) }  // ★ 失败用 fallback 盘
        w.CloseWithError(werr)
    }()
}
// 然后归并 N 个 reader 的有序流，逐条 resolve（agreed/partial 按 quorum）
```
`★ 列举的两个关键优化 ─────────────────────────`
- **每盘 WalkDir 流式 + io.Pipe**：每块盘独立地中序遍历目录、把条目**流式**写进 pipe，归并器从 N 个
  reader 做**多路归并**（各盘条目都是有序的，归并成全局有序）。流式 = 不把整个目录读进内存。
- **`askDisks` 子集 + fallback**：列举**不必问所有盘**——只要问"数据块数"个盘就能凑齐条目（每个对象
  在每块盘都有 `xl.meta`，问够 read quorum 个盘即可）。某盘失败时从 `fallbackDisks` 抓一块顶替。
  **这把列举的盘 I/O 砍到子集**，是大桶列举性能的关键。
- **`agreed` / `partial` 归并解析**（深读 17 见过）：多盘对同一条目一致 → 直接采纳；有分歧 → 按
  quorum `resolve` 出权威条目。列举也要 quorum——某盘多/少一个对象不能误导结果。
- **`ForwardTo`**：从 marker 位置"快进"到该 key，跳过前面已列过的——分页续扫不从头遍历目录。
`──────────────────────────────────────────`

---

## 5. marker：分页的状态载体

- S3 的 `marker`/`continuation-token` 在 MinIO 里被**编码成 `缓存ID + pool/set + 读取位置`**
  （`parseMarker`/`encodeMarker`）。客户端把上一页返回的 marker 带回来，服务端解码出"哪个缓存、
  从哪个块/偏移继续"。
- 所以 MinIO 的分页 marker**不是简单的"上一个对象名"**，而是携带了缓存路由信息的复合 token。这让
  续页能直接定位缓存所有者节点 + 缓存内偏移，O(1) 跳到该页。

---

## 6. ListObjects V1 / V2 / Versions 都走这套

- `ListObjects`（V1，marker）、`ListObjectsV2`（continuation-token + start-after）、
  `ListObjectVersions`（含版本与删除标记）—— 上层 API 不同，**底层都归约到 `listPath` + metacache**。
  版本列举多带版本信息，但缓存/归并/分页机制一致。

---

## 7. 一页纸总结列举与 metacache 的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | delimiter `/` → 公共前缀(目录式)，否则递归扁平 | S3 列举语义 |
| 2 | 列举要遍历所有盘，分页不能每页重扫 | metacache 存在的根本原因 |
| 3 | 首次扫描存缓存、后续页流式读缓存 | 把 O(页数²) 降为 O(扫描+页数) |
| 4 | 缓存条目分块流式存盘，不进内存 | 百万级列举不爆内存 |
| 5 | 缓存按 (bucket,prefix) 哈希归属一个节点 | 分布式翻页路由到所有者 |
| 6 | keepAlive 续约 + worthKeeping 回收 | 客户端在翻页就保活，走了就回收 |
| 7 | 每盘 WalkDir 流式 + io.Pipe 多路归并 | 各盘有序流归并成全局有序 |
| 8 | askDisks 子集 + fallback 盘 | 列举只问够 quorum 的盘，砍 I/O |
| 9 | agreed/partial quorum 归并 | 列举结果也要 quorum，防单盘误导 |
| 10 | ForwardTo 快进到 marker 位置 | 续页不从头遍历目录 |
| 11 | marker = 缓存ID+pool/set+偏移 复合 token | 续页 O(1) 定位缓存与偏移 |
| 12 | V1/V2/Versions 共用 listPath | 一套机制服务多种 List API |

下一篇深读：**bucket 元数据系统**——bucket 配置（versioning/policy/ILM/复制/加密…）如何存储、
内存缓存、跨节点同步。
