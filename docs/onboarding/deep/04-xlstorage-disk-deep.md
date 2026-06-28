# 深读 04 · `xlStorage` 单盘实现逐行精读

> `xlStorage`（`cmd/xl-storage.go`）是 `StorageAPI` 接口的**本地盘实现**——直接操作本机文件系统。
> 上层纠删码并行调用的"那块盘"，本地的就是它（远端的是 `storageRESTClient` 把调用代理过去）。
> 本篇聚焦最硬核的三件事：`RenameData` 的崩溃一致提交顺序、O_DIRECT 对齐写、fsync 与 `close()`
> 的持久化语义。这是"数据真正安全落盘"的最后一公里。

---

## 1. `RenameData`：单盘上的"事务提交"

写入路径（深读 01 §7）调 `renameData` 并行让每块盘执行 `RenameData`。单盘的 `RenameData`
（`cmd/xl-storage.go:2557`）做的事远不止"改个名"——它要把临时区的新版本**合并进已有的多版本
`xl.meta`**，并以严格的顺序提交，保证任何时刻崩溃都不破坏已有数据。

### 提交顺序（这是全篇最重要的一张图）
```
RenameData(src=临时区, dst=正式位置, fi=新版本):
  1. 读 dst 现有 xl.meta（若存在）→ 加载成 xlMeta（保留所有旧版本）       :2633
  2. 若替换的是 null 版且对象有 tier 数据 → AddFreeVersion（留 scanner 异步清远端）:2778
  3. 找到同 VersionID 的旧版本；若其 DataDir 不被共享 → 记 res.OldDataDir 待清 :2785
  4. xlMeta.AddVersion(fi)  → 新版本并入多版本账本                          :2803
  5. res.Sign = 所有版本 ID 拼接（仅当 ≤10 版本）                            :2811
  6. 把【合并后的新 xl.meta】写回【源临时区】srcPath/xl.meta                 :2836  ★
  7. 若非 inline：renameAll(srcDataPath → dstDataPath)  把数据目录移到正式位置 :2862  ★
  8. 若有 OldDataDir：把【当前 dst 的旧 xl.meta】备份进 OldDataDir/xl.meta.bkp :2882  ★
  9. ★★ 提交点：renameAll(srcFilePath → dstFilePath)  原子改名 xl.meta      :2896
 10. 删除源临时目录                                                          :2911
```
`★ 为什么这个顺序能崩溃一致 ─────────────────────`
- **`xl.meta` 的原子改名（步骤 9）是整个对象的"提交点 / 线性化点"**。在它之前：
  - 新数据目录（DataDir）已经在步骤 7 移到正式位置了，但**当前正式的 `xl.meta` 还指向旧版本**，
    所以读者看到的仍是旧对象——新 DataDir 只是"躺在那里没人引用"。
  - 直到步骤 9 把新 `xl.meta`（已包含指向新 DataDir 的新版本）原子换上去，对象才"瞬间"
    变成新版本。文件系统 `rename` 在同一挂载点是原子的，所以**不存在"meta 半新半旧"的可读中间态**。
- **顺序不能反**：必须"先把数据放好，再切元数据指针"。如果先切 meta 再放数据，崩溃在中间就会
  出现"meta 说有这个版本，但数据目录还没到位"的悬空引用。
- **步骤 6 把合并后的 meta 写回源临时区**（而不是直接写 dst），是为了让步骤 9 能用一次原子
  `rename` 完成提交——临时区的 meta 已经是最终内容，rename 即生效。
- **步骤 8 的备份（`xl.meta.bkp` 存进 OldDataDir）**：覆盖写时，旧数据目录还没删。把旧 `xl.meta`
  备份进去，万一后续 rename 失败需要回滚，能从备份恢复旧版本。这是"提交前留一手"的保险。
`──────────────────────────────────────────`

### 失败回滚（unroll）
每一步失败都会清理已做的部分：
```go
// 例如步骤 7 失败：
if err = renameAll(srcDataPath, dstDataPath, skipParent); err != nil {
    if legacyPreserved { s.deleteFile(dstVolumeDir, legacyDataPath, true, false) }
    s.deleteFile(dstVolumeDir, dstDataPath, false, false)   // 注意 false：部分 rename 不递归删
    return res, osErrToFileErr(err)
}
```
- 注释 `// if its a partial rename() do not attempt to delete recursively`：部分完成的 rename
  可能让 dstDataPath 处于半移动状态，**递归删除可能误删本不该删的东西**，所以只删这一层。
  这种"回滚要保守、宁可留垃圾给 heal 也不误删"的态度贯穿全篇。

### `res.Sign`：版本指纹回传给上层
```go
// :2811
if len(xlMeta.versions) <= 10 {        // 版本太多不走这套，交给常规 scanner heal
    dst := []byte{}
    for _, ver := range xlMeta.versions {
        dst = slices.Grow(dst, 16); copy(dst[len(dst):], ver.header.VersionID[:])
    }
    res.Sign = dst   // 所有版本 ID 拼接
}
```
- 这个 `Sign` 就是深读 01 里 `renameData`（复数，上层）用 `reduceCommonVersions` 比对的"版本签名"。
  各盘返回的 `Sign` 不一致 → 说明盘间版本历史有分歧 → 触发 MRF 修复。
- **`> 10 版本就不算 Sign**：版本爆炸的对象不走这条 inline heal 路径，留给 scanner 慢慢修，避免
  在热路径上处理超长版本列表。

### free-version：删 null 版时给远端 tier 留个待清标记
```go
// :2773
if fi.VersionID == "" && !fi.IsRestoreObjReq() && !fi.Healing() {
    xlMeta.AddFreeVersion(fi)   // 未开版本控制覆盖写，旧 null 版若有 tier 数据 → 加 free-version
}
```
- 呼应深读 03 §2：覆盖一个"已转储到远端冷存储"的 null 版本时，本地立刻替换，但远端那份数据
  要异步删——free-version 就是给 scanner 的待办。**Restore 和 Heal 请求不加**（它们不替换版本，
  没有要异步清理的旧 tier 数据）。

### legacy（V1）数据保留
`:2668-2745` 大段处理"目标位置存的是 V1 旧格式（`xl.json` 或裸 `part.1`）"的情况：
- 把旧的 V1 对象 `AddLegacy` 进新的 V2 `xlMeta`（保留为 `LegacyType` 版本），并把旧 part 文件
  搬进 `legacy/` 目录。这保证**从 V1 升级到 V2 的过程中老数据不丢**，覆盖前一直可读。

### `skipParent`：省 mkdir 系统调用
```go
// :2747
skipParent := dstVolumeDir
if len(dstBuf) > 0 { skipParent = pathutil.Dir(dstFilePath) }  // 覆盖写/版本对象：父目录已存在
```
- 深层嵌套对象名（`a/b/c/d/e/obj`）每层都要 `mkdirAll` 检查/创建。如果是覆盖写或版本对象，
  父目录早就存在了，把 `skipParent` 设成已知存在的目录，`mkdirAll` 就能跳过这些层，
  **省下 `strings.Split(path,"/")` 次系统调用**。海量小对象写入时这是实打实的 IOPS 节省。

---

## 2. O_DIRECT 对齐写：`writeAllDirect`

非 inline 的 part 数据，最终通过 `writeAllDirect`（`:2131`）落盘。

```go
odirectEnabled := globalAPIConfig.odirectEnabled() && s.oDirect && fileSize > 0
if odirectEnabled {
    w, err = OpenFileDirectIO(filePath, flags, 0o666)     // O_DIRECT 打开
} else {
    w, err = OpenFile(filePath, flags, 0o666)
}

// 按大小选对齐 buffer 池
switch {
case fileSize <= xioutil.SmallBlock: bufp = xioutil.ODirectPoolSmall.Get()
default:                             bufp = xioutil.ODirectPoolLarge.Get()
}

if odirectEnabled {
    written, err = xioutil.CopyAligned(diskHealthWriter(ctx, w), r, *bufp, fileSize, w)  // 对齐拷贝
} else {
    written, err = io.CopyBuffer(diskHealthWriter(ctx, w), r, *bufp)
}
```
`★ O_DIRECT 的全部讲究 ─────────────────────────`
- **为什么用 O_DIRECT**：对象存储是"写一次、之后偶尔顺序读"，page cache 命中率低。O_DIRECT
  绕过内核页缓存，避免 double-buffering 浪费内存、避免大对象写把别的进程的热数据挤出缓存。
- **对齐是硬要求**：O_DIRECT 要求 buffer 地址、偏移、长度都按扇区（通常 512B/4KB）对齐。所以：
  - 用专门的对齐 buffer 池 `ODirectPoolSmall/Large`（按对象大小选，小对象不借大 buffer）。
  - 用 `CopyAligned` 而非普通 `io.Copy`——它处理"最后不满一个对齐块的尾巴"（尾部退回普通写）。
- **`diskHealthWriter` 包一层**：把写操作纳入磁盘健康监控，慢盘/卡死会被探测到（配合
  DeadlineWriter，深读 01）。
`──────────────────────────────────────────`

### 写入字节数校验 + 失败置 0
```go
// :2180
if written < fileSize && fileSize >= 0 {
    if truncate { w.Truncate(0) }   // ★ 写少了 → 截断成 0 字节，标记为"不可读"
    w.Close(); return errLessData
} else if written > fileSize {
    if truncate { w.Truncate(0) }
    w.Close(); return errMoreData
}
```
- **写入字节与声明大小不符 → `Truncate(0)`**：把文件截成 0 字节。注释说"zero-in the file size
  to indicate that its unreadable"——半截文件比 0 字节文件更危险（可能被误当成完整数据读），
  所以宁可清零让它明确"坏"，靠 quorum/heal 兜底。

### 持久化：`Fdatasync` + 检查 `close()` 返回值
```go
// :2195
if err = Fdatasync(w); err != nil { w.Close(); return err }   // ★ 只刷数据，不刷 mtime/atime
...
return w.Close()   // ★ 必须检查 close 的返回值
```
`★ 持久化语义的两个深坑 ─────────────────────────`
- **`Fdatasync` 而非 `Fsync`**：`fdatasync` 只把文件**数据**和"为读取数据所必需的元数据（大小）"
  刷到盘，**不刷 mtime/atime**。注释 "Only interested in flushing the size_t not mtime/atime"——
  少刷一次元数据更新，是性能优化。对象数据的持久性只依赖数据+大小落盘，访问时间无所谓。
- **必须检查 `close()` 的返回值**（注释引用 `man 2 close`）：write(2) 的错误**可能要到 close()
  才暴露**（尤其 NFS、磁盘配额场景）。如果只 `defer w.Close()` 不检查返回值，**会静默丢数据**。
  所以这里显式 `return w.Close()`。这是一个被注释郑重警告、极易被新手忽略的正确性点——
  你在自己代码里 `defer f.Close()` 写文件时，其实埋着同样的隐患。
`──────────────────────────────────────────`

---

## 3. 写 `xl.meta`：sync 路径的两种策略

`xl.meta`（含可能的 inline 数据）通过 `writeAllInternal`（`:2247`）落盘，sync 模式下分两路：

```go
// :2251
if sync {
    if len(b) > xioutil.DirectioAlignSize {
        // 大 xl.meta（多半是 inline 了数据）→ O_DIRECT 写 + 末尾 fdatasync
        return s.writeAllDirect(ctx, filePath, r.Size(), r, flags, skipParent, true)
    }
    w, err = s.openFileSync(filePath, flags, skipParent)   // 小 meta → O_DSYNC 打开
} else {
    w, err = s.openFile(filePath, flags, skipParent)       // 非 sync → 普通写
}
_, err = w.Write(b)
if err != nil { w.Truncate(0); w.Close(); return err }     // 部分写 → 截 0
return w.Close()
```
`★ 细节 ────────────────────────────────────`
- **大 inline meta 用 "O_DIRECT + 末尾一次 fdatasync"，小 meta 用 "O_DSYNC 逐写同步"**。
  注释解释：inline 了数据的大 `xl.meta`，O_DIRECT 一次性写 + 结尾 fdatasync 比每次 write 都
  O_DSYNC 同步要快。这是"按大小选同步策略"的微调。
- `writeAllMeta`（`:2212`）走的是"**写临时文件 → `renameAll` 到正式位置**"——又是临时文件 +
  原子改名的套路，保证 `xl.meta` 的替换本身是原子的（不会读到写一半的 meta）。
`──────────────────────────────────────────`

---

## 4. 读盘侧：`ReadXL` / `ReadVersion` / `readRaw`

读路径（深读 02 §2）并行调各盘的 `ReadXL`（无版本号取最新）或 `ReadVersion`（指定版本）。

```go
// :1627
func (s *xlStorage) ReadXL(ctx, volume, path string, readData bool) (RawFileInfo, error) {
    buf, _, err := s.readRaw(ctx, volume, volumeDir, filePath, readData)
    return RawFileInfo{Buf: buf}, err   // 返回 xl.meta 的【原始字节】，不在盘层解析
}
```
`★ 细节 ────────────────────────────────────`
- **`ReadXL` 返回 `RawFileInfo`（原始 `xl.meta` 字节），盘层不解析版本**。解析（`fileInfoFromRaw`
  / `mergeXLV2Versions`）在上层 `getObjectFileInfo` 做。好处：盘层只管"把字节读上来"，跨盘的
  版本合并/quorum 逻辑集中在上层一处，盘层保持简单。
- **`ReadVersion` 的 inline 红利**：注释（`:1652`）"for all objects less than 32KiB this call
  returns data as well along with metadata"——小对象的数据 inline 在 `xl.meta` 里，`readRaw`
  带 `readData=true` 一次就把元数据+数据全读上来。这就是深读 02 说的"inline 对象一次 `xl.meta`
  读完成整个 GET"在盘层的落地。
- `readData` 是个开关：只要元数据（如 list、stat）时传 false，省去读 inline 数据的开销。
`──────────────────────────────────────────`

### `ReadFileStream`：非 inline 的流式读
- 非 inline 的 part 数据通过 `ReadFileStream`（`:2017`）返回一个 `io.ReadCloser`，
  bitrotReader（深读 02 §6）从它流式读 + 校验。配合 `Close` 时 `DrainBody` 复用连接。

---

## 5. 全盘 fsync：`globalSync`

```go
// RenameData 的 defer 尾部 :2576
if s.globalSync { globalSync() }
```
- 某些部署开启 `globalSync`，在关键操作后做一次全局 `sync()`（刷所有脏页）。这是"最强持久性、
  最慢"的档位，多用于对断电一致性要求极高的场景。默认不开（靠 fdatasync 已足够）。

---

## 6. 一页纸总结单盘层的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | RenameData 提交顺序：写临时 meta→移数据目录→备份旧 meta→**最后原子改名 meta** | meta 改名是提交点，无半新半旧可读中间态 |
| 2 | 顺序不可反（先放数据再切指针） | 防"meta 指向不存在的数据" |
| 3 | OldDataDir 里备份 `xl.meta.bkp` | 提交前留一手，可回滚 |
| 4 | 失败回滚保守（部分 rename 不递归删） | 宁留垃圾给 heal，不误删 |
| 5 | res.Sign = 版本 ID 拼接（≤10 版本） | 上层比对版本一致性、触发 MRF |
| 6 | free-version（删 null 版且有 tier 数据） | 远端冷存储数据异步清理 |
| 7 | legacy V1 数据保留 + 搬进 `legacy/` | V1→V2 升级老数据不丢 |
| 8 | skipParent 省 mkdir 系统调用 | 深层嵌套对象写入降 IOPS |
| 9 | O_DIRECT 绕 page cache + 对齐 buffer 池 + CopyAligned | 对象存储访问模式下的 I/O 优化 |
| 10 | 写入字节不符 → `Truncate(0)` 置坏 | 半截文件比 0 字节更危险 |
| 11 | `Fdatasync`（不刷 mtime/atime） | 只保证数据+大小持久，省一次元数据刷 |
| 12 | **必须检查 `close()` 返回值** | write 错误可能只在 close 暴露，否则静默丢数据 |
| 13 | xl.meta 大用 O_DIRECT+fdatasync、小用 O_DSYNC | 按大小选同步策略 |
| 14 | ReadXL 返回原始字节、盘层不解析版本 | 版本合并逻辑集中上层，盘层简单 |
| 15 | ReadVersion <32KiB 连数据一起返回 | inline 对象一次读完成 GET |

下一篇深读：**Pool/Set 路由与扩缩容**——`getHashedSet`/`getPoolIdx` 的确定性映射、
分布算法、以及扩容（新增 Pool）、重平衡（rebalance）、退役（decommission）如何在不改老 Set
哈希的前提下迁移数据。
