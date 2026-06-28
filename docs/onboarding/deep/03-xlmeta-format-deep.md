# 深读 03 · `xl.meta` 元数据格式逐层精读

> 每个对象在每块盘上都有一个 `xl.meta`。它不是简单的"元数据文件"，而是一个**多版本容器 +
> 自带 CRC 的二进制格式 + 惰性反序列化的索引结构 + 内联小对象数据**的复合体。本篇逐层拆开
> `cmd/xl-storage-format-v2.go`（及 `xl-storage-meta-inline.go`），把每个字段、每处惰性优化、
> 每个版本兼容设计讲到底。

---

## 1. 盘上的物理形态：目录树与文件头

源码注释（`xl-storage-format-v2.go:90`）直接画出了后端目录树：
```
disk1/
└── bucket
    └── object
        ├── a192c1d5-9bd5-41fd-9a90-ab10e165398d   ← 一个版本的 DataDir（UUID）
        │   └── part.1
        ├── c06e0436-f813-447e-ae5e-f2564df9dfd4   ← 另一个版本的 DataDir
        │   └── part.1
        ├── legacy                                  ← V1 旧格式对象的固定目录名
        │   └── part.1
        └── xl.meta                                 ← 所有版本的元数据都在这一个文件里
```
`★ 关键认知 ─────────────────────────────────`
- **一个对象 = 一个目录 + 一个 `xl.meta` + 每个版本一个 DataDir 子目录**。`xl.meta` 是这个对象
  所有版本的"账本"；每个版本的实际数据 part 放在以 `DataDir`（UUID）命名的子目录里。
- **多版本共存一个 `xl.meta`**：开启 versioning 后，对象的 v1/v2/v3... 都记在同一个 `xl.meta`
  的 `versions` 数组里，各自指向不同的 DataDir。删除标记也是其中一个"版本"。
- inline 小对象**没有** DataDir 子目录——数据直接塞在 `xl.meta` 尾部（见 §6）。
`──────────────────────────────────────────`

### 文件头：魔数 + 版本号
```go
// :44
xlHeader = [4]byte{'X', 'L', '2', ' '}        // 魔数 "XL2 "
// :59
xlVersionMajor = 1   // 破坏性变更：老软件读不了新格式（防降级到不兼容版本）
xlVersionMinor = 3   // 兼容性变更：仅信息性，用于事后判断确切版本
// :68 init()
binary.LittleEndian.PutUint16(xlVersionCurrent[0:2], xlVersionMajor)
binary.LittleEndian.PutUint16(xlVersionCurrent[2:4], xlVersionMinor)
```
`★ 版本号语义（容易改错的地方）───────────────────`
- **major 是"防降级闸门"**：注释明说"Newer versions cannot be read by older software. This will
  prevent downgrades to incompatible versions."——升 major = 老节点直接拒读，逼你不能滚回旧版本。
- **minor 只是信息性**：改了存储内容但保持兼容时 bump，方便事后定位"这条数据是哪个 minor 写的"。
- 改 `xl.meta` 结构时，**bump 错 major/minor 会造成滚动升级期间新老节点互不兼容**——这是最危险的
  改动类别之一。
`──────────────────────────────────────────`

---

## 2. 三种半（version type）

```go
// :108
ObjectType  VersionType = 1   // 正常对象版本（PutObject / multipart / copy）
DeleteType  VersionType = 2   // 删除标记（versioning 下的"软删除"）
LegacyType  VersionType = 3   // V1 旧格式对象，覆盖前一直保留
```
外加一个**特殊种类 free-version**（注释 `:85`）：
- 用一个 delete-marker + 特定 MetaSys 项表示，**只对 scanner 可见**。
- 用途：当一个有"分层（tiered）"数据的版本被删除/覆盖时，它在远端 tier 的数据要**异步**删除。
  free-version 就是给 scanner 留的"待回收远端数据"的待办标记。

`★ 为什么需要 free-version ─────────────────────`
- 删除一个已转储到远端冷存储（tier）的版本时，本地 `xl.meta` 可以立刻改，但**远端 tier 上的
  那份数据不能同步删**（慢、可能失败）。于是留一个 free-version 标记，scanner 后续扫到它时
  再去远端清理。这是"本地元数据操作要快、远端清理可以慢"的解耦。
`──────────────────────────────────────────`

---

## 3. Shallow Version：惰性反序列化的核心设计

`xl.meta` 里每个版本不是直接存成结构体，而是**"轻量头 + 原始字节"**：

```go
// :897
type xlMetaV2ShallowVersion struct {
    header xlMetaV2VersionHeader   // 轻量头：可直接比较/排序，无需反序列化
    meta   []byte                  // 完整版本的 msgp 原始字节，按需才 unmarshal
}
// :904
type xlMetaV2 struct {
    versions []xlMetaV2ShallowVersion   // 所有版本，按 modtime 降序（index 0 = 最新）
    data     xlMetaInlineData           // inline 数据，按 versionID 索引
    metaV    uint8
}
```

### 轻量头 `xlMetaV2VersionHeader`
```go
// :249
type xlMetaV2VersionHeader struct {
    VersionID [16]byte
    ModTime   int64
    Signature [4]byte       // 版本内容的指纹（快速判等）
    Type      VersionType
    Flags     xlFlags       // FreeVersion / UsesDataDir / InlineData 位标志
    EcN, EcM  uint8         // 校验块/数据块数（老格式为 0/0）
}
```
`★ 这个设计为什么聪明 ─────────────────────────`
- **大量操作只看 header，不碰 `meta`**：列版本、排序、选最新版、跨盘比对——全靠 header
  （定长、可 `==` 比较、可排序）。只有真的要读某个版本的细节时，才 `getIdx(i)` 把 `meta`
  反序列化成完整结构。这是**惰性反序列化**：避免每次操作都 unmarshal 所有版本。
- `Signature`（`getSignature`）是版本内容的 4 字节指纹。两个 header 的 `==` 比较就能快速判断
  "这两块盘上的这个版本是不是完全一样"，避免逐字段比。
- **`getIdx` 里有个被禁用的调试断言**（`:1268` `if false { ... panic }`）：开发期用来校验
  "header 的 VersionID 和反序列化出来的对象 VersionID 一致"，发布时用 `if false` 关掉。这是一处
  有趣的"留在代码里的调试脚手架"。
`──────────────────────────────────────────`

### 确定性排序：`sortsBefore` 平局裁决
```go
// :294
func (x xlMetaV2VersionHeader) sortsBefore(o xlMetaV2VersionHeader) bool {
    if x == o { return false }
    if x.ModTime != o.ModTime { return x.ModTime > o.ModTime }   // ① 最新优先
    if x.Type != o.Type       { return x.Type < o.Type }         // ② 类型小的优先
    if v := bytes.Compare(x.Signature[:], o.Signature[:]); v != 0 { return v > 0 }  // ③ 签名
    if v := bytes.Compare(x.VersionID[:], o.VersionID[:]); v != 0 { return v > 0 }  // ④ 版本ID
    if x.Flags != o.Flags     { return x.Flags > o.Flags }       // ⑤ 标志
    return false
}
```
- 注释坦言 ②③④⑤ "doesn't make too much sense, but we want sort to be consistent nonetheless"。
- **意义**：当多块盘对"哪个是最新版本"有分歧（modtime 相同等）时，必须有一个**所有盘都会算出
  同样结果**的全序，否则不同盘选出不同"最新版"会导致不一致。这一串平局裁决就是为了**确定性**。

---

## 4. 一个对象版本 `xlMetaV2Object`：字段与惰性分配

```go
// :156（含 msgp 标签，决定盘上序列化的 key 名）
type xlMetaV2Object struct {
    VersionID        [16]byte   `msg:"ID"`
    DataDir          [16]byte   `msg:"DDir"`     // 指向哪个 DataDir 子目录
    ErasureAlgorithm ErasureAlgo`msg:"EcAlgo"`   // ReedSolomon
    ErasureM         int        `msg:"EcM"`      // 数据块（每对象独立！）
    ErasureN         int        `msg:"EcN"`      // 校验块
    ErasureBlockSize int64      `msg:"EcBSize"`
    ErasureIndex     int        `msg:"EcIndex"`  // 本盘在条带里的 1-based 位置
    ErasureDist      []uint8    `msg:"EcDist"`   // 分布数组（分片打散顺序）
    PartNumbers      []int      `msg:"PartNums"`
    PartETags        []string   `msg:"PartETags,allownil"`
    PartSizes        []int64    `msg:"PartSizes"`
    PartActualSizes  []int64    `msg:"PartASizes,allownil"`  // 压缩前大小
    PartIndices      [][]byte   `msg:"PartIdx,omitempty"`    // 压缩索引
    Size             int64      ...
    ModTime          int64      ...
    MetaSys          map[string][]byte   // 系统内部元数据（x-minio-internal-*）
    MetaUser         map[string]string   // 用户自定义元数据
}
```
`★ 细节：PartETags / PartIndices 的惰性分配 ──────`
在 `AddVersion`（`:1652`）里：
```go
for i := range fi.Parts {
    if fi.Parts[i].ETag != "" { ventry.ObjectV2.PartETags = make([]string, len(fi.Parts)); break }
}
for i := range fi.Parts {
    if len(fi.Parts[i].Index) > 0 { ventry.ObjectV2.PartIndices = make([][]byte, len(fi.Parts)); break }
}
```
- **只有当真有 part 带 ETag / 压缩索引时，才分配那个切片**。普通单 part PUT 没有 part-level ETag，
  这两个切片就保持 nil，序列化时（`allownil`/`omitempty`）省掉。海量对象下，这种"按需才占字节"的
  抠门积少成多。
`──────────────────────────────────────────`

---

## 5. 写入一个版本：`AddVersion` 的全部门道

`AddVersion`（`:1595`）把一个 `FileInfo` 转成 `xlMetaV2Version` 并插入。逐个门道：

### ① null 版本 与 WrittenByVersion
```go
if fi.VersionID == "" { fi.VersionID = nullVersionID }   // 未开版本控制 → "null"
ventry := xlMetaV2Version{ WrittenByVersion: globalVersionUnix }   // 记录"哪个 MinIO 版本写的"
```
- 未开 versioning 的对象，版本号统一是 `nullVersionID`（特殊常量），盘上不真存这个字符串。
- `WrittenByVersion`（MinIO 自身的版本时间戳）让运维能判断"这条数据是哪个 server 版本写的"，
  排查兼容问题时有用。

### ② MetaSys vs MetaUser 的拆分，以及被跳过的瞬态 key
```go
// :1684
for k, v := range fi.Metadata {
    if strings.HasPrefix(k, ReservedMetadataPrefixLower) {   // "x-minio-internal-"
        switch k {
        case tierFVIDKey, tierFVMarkerKey, xMinIOHealing, xMinIODataMov:
            continue   // ★ 这些是瞬态 key，只在 RenameData/free-version 流程用，不持久化进版本
        }
        ventry.ObjectV2.MetaSys[k] = []byte(v)    // 系统内部元数据
    } else {
        ventry.ObjectV2.MetaUser[k] = v           // 用户元数据
    }
}
```
`★ 细节 ────────────────────────────────────`
- **以 `x-minio-internal-` 前缀区分系统/用户元数据**。系统的进 `MetaSys`（值是 `[]byte`，可存
  二进制如封装密钥、复制状态），用户的进 `MetaUser`（纯字符串）。
- **`xMinIOHealing` / `xMinIODataMov` / tier free-version key 被显式跳过**：它们是流程内传递的
  瞬态信号（"这是 heal 写的""这是重平衡搬运""这是 tier 自由版本标记"），不该固化进对象版本。
  忘了跳过这类瞬态 key 会污染对象元数据——这是改 RenameData/heal 路径时要警惕的坑。
`──────────────────────────────────────────`

### ③ inline 数据按 versionID 存入
```go
// :1701
if len(fi.Data) > 0 || fi.Size == 0 {
    x.data.replace(fi.VersionID, fi.Data)    // inline 数据挂在这个 versionID 名下
}
```
- 注意 `fi.Size == 0` 也走 inline（空对象也"内联"一个空数据，统一处理）。

### ④ 分层（tiering）元数据
```go
// :1705  对象被转储到远端 tier 时，记录 tier 名、远端对象名、远端版本、状态
ventry.ObjectV2.MetaSys[metaTierStatus]   = []byte(fi.TransitionStatus)
ventry.ObjectV2.MetaSys[metaTierObjName]  = []byte(fi.TransitionedObjName)
ventry.ObjectV2.MetaSys[metaTierVersionID]= []byte(fi.TransitionVersionID)
ventry.ObjectV2.MetaSys[metaTierName]     = []byte(fi.TransitionTier)
```
- 转储后本地不再存 part 数据，只在 `xl.meta` 里留这几个"指针"，读时据此去远端拉（第 2 篇 GET 的
  `objInfo.IsRemote()` 分支）。

### ⑤ 替换 or 新增
```go
// :1727
for i := range x.versions {
    if x.versions[i].header.VersionID != uv { continue }
    switch x.versions[i].header.Type {
    case LegacyType: return x.setIdx(i, ventry)   // V1 旧版被新 ObjectType 顶替（清掉 null 版）
    case ObjectType: return x.setIdx(i, ventry)   // 同版本号覆盖
    case DeleteType: return x.setIdx(i, ventry)   // 删除标记可被对象替换（非严格 S3，留作灵活）
    }
}
return x.addVersion(ventry)   // 没找到同版本号 → 新增
```
- **同 VersionID 存在则替换（`setIdx`），否则新增（`addVersion`）**。`LegacyType` 被替换时
  相当于把 V1 旧对象就地升级成 V2 格式。

### `addVersion`：按 modtime 降序插入 + 版本数上限
```go
// :1144
func (x *xlMetaV2) addVersion(ver xlMetaV2Version) error {
    encoded, _ := ver.MarshalMsg(nil)
    if int64(len(x.versions)+1) > globalAPIConfig.getObjectMaxVersions() {
        return errMaxVersionsExceeded    // ★ 单对象版本数上限（防版本爆炸）
    }
    x.versions = append(x.versions, ...{header: {ModTime: -1}})   // 先占位
    for i, existing := range x.versions {
        if existing.header.ModTime <= modTime {       // 线性查找插入点（通常插在最前）
            copy(x.versions[i+1:], x.versions[i:])
            x.versions[i] = xlMetaV2ShallowVersion{header: ver.header(), meta: encoded}
            return nil
        }
    }
}
```
- **versions 永远按 modtime 降序**，index 0 是最新版。新版本通常插在最前，所以用线性查找
  （注释 "we likely have to insert at front"）而非二分——多数情况下第一次比较就命中。
- **`getObjectMaxVersions` 上限**：防止某个对象被反复覆盖产生无限版本，撑爆 `xl.meta`。

---

## 6. inline 数据：版本字节 + msgp map + 零拷贝读

```go
// xl-storage-meta-inline.go:28
type xlMetaInlineData []byte    // 整块就是一个 []byte
const xlMetaInlineDataVer = 1   // 第一个字节是版本号
```
布局：`[1 字节版本] [ msgp map: { versionID(string) → data(bin) } ]`

```go
// :51  find：在 inline map 里按 versionID 找数据，全程零拷贝
func (x xlMetaInlineData) find(key string) []byte {
    sz, buf, _ := msgp.ReadMapHeaderBytes(x.afterVersion())   // 跳过版本字节读 map 头
    for i := 0; i < sz; i++ {
        found, buf, _ = msgp.ReadMapKeyZC(buf)                // ZC = Zero Copy，直接切片不拷贝
        if string(found) == key { val, _, _ := msgp.ReadBytesZC(buf); return val }
        _, buf, _ = msgp.ReadBytesZC(buf)                     // 不匹配则跳过这个 value
    }
    return nil
}
```
`★ 细节 ────────────────────────────────────`
- **inline 数据本身是个 map**：因为一个 `xl.meta` 可能有多个版本都 inline，所以按 versionID 分键。
  `replace(versionID, data)`（`:225`）增改某版本的 inline 数据，`remove`/`rename` 管删/改名。
- **`...ZC`（Zero-Copy）系列读取**：`ReadMapKeyZC` / `ReadBytesZC` 直接返回底层 buffer 的切片，
  不分配不拷贝。读 inline 小对象时这意味着"几乎零开销地从 `xl.meta` 字节里切出数据"。这是 inline
  能让小对象读取极快的底层原因（呼应深读 02 的"inline 走内存 SectionReader"）。
- **`validate`/`repair`**（`:80`/`:114`）：加载时校验 inline map 结构，损坏能修复（丢弃坏条目），
  避免一条坏 inline 把整个 `xl.meta` 拖垮。
`──────────────────────────────────────────`

---

## 7. 序列化：`AppendTo` 手写 msgp + 自带 CRC

```go
// :1179
func (x *xlMetaV2) AppendTo(dst []byte) ([]byte, error) {
    // 预估容量，一次性扩容（避免多次 append 重新分配）
    sz := len(xlHeader) + len(xlVersionCurrent) + ... ; if cap(dst) < sz { ... }

    dst = append(dst, xlHeader[:]...)          // "XL2 "
    dst = append(dst, xlVersionCurrent[:]...)  // major/minor
    dst = append(dst, 0xc6, 0, 0, 0, 0)        // ★ msgp bin32 头占位，长度先填 0
    dataOffset := len(dst)

    dst = msgp.AppendUint(dst, xlHeaderVersion)
    dst = msgp.AppendUint(dst, xlMetaVersion)
    dst = msgp.AppendInt(dst, len(x.versions))
    for _, ver := range x.versions {
        tmp, _ = ver.header.MarshalMsg(tmp[:0])   // 先写 header
        dst = msgp.AppendBytes(dst, tmp)
        dst = msgp.AppendBytes(dst, ver.meta)     // 再写完整版本字节
    }
    // ★ 回填 bin32 的真实长度
    binary.BigEndian.PutUint32(dst[dataOffset-4:dataOffset], uint32(len(dst)-dataOffset))
    // ★ 追加 5 字节定长 CRC：0xce(muint32) + xxhash(meta 区)
    tmp = tmp[:5]; tmp[0] = 0xce
    binary.BigEndian.PutUint32(tmp[1:], uint32(xxhash.Sum64(dst[dataOffset:])))
    dst = append(dst, tmp[:5]...)
    return append(dst, x.data...), nil           // ★ inline 数据接在最后
}
```
`★ 序列化的三个讲究 ───────────────────────────`
- **bin32 占位再回填**：先写 `0xc6,0,0,0,0`（msgp 的 bin32 类型 + 4 字节长度占位），把版本数据
  写完知道总长后，再 `PutUint32` 回填真实长度。这样不必预先算长度，一遍写完。
- **自带 xxhash CRC（5 字节定长）**：对 meta 区算 xxhash 存进去，加载时（`Load`/`isIndexedMetaV2`）
  校验，**`xl.meta` 自身损坏也能被检出**（注释提到 "Prior to v1.3 this was variable sized"——
  这就是 minor 版本号的用途：记录格式细节演进）。
- **inline 数据放最后**：元数据（定长可校验）和 inline 数据（变长）物理分离。读元数据不必读 inline，
  读 inline 也不影响元数据 CRC。
`──────────────────────────────────────────`

---

## 8. 共享 DataDir 与删除回收：`SharedDataDirCount`

一个微妙问题：**多个版本能不能共用同一个 DataDir？** 能——比如服务端 copy（`x-amz-copy-source`）
可以让新版本指向旧版本的同一份数据。于是删一个版本时，**不能无脑删它的 DataDir**，得先看还有没有
别的版本在用。

```go
// :1751
func (x *xlMetaV2) SharedDataDirCount(versionID, dataDir [16]byte) int {
    if x.data.entries() > 0 && x.data.find(uuid.UUID(versionID).String()) != nil {
        return 0    // inline 对象没有独立 DataDir，不存在共享问题
    }
    var sameDataDirCount int
    for _, version := range x.versions {
        if version.header.Type != ObjectType || version.header.VersionID == versionID || !version.header.UsesDataDir() {
            continue
        }
        // 反序列化看它的 DataDir 是否等于待删版本的 DataDir
        if decoded.ObjectV2.DataDir == dataDir { sameDataDirCount++ }
    }
    return sameDataDirCount
}
```
`★ 细节 ────────────────────────────────────`
- `DeleteVersion`（`:1349`）删一个版本时返回它的 DataDir，调用方据此清理盘上的数据目录。但
  **只有 `SharedDataDirCount == 0`（没别的版本共用）才真正删数据目录**，否则只是从 `xl.meta`
  移除这个版本条目，数据留给共用它的版本。
- 忽略这个检查会导致**删一个版本误删了另一个版本还在用的数据**——这是版本化 + 服务端 copy 场景下
  的经典数据丢失陷阱，`SharedDataDirCount` 就是防它的。
`──────────────────────────────────────────`

### `DeleteVersion` 还要懂复制
`DeleteVersion`（`:1349`）大段逻辑在处理**复制感知的删除**：
- `VersionPurgeStatus`、`DeleteMarkerReplicationStatus` 决定是"真删"还是"标记为待复制删除后保留"
  （`updateVersion`）。
- 跨站点复制下，一个删除要先在本地标记复制状态、等复制确认后才真正清除——所以删除标记里会塞
  `ReplicationStatus`/`ReplicaStatus`/`VersionPurgeStatus` 等 MetaSys 项。
- 这解释了为什么"删了的对象有时还在盘上"——它在等复制把删除传播出去。

---

## 9. 跨盘版本合并：`mergeXLV2Versions`

list / heal 时，要把**多块盘各自的 `xl.meta` 版本列表**合并成一个权威列表（盘之间可能有分歧）。

```go
// :1926
func mergeXLV2Versions(quorum int, strict bool, requestedVersions int, versions ...[]xlMetaV2ShallowVersion) []xlMetaV2ShallowVersion
```
- 输入：N 块盘各自的（已按 modtime 降序的）版本列表。
- 算法：**多路归并**。每轮取各盘当前最前的版本（`tops`），若它们 header 全相同（`consistent`），
  直接采纳；若有分歧，用 `sortsBefore` 找"最新"的，并统计有多少盘认同它。
- **达到 `quorum` 个盘认同的版本才进入合并结果**——少数盘独有的"幽灵版本"被丢弃（除非它够 quorum）。
- `strict` vs 非 strict：非严格模式用 `matchesNotStrict`（允许 EC 信息缺失的老对象按版本号+类型匹配）。

`★ 设计 ────────────────────────────────────`
- **这是"版本列表层面的 quorum"**。单个版本的数据有 quorum（够分片能重建），版本的**存在性**
  也要 quorum（够多盘记录了这个版本，它才算真存在）。`mergeXLV2Versions` 把后者实现成一个
  确定性的多路归并——依赖 §3 的 `sortsBefore` 全序保证各盘算出一致结果。
- heal 正是基于合并结果：权威列表里有、但某盘缺的版本，就是要修到那块盘上的。
`──────────────────────────────────────────`

---

## 10. 一页纸总结 `xl.meta` 的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 一对象 = 目录 + 一个 `xl.meta` + 每版本一个 DataDir | 多版本共账本，数据按 DataDir 隔离 |
| 2 | major 防降级 / minor 信息性 | 改格式 bump 错会破坏滚动升级 |
| 3 | 三种版本类型 + free-version（仅 scanner 可见） | 软删除、V1 兼容、远端 tier 异步回收 |
| 4 | shallow version = 轻量 header + 惰性 `meta []byte` | 列/排序/比对不必反序列化全部版本 |
| 5 | `sortsBefore` 全序平局裁决 | 各盘对"最新版"算出一致结果（确定性） |
| 6 | PartETags/PartIndices 惰性分配 | 海量对象按需才占字节 |
| 7 | MetaSys/MetaUser 按 `x-minio-internal-` 前缀拆分 | 系统二进制元数据 vs 用户字符串元数据 |
| 8 | 瞬态 key（healing/datamov/tierFV）写版本时跳过 | 不让流程信号污染持久化元数据 |
| 9 | versions 按 modtime 降序 + 版本数上限 | index 0 即最新；防版本爆炸 |
| 10 | inline = 版本字节 + msgp map{versionID:data} + 零拷贝读 | 小对象数据随元数据走，读取近零开销 |
| 11 | AppendTo：bin32 占位回填 + xxhash 自带 CRC | 一遍写完、元数据自校验 |
| 12 | `SharedDataDirCount` 防共享 DataDir 被误删 | 版本化 + 服务端 copy 下的数据安全 |
| 13 | DeleteVersion 复制感知 | 删除要等复制传播，"删了还在"有原因 |
| 14 | `mergeXLV2Versions` 版本存在性 quorum | 版本列表层面的多数派合并，heal 的依据 |

下一篇深读：**`xlStorage` 单盘实现**——`RenameData` 的原子性细节、O_DIRECT 对齐写、
`ReadXL`/`ReadFileStream`、目录 fsync 与崩溃一致，以及 inline 与非 inline 在盘上的落地差异。
