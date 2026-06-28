# 深读 05 · Pool/Set 路由与扩缩容逐层精读

> 概览第 1 篇说过"位置靠算不靠查"。本篇把这句话的代码兑现：对象如何被两次哈希定位到
> Pool 和 Set（`cmd/erasure-sets.go` / `erasure-server-pool.go`），分片如何按 `hashOrder`
> 在 Set 内打散，以及为什么"扩容只能加新 Pool"、退役（decommission）和重平衡（rebalance）
> 如何在不改老 Set 哈希的前提下迁移数据。

---

## 1. 第二跳：对象 → Set（确定性哈希）

```go
// erasure-sets.go:695
func (s *erasureSets) getHashedSetIndex(input string) int {
    return hashKey(s.distributionAlgo, input, len(s.sets), s.deploymentID)
}
func (s *erasureSets) getHashedSet(input string) *erasureObjects {
    return s.sets[s.getHashedSetIndex(input)]   // 同一对象名 → 永远同一个 Set
}
```

### 三种分布算法
```go
// format-erasure.go:55
formatErasureVersionV2DistributionAlgoV1 = "CRCMOD"          // V2 旧版
formatErasureVersionV3DistributionAlgoV2 = "SIPMOD"          // V3
formatErasureVersionV3DistributionAlgoV3 = "SIPMOD+PARITY"   // V3，当前默认
```
```go
// erasure-sets.go:682
func hashKey(algo, key string, cardinality int, id [16]byte) int {
    switch algo {
    case CRCMOD:                return crcHashMod(key, cardinality)         // crc32(key) % setCount
    case SIPMOD, SIPMOD+PARITY: return sipHashMod(key, cardinality, id)     // siphash(key, key=deploymentID) % setCount
    }
}
// :663
func sipHashMod(key string, cardinality int, id [16]byte) int {
    k0, k1 := binary.LittleEndian.Uint64(id[0:8]), binary.LittleEndian.Uint64(id[8:16])
    return int(siphash.Hash(k0, k1, []byte(key)) % uint64(cardinality))
}
```
`★ 为什么用 deploymentID 当 SipHash 密钥 ─────────`
- **CRC32 是无密钥的固定哈希**：所有 MinIO 部署对同一对象名算出同样的 CRC，分布模式可预测。
  攻击者能构造大量"都落到同一个 Set"的对象名，制造热点（哈希碰撞 DoS）。
- **SipHash 以本部署唯一的 `deploymentID`（16 字节）为密钥**：每个集群的哈希映射都不同且不可
  从外部预测，**抗碰撞攻击**，分布也更均匀。这是 V3 相比 V2 的安全升级。
- **`deploymentID` 一旦确定就不能变**：它进了哈希函数，改了等于所有对象的 Set 映射全变 =
  数据全迁。所以它是集群的"出生证"，写在 format.json 里，终生不变。
`──────────────────────────────────────────`

### "SIPMOD+PARITY" 与 V2 的区别
- `SIPMOD+PARITY` 在 `SIPMOD` 基础上，让 **parity 的分布也参与**——配合每对象可变 parity
  （深读 01 §2），让校验块在盘间更均匀。算法名直接编码进 format.json，**升级集群时算法保持
  不变**以兼容老对象的定位。

---

## 2. 第三跳：分片 → 盘（`hashOrder` 打散）

选定 Set 后，对象的 N+K 个分片不是顺序写到盘 0..N+K-1，而是按 `hashOrder` 生成的**每对象专属
排列**打散：

```go
// erasure-metadata-utils.go:178
func hashOrder(key string, cardinality int) []int {
    nums := make([]int, cardinality)
    keyCrc := crc32.Checksum([]byte(key), crc32.IEEETable)
    start := int(keyCrc % uint32(cardinality))
    for i := 1; i <= cardinality; i++ {
        nums[i-1] = 1 + ((start + i) % cardinality)   // 1-based 旋转排列
    }
    return nums
}
```
`★ 细节 ────────────────────────────────────`
- **这就是 `xl.meta` 里 `ErasureDist` 的来源**（深读 03 §4）。它是一个由对象名 CRC 决定起点的
  **旋转排列**：对象 A 可能是 `[3,4,5,...,1,2]`，对象 B 是 `[7,8,...,5,6]`。结果是不同对象的
  "第 0 个分片"落在不同物理盘上。
- **注释明说 "collisions are fine, we are not looking for uniqueness"**：两个对象的 hashOrder
  可以相同，没关系——目的不是唯一性，而是**让分片在盘间均摊**，避免某块盘永远只存校验块（IO 倾斜）。
- **1-based**：返回的盘号从 1 开始（0 在 `ErasureIndex` 里被保留为"未设"，深读 01 §7 的
  `Erasure.Index = index+1` 与此呼应）。
- 读时 `shuffleDisksAndPartsMetadataByIndex`（深读 02 §3）按 `ErasureDist` 把盘序还原回逻辑顺序。
`──────────────────────────────────────────`

至此，**"对象名 → 物理位置"完全由三次哈希确定**（Pool→Set→盘内分布），无需任何中心元数据表。
这就是"位置靠算不靠查"的全部代码。

---

## 3. 第一跳：对象 → Pool（PUT 时的选择）

多 Pool 部署时，PUT 要先选 Pool。入口 `getPoolIdx`（`erasure-server-pool.go:637`，专供 PutObject/
CopyObject/NewMultipartUpload）：

```go
func (z *erasureServerPools) getPoolIdx(ctx, bucket, object string, size int64) (idx int, err error) {
    idx, err = z.getPoolIdxExistingWithOpts(ctx, bucket, object, ObjectOptions{
        SkipDecommissioned: true,    // ★ 不往退役中的 Pool 写
        SkipRebalancing:    true,    // ★ 不往重平衡中的 Pool 写
    })
    if isErrObjectNotFound(err) {
        idx = z.getAvailablePoolIdx(ctx, bucket, object, size)  // 对象不存在 → 选最空的 Pool
        if idx < 0 { return -1, toObjectErr(errDiskFull) }
    }
    return idx, nil
}
```
**两步策略**：① 先查对象是否**已存在**于某个 Pool（覆盖写要落回原 Pool）；② 不存在则选**剩余
空间最充足**的 Pool。

### ① 查"对象已在哪个 Pool"：并行问所有 Pool
```go
// :494 getPoolInfoExistingWithOpts
for i, pool := range z.serverPools {
    go func(...) { pinfo.ObjInfo, pinfo.Err = pool.GetObjectInfo(ctx, bucket, object, opts) }(...)
}
wg.Wait()
// 按 ModTime 降序排（防御性：万一多 Pool 都有，服务最新的那个）
sort.Slice(poolObjInfos, func(i, j int) bool { return poolObjInfos[i].ObjInfo.ModTime.After(...) })
```
`★ 细节 ────────────────────────────────────`
- **并行问所有 Pool "你有没有这个对象"**，按 ModTime 降序，取最新。注释说这是"defensive change
  to handle any duplicate content"——正常情况下一个对象只在一个 Pool，但若因故障/迁移产生了
  重复，**永远服务 ModTime 最新的那份**。
- **`isErrReadQuorum` 也算"在这个 Pool"**（`:548`）：如果对象在某 Pool 可见但读不出 quorum，
  仍把写调度到这个 Pool（覆盖/加新版本），而不是另选 Pool 制造重复。注释解释得很细——
  "visibly present but unreadable, schedule writes to this pool instead"。这避免了"读不出旧的
  就在别处建新的"导致的双份。
- **`SkipDecommissioned`/`SkipRebalancing`**：退役中或重平衡中的 Pool 被跳过，新写不进去——
  这是数据迁移期间"只出不进"的关键（见 §5）。
`──────────────────────────────────────────`

### ② 选最空 Pool：`getServerPoolsAvailableSpace`
```go
// :416
for index := range z.serverPools {
    if z.IsSuspended(index) || z.IsPoolRebalancing(index) { continue }   // 跳过退役/重平衡中的
    storageInfos[index] = getDiskInfos(ctx, pool.getHashedSet(object).getDisks()...)  // ★ 只看该对象会落的那个 Set
}
for i, zinfo := range storageInfos {
    if !isMinioMetaBucketName(bucket) {
        if avail, err := hasSpaceFor(zinfo, size); err != nil || !avail { continue }   // 放不下就排除
    }
    for _, disk := range zinfo {
        available += disk.Total - disk.Used
        if pctUsed := disk.Used*100/disk.Total; pctUsed > maxUsedPct { maxUsedPct = pctUsed }
    }
    available *= uint64(nSets[i])   // ★ 按 Set 数归一化（Pool 大小不同时公平比较）
    serverPools[i] = poolAvailableSpace{Index: i, Available: available, MaxUsedPct: maxUsedPct}
}
```
`★ 细节 ────────────────────────────────────`
- **只看"该对象会落的那个 Set"的盘**（`pool.getHashedSet(object).getDisks()`），不是整个 Pool
  的平均。因为对象一定落在那个 Set，要确保那个 Set 放得下。这避免"Pool 整体有空间但目标 Set
  恰好满了"的误判。
- **`available *= nSets`（按 Set 数归一化）**：不同 Pool 可能 Set 数不同。比较"哪个 Pool 更空"
  时乘以 Set 数，补偿规模差异，让大 Pool 不会仅因为"单 Set 可用空间"被低估。注释解释了这点。
- **`hasSpaceFor` 预留**：还会预留一定比例空间（避免写满盘导致无法 heal/rebalance）。系统桶
  （`.minio.sys`）不受此限（IAM/配置等关键数据优先）。
`──────────────────────────────────────────`

---

## 4. 为什么扩容只能"加新 Pool"

`getHashedSet` 用 `len(s.sets)`（Set 数）做模。**改 Set 数 = 模数变 = 几乎所有对象重新映射到
不同 Set = 全量数据迁移**。所以 MinIO **不允许往已有 Pool 里加盘改变 Set 数**。

扩容的正确姿势是**加一个新 Pool**（`minio server http://node{1...N}/disk{1...M} http://newnode{1...K}/disk{1...M}`）：
- 老 Pool 的 Set 数、哈希、deploymentID 全不变 → **老对象定位不变，零迁移**。
- 新写入通过 `getAvailablePoolIdx` 倾向落到更空的新 Pool（自然填充）。
- 读老对象走老 Pool、读新对象走新 Pool，`getPoolIdxExisting` 并行查所有 Pool 找到它。

`★ 这就是 MinIO 扩容模型的本质 ───────────────────`
- **Pool 是扩容单位，Set 是哈希单位**。Set 数固定保证老数据定位稳定；Pool 可叠加保证容量可扩。
  这是"无中心元数据"的代价与智慧：不能像有 master 的系统那样随意 reshuffle，但换来了无单点、
  无元数据热点。理解这点，你就理解了为什么 MinIO 的部署/扩容文档长那样。
`──────────────────────────────────────────`

---

## 5. 退役（Decommission）与重平衡（Rebalance）

这两个是"在哈希约束下迁移数据"的机制，都靠"**目标 Pool 只出不进**（Skip 标志）+ 逐对象搬运"实现。

### Decommission：清空一个 Pool 再移除
- 入口 `Decommission`（`erasure-server-pool-decom.go:1269`）→ `decommissionInBackground`（`:1080`）。
- 机制：把被退役 Pool 标记为 **Suspended**（`IsSuspended` 返回 true）→ 所有新写经 `SkipDecommissioned`
  避开它 → 后台**遍历该 Pool 的所有对象，逐个重新 PutObject**。
- 重新 PUT 时 `getAvailablePoolIdx` 因为跳过了被退役 Pool，自然把对象写到**其它 Pool**，
  完成数据搬离。全部搬完，这个 Pool 就能安全下线。
- **优雅之处**：复用了正常的 PUT 路径（纠删码、quorum、加密原样保留），没有专门的"迁移协议"。
  退役 = "把对象在系统内重新写一遍，落到别处"。

### Rebalance：新 Pool 加入后摊平使用率
- 入口在 `erasure-server-pool-rebalance.go`，状态记在 `rebalanceMeta`（`:99`）。
- 触发：加了新（空）Pool 后，老 Pool 可能很满、新 Pool 很空，使用率失衡。Rebalance 把对象从
  使用率高的 Pool 搬到低的，直到各 Pool 使用率接近。
- 机制同 decommission：参与 rebalance 的 Pool 经 `SkipRebalancing` 避开新 I/O 干扰，后台逐对象
  搬运，搬运也是走"重新写一遍落到目标 Pool"。
- 与 decommission 的区别：decommission 是**清空**一个 Pool（要移除它），rebalance 是**摊平**
  多个 Pool（都保留）。

`★ 共同设计：数据迁移 = 在系统内重写对象 ───────────`
- decommission / rebalance / 甚至 heal，本质都是"读出对象 → 按当前规则重新写入 → 落到正确位置"。
  没有底层的块迁移协议，全部复用 ObjectLayer 的读写路径。这让迁移天然继承了纠删码、加密、
  版本、quorum 的所有保证——**迁移期间崩溃也安全**（重写没完成的对象，老位置还在，重试即可）。
- **Skip 标志（SkipDecommissioned/SkipRebalancing/IsSuspended）是迁移的方向阀**：它们让被迁移的
  Pool 进入"只读+只出"状态，保证搬运过程中不会有新数据又写进来，使搬运能收敛。
`──────────────────────────────────────────`

---

## 6. 一页纸总结路由与扩缩容的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | 对象→Set 用 `len(sets)` 取模 | Set 数固定保证定位稳定 |
| 2 | SipHash 以 deploymentID 为密钥 | 抗哈希碰撞 DoS，分布均匀，每集群不可预测 |
| 3 | deploymentID 终生不变 | 进了哈希，改了=全量迁移 |
| 4 | `hashOrder` 旋转排列 = `ErasureDist` | 分片按对象打散到不同盘，均摊 IO；碰撞无所谓 |
| 5 | PUT 选 Pool：先查已存在、再选最空 | 覆盖写落回原 Pool，新对象填空 |
| 6 | 并行查所有 Pool + ModTime 最新优先 | 防御重复内容，永远服务最新 |
| 7 | ReadQuorum 错也算"在本 Pool" | 避免"读不出旧的就在别处建新的"双份 |
| 8 | 选 Pool 只看目标 Set 的盘 + 按 Set 数归一化 | 精确判断目标 Set 放得下、跨 Pool 公平比较 |
| 9 | 扩容=加新 Pool（不改老 Set 数） | 老数据零迁移，无单点的代价与智慧 |
| 10 | decommission/rebalance = 逐对象重写 | 复用读写路径，迁移继承所有保证，崩溃安全 |
| 11 | Skip 标志 = 迁移方向阀 | 被迁移 Pool 只出不进，搬运可收敛 |

下一篇深读：**`internal/grid` 节点间 RPC**——连接状态机、WebSocket 握手、mux 多路复用的
建立与拆除、背压令牌、自动重连与消息合并。
