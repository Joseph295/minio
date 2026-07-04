# MinIO 改进路线图 / TODO

> 基于 [源码精读系列](deep/00-index.md) 反推出的改进清单。每条标注 **难度 / 风险 / 影响面 /
> 代码区域 / 关联深读篇**。
>
> 图例：难度 🟢易 🟡中 🔴难 · 风险 ⚪低 🟠中 🔴高
>
> **重要前提**：MinIO 很多"缺点"是**有意的取舍**（换来零依赖、单二进制、部署极简）。A 类改动是在
> 挑战它的立身之本——收益大但风险极高，社区审查极严。新人建议从 🟢/B 类入手积累信任，再碰核心。

---

## A. 架构级短板（核心取舍的代价，高价值高风险）

> 洞察：A1/A2/A3 同源于"**无中心元数据 + 位置靠算**"这一个核心决策。改它们等于重新在
> "简单性 vs 灵活性/性能"之间做选择。

- [ ] **A1 · 扩容模型僵硬** 🔴🔴
  - 问题：Set 数进哈希，一旦确定不能改；扩容只能加新 Pool，不能往老 Set 加盘；加 pool 后需 rebalance 逐对象重写迁移，慢且吃 I/O。
  - 根因：无中心元数据 → 位置靠哈希 → 改哈希=全量迁移。
  - 方向：一致性哈希 + 虚拟节点让加盘平滑；或 set 级在线渐进 rehash（不影响读写）。
  - 代码：`cmd/erasure-sets.go`（getHashedSet）、`erasure-server-pool*.go`、`*-decom/rebalance.go`
  - 关联：[deep/05](deep/05-pool-set-routing-deep.md) · [deep/17](deep/17-decom-rebalance-deep.md)

- [ ] **A2 · 海量对象列举 / 无元数据索引层** 🔴🟠
  - 问题：列举要遍历所有盘 + metacache 缓存；数十亿对象、深前缀、高频 List 时成本线性上升。
  - 对比：Ceph RGW 有 bucket index，AWS S3 有内部索引；MinIO 走"扫盘+缓存"。
  - 方向：可选 per-set 嵌入式 KV 索引（RocksDB/badger）加速列举与前缀查询；增强现有 bloom filter。
  - 代码：`cmd/metacache-*.go`、`cmd/data-scanner.go`
  - 关联：[deep/23](deep/23-listing-metacache-deep.md) · [deep/12](deep/12-scanner-lifecycle-deep.md)

- [ ] **A3 · 一致性模型的边界** 🔴🔴
  - 问题：① 分布式锁 quorum+续约非线性一致，极端分区有理论边界；② site-repl 用 LWW 时间戳冲突解决，依赖时钟，并发写会丢一个；③ 无跨对象原子事务。
  - 方向：关键元数据可选强共识（Raft）；**向量时钟/因果一致替代 LWW**（相对可控的切入点）。
  - 代码：`internal/dsync/`、`cmd/site-replication.go`
  - 关联：[deep/07](deep/07-dsync-locks-deep.md) · [deep/15](deep/15-site-replication-deep.md)

- [ ] **A4 · 纠删码小对象写放大** 🔴🟠
  - 问题：每个对象 N+K 次盘写；海量小对象 IOPS 压力大；无对象合并/打包（除 batch snowball）。
  - 方向：log-structured 小对象聚合写（类似 Haystack/f4），多个小对象打进一个 blob 一次纠删码。
  - 代码：`cmd/erasure-object.go`、`xl-storage*.go`（需改存储格式，兼容性挑战大）
  - 关联：[deep/01](deep/01-write-path-deep.md) · [deep/03](deep/03-xlmeta-format-deep.md)

- [ ] **A5 · 空间回收滞后 / 配额不精确** 🟡🟠
  - 问题：删除不立即回收（delete marker/free-version/dangling/未完成 multipart/tier 残留靠 scanner 异步清）；用量是采样快照 → 配额执行滞后一个扫描周期。
  - 方向：增量实时用量计数器（替代纯采样）；更主动的孤儿检测与即时回收。
  - 代码：`cmd/data-usage-cache.go`、`cmd/data-scanner.go`、`cmd/bucket-quota.go`
  - 关联：[deep/12](deep/12-scanner-lifecycle-deep.md) · [deep/13](deep/13-healing-mrf-deep.md)

---

## B. 工程 / 运维级短板（更接地气，更易改）

- [ ] **B1 · 配置易踩坑** 🟢⚪ — ENV 覆盖 config.json 优先级令人困惑；加更严校验 + 更友好错误提示。代码：`cmd/config*.go` · [deep/26](deep/26-config-subsystem-deep.md)
- [ ] **B2 · 限流是节点级非集群级** 🟡🟠 — `maxClients` 每节点信号量；探索集群级协调限流。代码：`cmd/handler-api.go` · [deep/30](deep/30-peripheral-modules-notes.md)
- [ ] **B3 · 可观测性门槛高** 🟢⚪ — 锁争用/heal 进度/复制积压排障难；加更细指标、trace 标签、面板。代码：`cmd/metrics-*.go` · [deep/16](deep/16-metrics-v2-deep.md)
- [ ] **B4 · 加密运维高危** 🟡🟠 — SSE-C 丢钥即丢数据；builtin KMS 仅测试；密钥轮换不平滑。方向：密钥托管/恢复选项、平滑轮换。代码：`internal/kms/`、`internal/crypto/` · [deep/10](deep/10-crypto-sse-deep.md)
- [ ] **B5 · 高延迟 tier 读无缓存** 🟡🟠 — 转储对象回读慢，无热缓存层。方向：可选读缓存/加速层（独立模块，不动核心）。代码：`cmd/bucket-lifecycle.go`、`cmd/tier.go` · [deep/18](deep/18-lifecycle-ilm-deep.md)
- [ ] **B6 · 代码耦合** 🟡⚪ — cmd 包巨大、全局变量众多、DI 弱、手写 msgp 改格式易错。方向：解耦、增强可测性。代码：`cmd/`（admin-handlers 等）· [deep/03](deep/03-xlmeta-format-deep.md)

---

## C. 贡献路线（按难度分层，建议从上到下）

### 🟢 上手层（低风险、易被接受，先建立信任）
- [ ] **C1 · 补测试** — 给核心路径加 `_test.go` 边界 case 覆盖（测试=最好的用法契约，最受欢迎的 PR）
- [ ] **C2 · 可观测性增量** — 加指标/trace 标签/审计字段（纯增量，不动核心逻辑）→ 对应 B3
- [ ] **C3 · 配置校验与错误提示** — 减少运维踩坑 → 对应 B1
- [ ] **C4 · good first issue** — 从 GitHub issues 挑标记项，小 bug/边界修复

### 🟡 进阶层（中风险、有技术含量）
- [ ] **C5 · 热路径性能优化** — 复制 deep/01/02 的 buffer 池 / 记录复用 / 精确容量 思路到别处；列举/扫描的实现级优化（不改架构）
- [ ] **C6 · 运维 UX** — 改进 `mc admin` 体验、heal/rebalance 进度与可控性 → 对应 deep/29
- [ ] **C7 · 可选读缓存层** — 给高延迟 tier 加可选热缓存（独立模块）→ 对应 B5

### 🔴 研究层（高风险、需社区共识，长期投入）
- [ ] **C8 · 元数据索引层** — 加速列举/前缀查询 → 对应 A2
- [ ] **C9 · 小对象聚合** — log-structured 打包 → 对应 A4
- [ ] **C10 · 一致性增强** — 向量时钟替代 site-repl LWW（相对 A1 更可控、价值高）→ 对应 A3
- [ ] **C11 · 在线渐进式扩容/rebalance** — 最难但价值最高 → 对应 A1

---

## 推荐起点

> **最优切入组合**：先做 🟢（C1 补测试 + C2 可观测性 + C3 配置校验）熟悉代码、建立社区信任；
> 想做有分量的贡献，**A3（一致性，向量时钟替代 LWW）或 A2（元数据索引）** 是相对可控、价值又高的
> 切入点——它们能做成**可选特性**而不必推翻现有设计，比 A1（扩容）风险低。

---

## 备注

- 每条 A 类改动落地前，务必：① 在 GitHub 开 issue/discussion 与维护者对齐设计；② 准备充分的
  兼容性与故障注入测试（数据安全相关，社区审查极严）。
- 改存储格式（A4）要动 `xl.meta`（deep/03）——注意 major/minor 版本号与滚动升级兼容。
- 这份清单是"反推"出的方向性建议，不代表官方 roadmap；实际优先级请以 MinIO 社区/issues 为准。
