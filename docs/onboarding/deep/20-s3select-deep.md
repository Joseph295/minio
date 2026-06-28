# 深读 20 · S3 Select 查询下推逐层精读

> S3 Select 让你用 **SQL 直接查询对象内容**（CSV/JSON/Parquet）——服务端读对象、跑 SQL、
> **只返回匹配的行**，而不是把整个对象下载到客户端再过滤。本篇拆开 SQL 下推流程、多格式记录
> 读取、即时解压、流式求值、聚合处理（`internal/s3select/` + `internal/s3select/sql/`）。

---

## 1. 核心价值：把计算推到数据旁边

```
传统：客户端 GET 整个 10GB CSV → 本地过滤出 100 行
S3 Select：服务端读 10GB CSV → 跑 SQL → 只回传 100 行
```
- **省带宽**：不必传输整个对象，只传结果。对"大文件里捞少量行"的场景（日志分析、数据筛选）
  带宽节省可达几个数量级。
- 这是"**把计算下推到存储**"（compute pushdown）的经典模式——存储层做过滤/投影/聚合，
  只把精炼后的结果交给上层。

---

## 2. Handler 流程：解析 → 打开 → 求值

```go
// object-handlers.go:240 SelectObjectContentHandler
s3Select, _ := s3select.NewS3Select(r.Body)   // ① 解析 SQL 请求（XML：SQL 语句 + 输入/输出格式）
defer s3Select.Close()
s3Select.Open(objectRSC)                       // ② 在对象上打开 reader（含解密/解压/格式解析）
s3Select.Evaluate(w)                           // ③ 流式跑 SQL，结果写回 ResponseWriter
```
- **加密对象先解密再 select**（`:279-296`）：SSE-S3/KMS/C 的对象，handler 先验证/解封密钥，
  `objectRSC` 是解密后的流——S3 Select 跑在明文上。SSE-C 还要校验客户端密钥。

---

## 3. 输入格式与即时解压：`Open`

```go
// select.go:373
func (s3Select *S3Select) Open(rsc io.ReadSeekCloser) error {
    offset, length, _ := s3Select.ScanRange.StartLen()   // ★ 可只扫对象的一个字节范围
    switch s3Select.Input.format {
    case csvFormat:
        rsc.Seek(offset, seekDirection)
        rc := newLimitedReadCloser(rsc, length)
        s3Select.progressReader, _ = newProgressReader(rc, s3Select.Input.CompressionType)  // ★ 解压 + 进度
        s3Select.recordReader, _ = csv.NewReader(s3Select.progressReader, &CSVArgs)
    case jsonFormat:
        ...
        if JSONArgs.ContentType == "lines" {
            if simdjson.SupportedCPU() {
                s3Select.recordReader = simdj.NewReader(...)    // ★ SIMD 加速 JSON
            } else {
                s3Select.recordReader = json.NewPReader(...)    // 并行 JSON
            }
        } else {
            s3Select.recordReader = json.NewReader(...)         // 文档模式
        }
    case parquetFormat:
        if offset != 0 || length != -1 { return error }        // ★ parquet 不支持 offset
        s3Select.recordReader, _ = parquet.NewParquetReader(rsc, &ParquetArgs)
    }
}
```
`★ Open 的几个讲究 ─────────────────────────────`
- **`recordReader` 接口统一三种格式**（CSV/JSON/Parquet）。`Evaluate` 只调 `recordReader.Read()`
  逐条取记录，不关心底层格式——又一处接口抽象（呼应深读 19 Target、深读 18 WarmBackend）。
- **即时解压 `progressReader`**：对象若是 gzip/bzip2/s2/zstd/lz4 压缩的，**边读边解压**再喂给
  record reader。压缩对象的 select 无需先整体解压落盘。`Open` 里一长串压缩错误（`gzip.ErrHeader`
  等）映射成 `errInvalidCompression`——压缩格式不对能给出明确错误。
- **`ScanRange` 字节范围扫描**：CSV/JSON 可以只 seek 到对象的某段扫描（如只查文件后半部分）。
  **Parquet 不支持 offset**——因为 parquet 是列式格式，有自己的页/行组结构，字节偏移无意义。
- **SIMD JSON（simdj）**：CPU 支持时用 simdjson 库（SIMD 指令并行解析 JSON），比标量解析快数倍。
  这是为"JSON lines"这种高频格式做的硬件加速。
`──────────────────────────────────────────`

---

## 4. 流式求值 `Evaluate`：逐记录处理

```go
// select.go:517（主循环精简）
for {
    if s3Select.statement.LimitReached() { sendRecord(); writer.Finish(); break }  // ★ LIMIT 提前退出
    rec, err := s3Select.recordReader.Read(rec)        // ★ 读一条记录（复用 rec）
    if err == io.EOF {
        if statement.IsAggregated() {                  // ★ 聚合查询：EOF 时输出唯一结果
            statement.AggregateResult(outputRecord); outputQueue = append(...)
        }
        sendRecord(); writer.Finish(); break
    }
    inputRecords, _ := statement.EvalFrom(format, rec) // ★ FROM 子句（如展开 JSON 数组）
    for _, inputRecord := range inputRecords {
        if statement.IsAggregated() {
            statement.AggregateRow(*inputRecord)       // ★ 聚合：累加，不立即输出
        } else {
            outputRecord = 复用 outputQueue 里的记录对象（Reset）
            statement.Eval(*inputRecord, outputRecord) // ★ WHERE 过滤 + SELECT 投影
            // 满 100 条 → sendRecord() 批量发出
        }
    }
}
```
`★ 流式求值的设计 ─────────────────────────────`
- **逐条流式，不把整个对象读进内存**：一次读一条记录、求值、输出，内存占用与对象大小无关
  （呼应深读 01/02 的流式哲学）。10GB 文件也只占一条记录的内存。
- **聚合 vs 非聚合两种模式**：
  - **非聚合**（`SELECT a,b WHERE c`）：每条匹配 WHERE 的记录立即投影输出，攒够 100 条批量发。
  - **聚合**（`SELECT COUNT(*) / SUM(x)`）：每条记录 `AggregateRow` 累加到聚合器，**直到 EOF 才
    `AggregateResult` 输出唯一结果**。聚合查询天然要扫完全部数据才有答案。
- **记录对象复用**（`outputQueue[...].Reset()`）：不为每行 new 一个记录对象，而是复用队列里的旧
  对象 Reset 后重填。海量行查询下，这避免了每行一次堆分配 + GC 压力——又一处热路径抠内存
  （呼应深读 01/02 的 buffer 池）。
- **`EvalFrom`（FROM 子句）**：处理 `FROM S3Object[*].path` 这种——对 JSON 可以展开数组、下钻
  路径，把一条原始记录变成多条待查记录。
- **`LIMIT` 提前退出**：`SELECT ... LIMIT 10` 取够 10 条就停，不扫完整个文件。
- **`maxRecordSize` 1MB**：单条记录/结果超 1MB 报 `OverMaxRecordSize`——防御畸形数据撑爆内存。
`──────────────────────────────────────────`

---

## 5. SQL 引擎（`internal/s3select/sql/`）

S3 Select 自带一个**精简 SQL 引擎**：
- **`parser.go`**：解析 SQL（基于 `participle` 解析器生成器），支持 SELECT / FROM / WHERE / LIMIT、
  函数、聚合。
- **`statement.go`**：`SelectStatement`——`EvalFrom`（FROM）、`Eval`（WHERE+SELECT）、`AggregateRow`/
  `AggregateResult`（聚合）、`LimitReached`。
- **`evaluate.go` + `value.go`**：表达式求值与**类型系统**（`Value` 支持 string/int/float/bool/
  timestamp/null/bytes，带类型转换与比较）。
- **`funceval.go` + `stringfuncs.go` + `timestampfuncs.go`**：内置函数（字符串、时间、CAST 等）。
- **`aggregation.go`**：COUNT/SUM/AVG/MIN/MAX 聚合器。
- **`jsonpath.go`**：JSON 路径求值（`s.address.city` 这种下钻）。

`★ 细节：自带 SQL 引擎而非依赖外部 ───────────────`
- MinIO **内置**这套 SQL 引擎（而非嵌入 SQLite/DuckDB 等）。因为 S3 Select 的 SQL 子集很窄
  （单表、无 JOIN、面向半结构化数据），自己实现能精确控制语义、零外部依赖、流式友好。代价是
  功能受限（不支持 JOIN、子查询有限）——这是"够用就好、保持轻量"的取舍。
`──────────────────────────────────────────`

---

## 6. 输出：S3 Select 事件流协议

```go
// message.go：结果不是普通 HTTP body，而是分帧的事件流
// newMessageWriter → SendRecord（数据帧）/ 进度帧 / 统计帧 / End 帧
```
- S3 Select 的响应是一个**分帧的二进制事件流**（不是简单 body）：
  - **Record 帧**：批量结果记录。
  - **Progress 帧**（可选）：扫描进度（已扫字节/已处理字节/已返回字节）——长查询能看进度。
  - **Stats 帧**：最终统计。
  - **End 帧**：结束标记。
- 每帧带 CRC 校验。客户端 SDK 解析这个流还原成记录。这套协议让"边查边返回 + 进度反馈"成为可能。

---

## 7. 一页纸总结 S3 Select 的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | SQL 下推：服务端过滤，只回结果 | 大文件捞少量行省几个数量级带宽 |
| 2 | 加密对象先解密再 select | 跑在明文上，SSE-C 校验客户端密钥 |
| 3 | recordReader 接口统一 CSV/JSON/Parquet | 求值逻辑不关心格式 |
| 4 | progressReader 即时解压（gzip/zstd/lz4...） | 压缩对象无需先整体解压落盘 |
| 5 | ScanRange 字节范围扫描（parquet 除外） | 只查对象的一段 |
| 6 | SIMD JSON（simdj）硬件加速 | JSON lines 高频格式提速数倍 |
| 7 | 逐条流式求值，内存与对象大小无关 | 10GB 文件只占一条记录内存 |
| 8 | 聚合（EOF 出结果）vs 非聚合（逐行出） | 两类查询不同输出时机 |
| 9 | 输出记录对象复用（Reset） | 海量行查询避免每行堆分配 |
| 10 | LIMIT 提前退出、maxRecordSize 1MB | 取够即停、防畸形数据 |
| 11 | 自带精简 SQL 引擎（非外部库） | 窄子集、零依赖、流式友好 |
| 12 | 分帧事件流输出（record/progress/stats/end） | 边查边返回 + 进度反馈 |

---

## 系列三度收官：20 篇精读的全貌

```
存储核心    01 写入 · 02 读取 · 03 xl.meta · 04 单盘 · 05 路由
分布式      06 grid · 07 dsync
安全        08 签名 · 09 IAM · 10 加密
数据服务    11 复制 · 12 扫描 · 13 自愈
外围一      14 batch · 15 site-replication · 16 metrics
外围二      17 decom/rebalance · 18 lifecycle · 19 事件通知 · 20 s3 select
```

读到第 20 篇，你应该已经能在每个新模块里**一眼认出复用的核心模式**：

- **接口抽象吃异构**：StorageAPI（盘）/ WarmBackend（tier）/ Target（通知）/ recordReader（格式）—— 
  上层只依赖契约，底层换实现。
- **流式 + 内存无关**：写入/读取/复制/select 都逐块/逐条流式，内存占用与数据大小解耦。
- **持久化队列 + 重试**：MRF（复制/自愈）/ batch 检查点 / QueueStore（通知）—— 落盘、重启不丢、回放。
- **背压"宁降级不阻塞"**：复制/通知/metrics 满了就丢+计数，绝不拖垮前台。
- **单 leader + 后台 goroutine**：scanner / site-heal / 各类 worker 池。
- **崩溃一致：临时区 + 原子提交 / 检查点续传**。
- **热路径抠内存：buffer 池 / 记录复用 / 精确容量预留**。

这就是读透一个成熟系统的终极收获——**你掌握的不再是 20 个孤立模块，而是一套反复出现的设计语言**。
拿着这套语言，MinIO 里任何你还没读的文件，你都能独立读透。
```
```
