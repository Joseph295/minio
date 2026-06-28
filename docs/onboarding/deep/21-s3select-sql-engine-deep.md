# 深读 21 · S3 Select SQL 引擎内核逐层精读

> 深读 20 讲了 S3 Select 的整体下推流程。本篇下钻一层，拆开它自带的**精简 SQL 引擎**——
> 声明式语法解析、AST、树遍历解释器、动态类型系统、JSONPath 求值、函数与聚合
> （`internal/s3select/sql/`：parser.go / evaluate.go / value.go / jsonpath.go / funceval.go /
> aggregation.go）。

引擎三段式：**SQL 文本 → (参解析) AST → (树遍历解释器) 对每条记录求值**。

---

## 1. 声明式语法：participle 结构体标签即文法

MinIO 用 `participle`（解析器组合子库）——**文法直接写在 Go 结构体的 tag 里**：
```go
// parser.go:88  顶层 AST
type Select struct {
    Expression *SelectExpression `parser:"\"SELECT\" @@"`
    From       *TableExpression  `parser:"\"FROM\" @@"`
    Where      *Expression       `parser:"( \"WHERE\" @@ )?"`     // 可选
    Limit      *LitValue         `parser:"( \"LIMIT\" @@ )?"`     // 可选
}
type SelectExpression struct {
    All         bool                 `parser:"  @\"*\""`           // SELECT *
    Expressions []*AliasedExpression `parser:"| @@ { \",\" @@ }"`  // SELECT a, b AS x
}
```
`★ 声明式文法的价值 ───────────────────────────`
- **文法即类型**：`parser:` tag 描述如何从 token 流构造这个结构体（`@@` = 递归子规则、`{...}` =
  重复、`(...)? ` = 可选、`|` = 选择）。解析出来的就是一棵强类型 AST，无需手写递归下降解析器。
- **只支持 SELECT**（注释 `:85`）：S3 Select 不是通用 SQL——没有 INSERT/UPDATE/DELETE/JOIN。
  文法天然限定了能力边界（深读 20 §5 的"窄子集"在这里是字面意义的"文法里就没写"）。
`──────────────────────────────────────────`

### 字面量的 `Capture` 接口
```go
// parser.go:42  字符串字面量去引号 + 处理转义
func (ls *LiteralString) Capture(values []string) error {
    r := values[0][1 : len(values[0])-1]                  // 去掉两端单引号
    *ls = LiteralString(strings.ReplaceAll(r, "''", "'")) // SQL 里 '' 表示一个 '
    return nil
}
```
- participle 的 `Capture` 钩子让 token 在解析时就被规范化：字符串去引号、`''`→`'`、列表拆分。
  语义清洗发生在解析阶段，求值阶段拿到的是干净值。

---

## 2. 运算符优先级 = 文法嵌套深度

表达式文法（`parser.go:134` 的注释画出了产生式）：
```
Expression    → AndCondition ("OR"  AndCondition)*      ← 优先级最低（OR）
AndCondition  → Condition    ("AND" Condition)*         ←        （AND）
Condition     → "NOT" Condition | ConditionExpression   ←        （NOT）
ConditionExpr → ValueExpr (= <> <= >= < > | LIKE | BETWEEN | IN) ValueExpr   ← 比较
Operand       → MultOp  (("+"|"-") MultOp)*             ← 加减
MultOp        → UnaryTerm (("*"|"/"|"%") UnaryTerm)*    ← 乘除模
UnaryTerm     → "-" UnaryTerm | PrimaryTerm             ← 一元负号
PrimaryTerm   → Value | JSONPath | "(" Expression ")" | FuncCall   ← 最高（原子/括号）
```
`★ 优先级靠嵌套实现 ───────────────────────────`
- **运算符优先级不是靠优先级表，而是靠文法的嵌套层级**：OR 在最外层、PrimaryTerm 在最内层。
  解析 `a OR b AND c * 2` 时，`*` 在最深处先结合、`AND` 次之、`OR` 最后——嵌套深度天然编码了
  "谁先算"。这是表达式文法的经典手法（precedence climbing 的声明式版本）。
- **括号 `"(" Expression ")"` 回到 PrimaryTerm**：括号把一个完整 Expression"降"回最内层原子，
  从而能覆盖默认优先级。
`──────────────────────────────────────────`

---

## 3. 树遍历解释器：每个 AST 节点一个 `evalNode`

求值是**对每条输入记录、自顶向下遍历 AST**。每种节点实现 `evalNode(r Record, tableAlias) (*Value, error)`：
```go
// evaluate.go:55  Expression（OR）：短路
func (e *Expression) evalNode(r Record, tableAlias string) (*Value, error) {
    if len(e.And) == 1 { return e.And[0].evalNode(r, tableAlias) }   // ★ 单子节点：结果不必是 bool
    for _, ex := range e.And {
        res, _ := ex.evalNode(r, tableAlias)
        b, ok := res.ToBool(); if !ok { return nil, errExpectedBool }
        if b { return FromBool(true), nil }   // ★ OR 短路：一真即真
    }
    return FromBool(result), nil
}
// :81 AndCondition（AND）：一假即假短路
// :106 Condition（NOT）：取反
// :124 ConditionOperand：求值左操作数 → 按 Compare/Between/Like/In 处理右侧
```
`★ 解释器的两个讲究 ───────────────────────────`
- **短路求值**：OR 遇真即返回、AND 遇假即返回——和编程语言的布尔短路一样，省去不必要的子树遍历。
- **"单子节点结果不必是 bool"**（`:56` 注释）：`SELECT price * 2` 里 `price * 2` 是个 Expression，
  但它只有一个 AndCondition 链、最终是数值不是布尔。引擎检测"只有一条链"时直接返回原值，不强转
  bool——这让**同一套 Expression 文法既能当 WHERE 条件（强转 bool）又能当 SELECT 投影（任意类型）**。
  WHERE 子句外层会 `ToBool` 强制布尔；SELECT 投影则保留原类型。一套 AST 两种用途。
- **`tableAlias` 透传**：`FROM S3Object AS s` 的别名 `s` 一路传到 JSONPath 求值，用于剥离路径前缀。
`──────────────────────────────────────────`

比较/区间/模糊匹配都归约成基础操作：
```go
// :141 BETWEEN → 两次 compareOp（start <= arg <= end）
// :184 LIKE → inferTypeAsString + 通配匹配（带 ESCAPE）
// :258 IN  → 对列表/JSONPath 逐个 compareOp 判成员
```

---

## 4. 动态类型系统：`Value` 与"CSV 字节延迟推断"

```go
// value.go:48
type Value struct { value interface{} }   // 只存一个受限类型
// 受限类型：nil(NULL) / bool / string / int64 / float64 / time.Time(TIMESTAMP) /
//          []byte(BYTES,未定型) / []Value(ARRAY) / Missing
```
`★ 关键：BYTES = 未定型，CSV 的灵魂 ───────────────`
- **CSV 没有类型**：`"42"`、`"3.14"`、`"hello"` 在 CSV 里都是文本。S3 Select 把 CSV 字段读成
  `[]byte`（`BYTES` 类型，**未定型**），**直到某个操作需要类型才推断**。
- **算术时的延迟推断**（`value.go:652`）：
  ```go
  func inferTypeForArithOp(a *Value) error {
      if _, ok := a.ToBytes(); !ok { return nil }   // 已定型，跳过
      if i, ok := a.bytesToInt(); ok { a.setInt(i); return nil }     // ★ 先试 int（保精度）
      if f, ok := a.bytesToFloat(); ok { a.setFloat(f); return nil } // 再试 float
      return errCannotConvert                                        // 都不行 → 报错
  }
  ```
  `price + 1` 里，CSV 的 `price`（字节 `"42"`）在加法时被推断：先试 int 成功 → 当 int64 算。
- **算术优先整数**（`value.go:615` arithOp）：两个操作数都能转 int 就走 `intArithOp`（精确），
  否则退化 float。`SELECT count * price` 全是整数时不丢精度。
- **`Missing` vs `NULL` 区分**：JSON 里"键不存在"是 `Missing`，"键存在但值为 null"是 `NULL`——
  SQL 语义上两者不同（IS NULL 行为、聚合是否计入）。CSV 没有这个区分，JSON 才有。
`──────────────────────────────────────────`

---

## 5. JSONPath：列取值 vs 路径下钻

`JSONPath.evalNode`（`evaluate.go:388`）对不同记录格式走不同取值：
```go
func (e *JSONPath) evalNode(r Record, tableAlias string) (*Value, error) {
    pathExpr := e.StripTableAlias(alias)   // 剥掉 FROM 别名前缀
    _, rawVal := r.Raw()
    switch rowVal := rawVal.(type) {
    case jstream.KVS, simdjson.Object:                    // ★ JSON 记录：沿路径下钻
        result, _, _ := jsonpathEval(pathExpr, rowVal)    // 走 s.address.city 这种路径
        return jsonToValue(result)
    default:                                              // ★ CSV/Parquet：按列名/位置取
        return r.Get(pathExpr[len(pathExpr)-1].Key.keyString())
    }
}
```
`★ 细节 ────────────────────────────────────`
- **JSON 沿路径下钻**：`s.address.city`、`s.items[2]`、`s.tags[*]` 这类嵌套/数组访问，由
  `jsonpath.go` 的 `jsonpathEval` 递归走 `JSONPathElement`（Key / Index / `.*` / `[*]` 通配）。
- **CSV/Parquet 是扁平的**：没有嵌套，`JSONPath` 退化成"取最后一段当列名/位置"（`r.Get`）。CSV
  支持按列名（有 header）或按 `_1`/`_2` 位置取列。
- **`jsonToValue`（`:416`）转换原生 JSON → Value**：标量直接转；**嵌套对象 re-marshal 成 BYTES**
  （保留为 JSON 文本，按需再解析）；数组转 `[]Value`；`nil`→NULL、`Missing`→MISSING。这统一了
  JSON 的动态结构到引擎的 Value 类型系统。
- **`Record` 接口**（`record.go`）统一 CSV/JSON/Parquet：`Get(key)` 取字段、`Set` 设字段、
  `Raw()` 拿底层表示、`WriteCSV`/`WriteJSON` 输出。求值器只依赖这个接口，不关心底层格式——又一处
  接口抽象（呼应深读 20 的 recordReader、深读全系列的"面向接口"）。
`──────────────────────────────────────────`

---

## 6. 函数与聚合：两条不同的求值路径

```go
// evaluate.go:484
func (e *FuncExpr) evalNode(r Record, tableAlias string) (*Value, error) {
    switch e.getFunctionName() {
    case aggFnCount, aggFnAvg, aggFnMax, aggFnMin, aggFnSum:
        return e.getAggregate()          // ★ 聚合函数：取累积状态
    default:
        return e.evalSQLFnNode(r, tableAlias)   // ★ 普通函数：对本行求值
    }
}
```
`★ 聚合 vs 标量函数的本质区别 ───────────────────`
- **标量函数（`funceval.go` / `stringfuncs.go` / `timestampfuncs.go`）**：`UPPER(s)`、`SUBSTRING`、
  `CAST(x AS INT)`、`DATE_ADD`……对**当前这一行**求值，立即出结果。每行独立。
- **聚合函数（`aggregation.go`）**：`COUNT(*)`、`SUM(x)`、`AVG`、`MIN`、`MAX`——**状态存在 AST 节点
  自身里**，跨行累积。深读 20 §4 的求值主循环对聚合查询调 `AggregateRow(record)`：每行让聚合函数
  把值累加进自己的内部状态（如 SUM 累加、COUNT 自增）。直到 EOF 才 `AggregateResult` / `getAggregate`
  把累积状态取出来当最终结果。
- **这解释了深读 20 的"聚合 EOF 才出结果"**：聚合函数节点是有状态的累加器，必须看完所有行才有答案。
  非聚合函数是无状态的逐行变换。**同一棵 AST，聚合节点有记忆、标量节点无记忆**——这是 SQL 聚合
  语义在树遍历解释器里的实现方式。
- `analysis.go` 在解析后做静态分析，判定一条查询是否是聚合查询（`IsAggregated`），决定主循环走哪条路。
`──────────────────────────────────────────`

---

## 7. 一条查询的完整生命周期（串起来）

以 `SELECT s.city, COUNT(*) FROM S3Object[*] AS s WHERE s.age > 30 GROUP...`（概念示意）为例：
```
1. 解析（participle）  → Select AST：投影[s.city, COUNT(*)]、FROM、WHERE(s.age > 30)
2. 分析（analysis）    → IsAggregated 判定、列引用收集
3. 主循环（深读 20）逐条记录：
   ├─ recordReader.Read() 取一行（CSV 字节 / JSON KVS）
   ├─ EvalFrom 展开 FROM（S3Object[*] 把数组展成多行）
   ├─ WHERE: Expression.evalNode → s.age(JSONPath 取值/CSV 字节) compareOp 30
   │         （CSV 的 age 字节延迟推断为 int 再比较）→ ToBool
   ├─ 过滤通过 → 聚合查询：COUNT 聚合节点 AggregateRow(+1)
   │             非聚合查询：Eval 投影 s.city → outputRecord
   └─ ...
4. EOF → 聚合查询 AggregateResult 取出 COUNT → 输出
       → messageWriter 分帧事件流回传（深读 20 §6）
```

---

## 8. 一页纸总结 SQL 引擎的"硬核点"

| # | 细节 | 为什么重要 |
|---|------|-----------|
| 1 | participle 声明式文法（tag 即产生式） | 文法即强类型 AST，无需手写解析器 |
| 2 | 只支持 SELECT，文法即边界 | S3 Select 窄子集在文法里就限定 |
| 3 | Capture 钩子解析期清洗字面量 | 去引号/转义在解析阶段完成 |
| 4 | 运算符优先级 = 文法嵌套深度 | OR 最外、原子最内，嵌套编码优先级 |
| 5 | 每节点 evalNode，树遍历解释器 | 经典 tree-walking interpreter |
| 6 | OR/AND 短路求值 | 省不必要子树遍历 |
| 7 | 单子节点不强转 bool | 一套 Expression 既当 WHERE 又当 SELECT 投影 |
| 8 | Value = 受限 interface{}（含 BYTES 未定型） | 统一标量/数组/NULL/MISSING |
| 9 | CSV 字节延迟类型推断（int 优先→float） | 处理无 schema 的 CSV |
| 10 | 算术优先整数运算 | 不丢精度 |
| 11 | Missing vs NULL 区分 | JSON 缺键 vs 空值的 SQL 语义 |
| 12 | JSONPath：JSON 路径下钻 vs CSV 列取 | 同一 JSONPath 节点适配嵌套与扁平 |
| 13 | jsonToValue：嵌套对象 re-marshal 为 BYTES | 动态 JSON 结构归约到 Value 系统 |
| 14 | Record 接口统一 CSV/JSON/Parquet | 求值器不关心底层格式 |
| 15 | 聚合节点有状态累加、标量节点无状态 | 聚合"EOF 出结果"的实现根源 |

---

至此，S3 Select 从"SQL 下推的整体流程"（深读 20）一直钻到"SQL 引擎如何解析、如何对每条记录
求值、如何处理无类型 CSV 与嵌套 JSON、如何实现聚合"（本篇）。这是整个文档系列**下钻最深的一篇**——
从一个 HTTP handler 一路追到了字节级的类型推断和树遍历求值。

用同样的方式，你可以对任何还想深究的子系统再下钻一层（比如把深读 06 的 grid 钻到 WebSocket 帧
编解码、把深读 03 的 msgp 钻到 `*_gen.go` 的字节布局）。**方法不变：追调用链、读数据结构、问设计
意图、找不变量。**
