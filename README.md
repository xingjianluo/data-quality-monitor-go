# 本地数据质量校验与监控（Go）

一套完全本地运行的数据质量校验与监控流程：用本地 JSON 数据文件模拟多来源数据集，
对多个数据集并发执行声明式校验，结论**一致、可解释、可复现**，并以本地文件留存批次状态、
以结构化日志投递告警。

无任何第三方依赖，仅使用 Go 标准库；支持 `-race` 竞态检测。

## 1. 规则模型

规则在计划文件（JSON）中**声明**，每条规则有稳定 `id`、可归类 `type`、目标 `dataset` 与参数。
非法声明在批次执行前被拒绝（`invalid_rule`），不会静默忽略。

| 类型 type       | 参数                        | 判定方式                                                                 |
|-----------------|-----------------------------|--------------------------------------------------------------------------|
| `completeness`  | `columns`(1)、`min_non_null` ∈ (0,1] | 非空（键存在且非显式 `null`）比例不得低于阈值，否则 `fail/rule_violation` |
| `uniqueness`    | `columns`(≥1)               | 列组合值不得重复；`null/缺失` 键不参与比较（交给完整性）                  |
| `consistency`   | `columns`(1)、`allowed`     | 非空取值必须落在允许集合内                                                |
| `freshness`     | `columns`(1)、`max_age`(如 `24h`) | 时间列（RFC3339）不得早于 `批次水位线 - max_age`，水位线随批次持久化      |

此外每个数据集自动附带一条**内置结构校验**（`structural`，不可由用户声明），先于数据规则执行：

- 字段缺失（`field_missing`）：声明列在全部记录中都不存在；
- 类型不匹配（`type_mismatch`）：取值与声明类型（`string/int/float/bool/time`）不符；
- 数据源形态变化（`schema_drift`）：数据中实际出现的列集合相对上一批次基线发生增删/改型；
- 数据源不可读（`source_unreadable`）：文件缺失或 JSON 无法解析。

结构问题一律判定为 **`error`**，与“数据违反规则”的 **`fail`** 明确区分；二者都**不会静默通过**。
结构前提不满足时（如列缺失），引用该列的数据规则直接返回 `error`，不产生可能误导的 pass/fail。

### 状态三态与归一化原因码

单条规则结果 `status ∈ {pass, fail, error}`；整批状态：任一 `error` → `error`，否则任一 `fail` → `fail`，否则 `pass`。
所有异常在对外结果中只出现归一化的 `reason`，不直接透出底层错误字符串（文件路径等上下文保留在 message 中）：

`rule_violation`、`field_missing`、`type_mismatch`、`schema_drift`、`source_unreadable`、
`rule_timeout`、`rule_cancelled`、`invalid_rule`、`input_incomplete`、
`state_rollback`、`state_conflict`、`internal`。

## 2. 状态、增量与兼容规则

状态保存在单个 JSON 文件（默认 `dqm-state.json`），采用 **临时文件 + fsync + 原子重命名** 写入。
文件带 `version`，不支持的版本会被明确拒绝，而不是按错误格式继续。

- **批次标识**：`batch_id`（业务标识）+ `ordinal`（单调批次序号）+ `watermark`（评估水位线）。
- **同批次重复执行（幂等）**：直接返回已提交的相同结论（`replayed=true`），不重新评估、不重复告警。
  重放要求序号、水位线、数据集声明结构、规则声明（含参数）与首次完全一致；任何一项不同即 `state_conflict`，
  明确拒绝，而不是“从中间继续”或给出新结论。
- **失败后重跑**：评估与告警解耦。提交成功后再投递告警，告警确认标记 `alerts_sent` 单独原子落盘；
  若投递阶段崩溃，重跑至多补投一次（at-least-once + 去重标记），常规重放不产生第二条告警。
- **乱序/迟到到达**：一个尚未被占用的 `ordinal` 即使小于当前最大序号也会被接受并持久化（不漏报），
  历史可按 `batch_id` 或 `ordinal` 复现。
- **状态回退明确拒绝**：用另一个批次抢占**已被占用**的 `ordinal` → `state_rollback`；
  相同 `batch_id` 但结论内容不同 → `state_conflict`。两种拒绝都不改变已有状态。
- **进程内并发**：同一 `batch_id` 的并发执行通过 singleflight 串行化，保证只评估/提交/告警一次；
  不同批次通过存储互斥与序号占用检查保证不互相覆盖。
- **历史复现**：新鲜度等时间相关判定使用批次自带的水位线而非“当前时间”，因此历史结论按批次复现时保持一致；
  `dqm replay` 仅从状态还原结果，不重新跑规则、不发告警。

## 3. 并发、资源与隔离

- 数据集加载与规则判定均使用可配置的并发度（`--concurrency`，默认 4）的信号量限流；
- 每条规则运行在独立的超时上下文内（`--rule-timeout`，默认 2s）；单条规则超时（`rule_timeout`）、
  被取消（`rule_cancelled`）或 panic（归一化为 `internal`）都只影响该规则，其余规则照常判定；
- 结论顺序稳定：结构结果按数据集声明顺序，用户规则按计划中的声明顺序；输入摘要按数据集名排序；
- 批次级前置错误（空计划、重名数据集、规则引用未声明数据集等）以 `input_incomplete/invalid_rule`
  直接拒绝，且**不写入状态**，可修正后重跑。

## 4. 本地验证方法

### 4.1 运行 CLI

```bash
go build ./...

# 执行一个批次（示例工作区在 testdata/workspace）
go run ./cmd/dqm run \
  --plan testdata/workspace/plan-b1.json \
  --state /tmp/dqm-state.json -v

# 退出码：0=整批通过；3=存在 fail/error 结论（结果 JSON 仍正常输出）；2=基础设施错误（状态拒绝/IO）

# 按批次 ID 或序号复现历史结论（不重新评估、不发告警）
go run ./cmd/dqm replay --state /tmp/dqm-state.json --batch 2026-09-23-b1
go run ./cmd/dqm replay --state /tmp/dqm-state.json --ordinal 1

# 列出已提交批次
go run ./cmd/dqm history --state /tmp/dqm-state.json
```

计划文件示例（`testdata/workspace/plan-b1.json`）：

```json
{
  "batch_id": "2026-09-23-b1",
  "ordinal": 1,
  "watermark": "2026-09-23T12:00:00Z",
  "datasets": [
    {"name": "orders", "path": "b1/orders.json", "columns": [
      {"name": "id", "type": "int"},
      {"name": "updated_at", "type": "time"}
    ]}
  ],
  "rules": [
    {"id": "orders-id-complete", "type": "completeness", "dataset": "orders",
     "columns": ["id"], "min_non_null": 1.0},
    {"id": "orders-id-unique", "type": "uniqueness", "dataset": "orders", "columns": ["id"]},
    {"id": "orders-status-enum", "type": "consistency", "dataset": "orders",
     "columns": ["status"], "allowed": ["paid", "pending", "refunded"]},
    {"id": "orders-fresh", "type": "freshness", "dataset": "orders",
     "columns": ["updated_at"], "max_age": "24h"}
  ]
}
```

数据文件为 JSON 对象数组；**键缺失与显式 `null` 可区分**：

```json
[
  {"id": 1, "updated_at": "2026-09-23T08:00:00Z"},
  {"id": null},
  {"updated_at": "2026-09-23T09:00:00Z"}
]
```

### 4.2 日志可解释性

每次运行在 stderr 打印结构化日志，包含输入摘要与判定依据，例如：

```
msg="batch run started" batch_id=... datasets=2 rules=6 watermark=...
msg="input summary"  dataset=orders rows=3 schema_fingerprint=int:id,...
msg="finding" rule_id=orders.structural status=error reason=type_mismatch column=user_id count=1 message=...
msg="alert"   rule_id=orders-fresh reason=rule_violation severity=error message=...
msg="batch committed" overall_status=error
```

### 4.3 测试（含竞态检测）

```bash
go test ./...                 # 全部测试
go test ./... -race           # 竞态检测下运行（CI 推荐）
go test ./... -cover          # 覆盖率
go vet ./...
```

测试覆盖的组合场景：

- 四类规则的 pass/fail，以及完整性、唯一性、一致性、新鲜度各自的归一化原因；
- 结构变化跨批次（`schema_drift` + `field_missing`）、类型不匹配、数据文件缺失，失败原因互不混淆；
- 同批次重复执行结论一致且不重复告警；同批次**并发**执行只评估/提交/告警一次；
- 乱序序号接受且可复现、抢占已占用序号被明确拒绝（`state_rollback`）、
  相同批次 ID 携带不同输入被拒绝（`state_conflict`）；
- 单规则超时/取消不影响其他规则，结果顺序稳定；FileStore 落盘后重开结论一致。

## 5. 代码结构

```
cmd/dqm/            CLI：run / replay / history
internal/model/     领域模型：数据形态、规则声明、结果、原因码
internal/plan/      批次计划（JSON）加载与路径/水位线解析
internal/source/    本地数据文件并发加载（缺失/null 可区分、失败不静默）
internal/rule/      四类规则判定器 + 内置结构校验 + 并发隔离执行
internal/state/     批次状态：内存/JSON 文件存储，幂等、回退拒绝、原子写
internal/monitor/   Finding -> Alert 归一化与投递（日志 sink / 内存 sink）
internal/engine/    批次编排：加载→结构校验→规则判定→提交→告警，含 singleflight
internal/errs/      归一化错误类型
internal/logx/      slog 结构化日志
testdata/workspace/ 本地示例数据与计划
```
