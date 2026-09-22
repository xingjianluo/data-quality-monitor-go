# data-quality-monitor-go

一套**本地数据质量校验与监控**流程：用本地 CSV / JSONL 文件模拟多个来源数据集，
基于声明式规则执行完整性、唯一性、取值一致性与新鲜度校验，并提供可增量、可复现、
并发可控的批次运行能力。

- 纯 Go 标准库实现，无第三方依赖（Go 1.25）
- 失败原因全部归类为稳定的 `ReasonCode`，底层错误不透出
- 批次状态持久化：重复执行幂等、乱序回退被拒绝、崩溃后必须显式 `--force` 回收
- 规则并发执行、信号量限流、单规则超时/取消/panic 隔离，结果顺序稳定
- 告警按 `(批次, 规则)` 精确去重，崩溃恢复后不重复、不漏报
- 新鲜度判定时间可注入，相同输入与判定时间结论完全一致、可复现

## 目录结构

```
cmd/dqmonitor          CLI 入口（run / replay / history）
internal/model         核心领域模型与稳定的失败原因码（ReasonCode）
internal/config        声明式配置加载与校验
internal/datasource    CSV / JSONL 加载、字段级校验、形态漂移检测
internal/rule          四类规则检查器（completeness/uniqueness/consistency/freshness）
internal/state         批次状态存储（租约、高水位、回退拒绝、历史结论）
internal/alert         告警归一化、精确一次投递与崩溃恢复
internal/engine        并发编排、资源限流、超时/取消/panic 隔离、指纹
examples/              示例数据与声明配置
```

## 快速开始（本地验证）

```bash
# 1. 运行一个批次（判定时间显式注入，保证可复现）
go run ./cmd/dqmonitor run \
  --config examples/config.json \
  --batch-id 20260923-001 \
  --state-dir .dq-state \
  --eval-time 2026-09-23T10:00:00Z

# 退出码：0 全部通过；1 存在 failed/errored 规则；2 用法或状态流转错误
# （如回退被拒绝、存在未回收的中断批次）

# 2. 重复执行同一批次 -> 幂等重放，结论/指纹不变，不重复告警
go run ./cmd/dqmonitor run --config examples/config.json \
  --batch-id 20260923-001 --state-dir .dq-state --eval-time 2026-09-23T10:00:00Z

# 3. 中途失败后重跑：默认拒绝从中间继续，需显式回收
go run ./cmd/dqmonitor run --config examples/config.json \
  --batch-id 20260923-002 --state-dir .dq-state --force

# 4. 按批次复现历史结论 / 列出所有批次
go run ./cmd/dqmonitor replay  --batch-id 20260923-001 --state-dir .dq-state
go run ./cmd/dqmonitor history --state-dir .dq-state

# 5. 测试（含竞态检测）
go test ./... -race -count=1
```

## 规则模型

规则在 `config.json` 中声明，每条规则绑定一个数据集与一个规则族：

| kind          | 含义       | 关键参数 |
|---------------|------------|----------|
| `completeness` | 完整性：目标字段存在且非空 | `min_fill_rate`（0~1，默认 1.0） |
| `uniqueness`   | 唯一性：单字段或字段组合在全表不重复 | `ignore_nulls`（默认 false） |
| `consistency`  | 取值一致性 | `mode=allowed` + `allowed[]`；`mode=range` + `min`/`max`；`mode=regex` + `pattern` |
| `freshness`    | 新鲜度：时间字段相对批次判定时间不超过阈值 | `max_age_seconds` |

字段类型：`string` / `int` / `float` / `bool` / `time`（`time` 必须声明 `format`，
如 `2006-01-02`）。`required: true` 表示字段结构性必填。

规则结果三态：`passed` / `failed`（数据违规，含 findings）/ `errored`（基础设施或
声明问题，含归一化错误码）。告警严重级别为 `error` / `warning`。

### 可区分的失败原因（ReasonCode）

系统绝不静默通过，所有异常都归入稳定编码：

**数据级（findings）**

- `missing_field` — 必填字段在行中缺失
- `null_value` — 字段存在但为 null（CSV 空单元格 / JSON `null` / 可选字段缺失）
- `type_mismatch` — 值无法转换为声明类型（保留原值作为证据，不强制转换）
- `duplicate_value` — 唯一键重复（证据列出所有冲突行号）
- `value_outside_set` / `value_out_of_range` / `regex_mismatch` — 一致性违规
- `stale_record` — 时间戳相对注入的判定时间超过最大年龄

**数据源/运行级（RuleResult.error_code）**

- `schema_field_missing` — 声明字段在数据源中整体消失（结构变化）
- `source_shape_changed` — 数据源出现未声明字段 / 列数错配（形态漂移）
- `source_load_failure` — 文件不可读或无法解析
- `rule_definition_invalid` — 规则声明非法（未知 kind、参数错误等）
- `rule_timeout` / `rule_cancelled` / `rule_panic` — 单规则运行异常，已归一化

> 设计原则：缺失与空值是两种不同结论（必填缺失 = `missing_field`，
> 存在但为 null = `null_value`）；类型不匹配的值不参与值域/唯一性比较，而是
> 单独以 `type_mismatch` 呈现。数据源形态变化时该数据集上的规则全部
> `errored` 并携带每个形态类别的 finding，绝不继续按旧形态“跑通”。

## 状态与兼容规则

批次状态保存在 `--state-dir`（原子写 `tmp + rename`，跨进程 `flock`）：

```
(不存在) ──Begin──▶ running(携带租约 token) ──Commit(token)──▶ committed
                         │ 崩溃/失败
                         └─ 留在 running（租约清空）── 仅允许 --force 回收重跑
```

- **重复执行（幂等）**：已提交批次再次 `Begin` 直接返回存储的历史结论
  （`status=replay`），不重新校验、不重复告警；历史结论可按批次随时复现。
- **乱序到达**：批次 ID 按字典序代表先后（建议使用时间戳/序列号，如
  `20260923-001`）。高水位（最新已提交批次）之后才允许提交；提交更老批次
  返回 `state rollback rejected`，**状态回退被明确拒绝**，不会从中间继续。
- **中途失败重跑**：崩溃留下的 `running` 批次默认返回 `ErrStaleBatch`；
  必须 `--force` 才会丢弃中断的中间状态并签发新租约重新开始。提交时校验
  租约 token，token 不匹配返回 `ErrLease`。
- **告警精确一次**：已投递的 `(batch_id, rule_id)` 记录在 append-only 日志
  `alert-journal.jsonl`；投递成功才落日志，因此崩溃后重跑不会重复通知，
  也不会漏掉尚未落日志的规则。

### 指纹与可复现性

`fingerprint = SHA256(batch_id + evalTime + 有序结论)`。相同批次、相同判定
时间、相同输入产出相同指纹；新鲜度等时间相关规则通过 `--eval-time` 注入，
保证测试与历史复现不受真实时钟影响。

## 并发与资源控制

- 数据集只加载一次，其规则共享加载结果；
- 规则在独立 goroutine 中执行，由带缓冲信号量限制并发数
  （默认 `MaxConcurrentRules=4`）；
- 每条规则拥有独立 `context.WithTimeout`（默认 10s）：单规则超时
  （`rule_timeout`）、批次取消（`rule_cancelled`）或 panic（`rule_panic`）
  只影响自身，其余规则照常判定；
- 结果始终按 `rule_id` 稳定排序输出，finding/evidence 内部同样确定序；
- 加载失败、形态漂移、超时、panic 全部归一化进 `RuleResult`，不向外部透出
  底层 error 文本。

## 日志

运行时使用 `log/slog` 打印两类依据：

- `input summary`：数据集名、格式、声明字段数、记录数、形态问题数、加载耗时；
- `rule verdict`：规则、数据集、类型、状态、finding 编码、证据条数、耗时。

## 示例数据说明

`examples/data/users.csv` 与 `examples/data/events.jsonl` 内置了重复主键、
域外形如 `banned` 的状态、非数字年龄、空年龄、null `user_id`、过期事件等，
配合 `examples/config.json` 可一次性观察到 `duplicate_value`、
`value_outside_set`、`type_mismatch`、`null_value`、`stale_record` 等各类
可区分结论。
