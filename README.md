# data-quality-monitor-go

一套**本地数据质量校验与监控**流程：用本地 CSV / JSONL 文件模拟多个来源数据集，
基于声明式规则执行完整性、唯一性、取值一致性与新鲜度校验，并提供可增量、可复现、
并发可控的批次运行能力。

- 纯 Go 标准库实现，无第三方依赖（Go 1.25）
- 失败原因全部归类为稳定的 `ReasonCode`，底层错误不透出
- 批次状态持久化：重复执行幂等、迟到批次正常接入、历史结论不可变
- **历史结论有效性判断**：重跑时比对数据文件与规则声明的输入清单，数据侧/规则侧漂移可区分
- 规则并发执行、信号量限流、单规则超时/取消/panic 隔离，结果顺序稳定
- 告警按 `(批次, 规则)` 精确去重，崩溃恢复后不重复、不漏报，失败自动补投并暴露补发/未决计数
- **并发单飞**：同批重复触发只有一个真正执行，其他触发拿到同一份结果或明确的“正在跑”
- 新鲜度判定时间可注入，相同输入与判定时间结论完全一致、可复现

## 目录结构

```
cmd/dqmonitor          CLI 入口（run / replay / history）
internal/model         核心领域模型与稳定的失败原因码（ReasonCode）
internal/config        声明式配置加载与校验
internal/datasource    CSV / JSONL 加载、字段级校验、形态漂移检测
internal/manifest      批次输入清单（数据文件/规则声明指纹）与历史有效性比对
internal/rule          四类规则检查器（completeness/uniqueness/consistency/freshness）
internal/state         批次状态存储（租约心跳、在途检测、迟到接入、历史结论）
internal/alert         告警归一化、精确一次投递、崩溃恢复与失败补投
internal/engine        并发编排、资源限流、超时/取消/panic 隔离、单飞与指纹
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

# 退出码：0 全部通过；1 存在 failed/errored 规则；2 用法、状态流转或历史结论冲突
# （如历史结论已失效 data/rules、批次正在跑、对已提交批次使用 --force）

# 2. 重复执行同一批次 -> 输入未变则幂等重放，结论/指纹不变，不重复告警
go run ./cmd/dqmonitor run --config examples/config.json \
  --batch-id 20260923-001 --state-dir .dq-state --eval-time 2026-09-23T10:00:00Z

# 3. 迟到批次：先跑较新的 003，再补跑更老但未处理的 001 -> 正常出结论（不再被拒收）
go run ./cmd/dqmonitor run --config examples/config.json \
  --batch-id 20260923-003 --state-dir .dq-state --eval-time 2026-09-23T10:00:00Z
go run ./cmd/dqmonitor run --config examples/config.json \
  --batch-id 20260923-001 --state-dir .dq-state --eval-time 2026-09-23T10:00:00Z

# 4. 改规则参数 / 换数据文件 / 删除数据文件后重跑同一批次
#    -> 退出码 2，明确说明历史结论不适用，以及是 data 侧还是 rules 侧变了（结论不被覆盖）
go run ./cmd/dqmonitor run --config examples/config-modified.json \
  --batch-id 20260923-001 --state-dir .dq-state --eval-time 2026-09-23T10:00:00Z

# 5. 中途失败后重跑：默认拒绝从中间继续，需显式回收
go run ./cmd/dqmonitor run --config examples/config.json \
  --batch-id 20260923-002 --state-dir .dq-state --force

# 6. 通知通道失败后重跑同一批次 -> 自动补投；结果里看 alerts_sent/retried/pending
go run ./cmd/dqmonitor run --config examples/config.json \
  --batch-id 20260923-001 --state-dir .dq-state

# 7. 按批次复现历史结论 / 列出所有批次
go run ./cmd/dqmonitor replay  --batch-id 20260923-001 --state-dir .dq-state
go run ./cmd/dqmonitor history --state-dir .dq-state

# 8. 测试（含竞态检测）
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

## 批次状态与并发

批次状态保存在 `--state-dir`（原子写 `tmp + rename`，跨进程在短临界区内 `flock`）：

```
(不存在) ──Begin──▶ running(携带租约 token + 心跳) ──Commit(token)──▶ committed
                         │ 进程中断
                         └─ 留在 running（租约心跳过期）── 仅允许 --force 回收重跑
```

- **迟到批次**：批次号按字典序代表先后（建议用时间戳/序列号）。比最新已提交批次**更老、但从未处理过**的迟到批次会被正常接收、正常校验、正常出结论和通知；高水位只用于历史排序，不再拒收旧批次。
- **同批重复触发（并发单飞）**：
  - 同一进程内多个触发只会有一次真正执行，其余触发**共享并拿到同一份结果**；
  - 跨进程/多实例时，另一个触发若撞上仍在执行的批次会得到明确的 `ErrBatchRunning`（batch is already being executed），而不是启动第二份评估；
  - 已提交批次的重复触发走历史重放。
  因此同一批次永远不会出现两份结论、两份通知。
- **租约与心跳**：执行期间按 TTL 的 1/3 周期续约（默认 TTL 30s）。心跳存活 = 正在跑（`ErrBatchRunning`）；心跳超过 TTL 未续 = 判定为崩溃中断（`ErrStaleBatch`），必须 `--force` 才能丢弃中间状态重新开始。
- **历史结论不可变**：已提交的结论永不被覆盖。`--force` 只能回收“中断在 running”的批次；对已提交批次使用 `--force` 会得到 `ErrImmutable`。

### 重复执行时的历史结论有效性判断

每个批次提交时都会保存一份**输入清单（manifest）**：每条数据集的文件路径/格式/schema 声明、文件内容的 SHA256、可读性，以及全部规则声明（id/kind/字段/参数/级别）与注入的判定时间。

重跑已提交批次时，引擎用当前输入重建清单并与存档逐条比对：

- **完全一致** → 照旧复现历史结论（`status=replay`），不重新校验。
- **对不上** → 返回类型化的 `model.HistoryInvalidError`，**不改写任何历史结论**，退出码 2。错误里同时说明两件事，杜绝含混拒绝：
  1. 该批次**已有结论**、本次没有覆盖；
  2. 具体是哪一侧变了、变了什么（`Sides` 与逐条 `Changes`）。

变化侧归类：

| 情况 | 归类 | 示例说明 |
|------|------|----------|
| 数据集文件内容变化 | `data` | `dataset users file content changed (size 213->225)` |
| 数据源文件被删除/不可读 | `data` | `dataset users source file is no longer readable: file missing` |
| 规则参数/定义变化 | `rules` | `rule declarations (...) differ from the original run` |
| 数据集路径/格式/schema 声明变化 | `rules` | `source path/format changed`、`schema declaration changed` |
| 判定时间 `--eval-time` 不同 | `rules` | `evaluation time differs: original=... current=...` |

同一次重跑若两侧都变，会同时给出 `data` 与 `rules`（以及全部差异条目）。内容哈希只取身份字段，重写一个内容完全相同的文件（mtime 变化）不会导致历史结论失效。

## 告警精确一次与失败补投

已投递的 `(batch_id, rule_id)` 记录在 append-only 日志 `alert-journal.jsonl`，包含两类落盘事实：

- `attempt`：开始投递前先落盘（投递前/投递中崩溃也会留下）；
- `delivered`：sink 接收成功并 `fsync` 后才落盘。

规则：

- **成功才记 delivered**：崩溃后重跑不会重复通知，也不会漏掉尚未 delivered 的规则。
- **失败自动补投**：通知通道抽风时，该批次把未送达的留为 backlog；之后**重跑同一批次（即使只是 replay）**会自动补齐。
- **对外计数可见**：每次结果都带
  - `alerts_sent`：本次实际送达几条（含补发）；
  - `alerts_retried`：其中有几条是补的历史欠账；
  - `alerts_pending`：本次之后仍有几条没送出去。
  日志同时打印 `alert dispatch` 依据。
- **崩溃窗口去重**：实现了 `DeliveryChecker` 的 sink（内置 `FileSink` 即实现）会在投递前回查外部是否已送达，堵住“Send 成功但 delivered 落盘前崩溃”的重复窗口。`FileSink` 每次写入都 `fsync`，并在下次操作时修复崩溃留下的半行。
- 旧版本无 `kind` 字段的 journal 行会被当作 `delivered` 兼容读取。

> 说明：对纯外部、不可回查的 sink，通用两阶段日志保证“崩溃前未确认送达的会重投、已确认的绝不重投”；要严格消除上述崩溃窗口，sink 需实现 `DeliveryChecker`（本地 `FileSink` 已满足）。

### 指纹与可复现性

`fingerprint = SHA256(batch_id + evalTime + 有序结论)`。相同批次、相同判定
时间、相同输入产出相同指纹；新鲜度等时间相关规则通过 `--eval-time` 注入，
保证测试与历史复现不受真实时钟影响。指纹用于结论复现；**输入是否仍有效**
则由上面的 manifest 清单独立判断（结论指纹相同不等于输入没变）。

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

## 兼容范围

- **旧状态目录可直接打开**：没有 manifest 的已提交批次（旧版本产生）重跑时会记录一条 warning，按历史结论复现（无法做有效性校验）；新提交的批次都会写入 manifest。
- **旧告警日志可直接读取**：`alert-journal.jsonl` 中无 `kind` 字段的旧条目按 `delivered` 处理，不会重发。
- **行为变更（相对早期版本）**：更早批次不再因“高水位回退”被拒绝——从未处理的迟到批次现在正常接入；只有“已有结论想覆盖/改写”才会被拒绝（`HistoryInvalidError` / `ErrImmutable`）。原来的 `ErrRollback` 已移除。
- 状态文件与告警日志仍是原子写、append-only JSONL，Go 版本要求不变（1.25，纯标准库）。

## 日志

运行时使用 `log/slog` 打印输入摘要与每条判定依据：

- `input summary`：数据集名、格式、声明字段数、记录数、形态问题数、加载耗时；
- `rule verdict`：规则、数据集、类型、状态、finding 编码、证据条数、耗时；
- `historical conclusion invalidated`：历史结论失效时，逐条给出 `side`（data/rules）与 `change`；
- 批次提交/重放与 `alert dispatch`：含 `alerts_sent` / `alerts_retried` / `alerts_pending`，可看出本次补发几条、还欠几条。

## 示例数据说明

`examples/data/users.csv` 与 `examples/data/events.jsonl` 内置了重复主键、
域外形如 `banned` 的状态、非数字年龄、空年龄、null `user_id`、过期事件等，
配合 `examples/config.json` 可一次性观察到 `duplicate_value`、
`value_outside_set`、`type_mismatch`、`null_value`、`stale_record` 等各类
可区分结论。
