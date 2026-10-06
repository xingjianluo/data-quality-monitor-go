# data-quality-monitor-go

一套**本地数据质量校验与监控**流程：用本地 CSV / JSONL 文件模拟多个来源数据集，
基于声明式规则执行完整性、唯一性、取值一致性与新鲜度校验，并提供可增量、可复现、
并发可控的批次运行能力。

- 纯 Go 标准库实现，无第三方依赖（Go 1.25）
- 失败原因全部归类为稳定的 `ReasonCode`，底层错误不透出
- 批次状态持久化：重复执行幂等重放、迟到批次（乱序到达）一律接受、崩溃后必须显式 `--force` 回收
- 同一批次跨进程只执行一次：并发触发共享同一份结论与同一份通知
- 按批次加锁：不同批次并行、只读查询（replay/history）不被运行中的批次阻塞
- 规则并发执行、信号量限流、单规则超时/取消/panic 隔离，结果顺序稳定
- 告警按 `(批次, 规则)` 精确一次：投递记录与日志双轨核对，进程在投递前后任意点
  被中断，重启后不重复、不漏发，未送达的自动补投
- 新鲜度判定时间可注入，相同输入与判定时间结论完全一致、可复现

## 目录结构

```
cmd/dqmonitor          CLI 入口（run / replay / history）
internal/model         核心领域模型与稳定的失败原因码（ReasonCode）
internal/config        声明式配置加载与校验
internal/datasource    CSV / JSONL 加载、字段级校验、形态漂移检测
internal/rule          四类规则检查器（completeness/uniqueness/consistency/freshness）
internal/state         批次状态存储（按批次租约锁、历史结论、输入快照复核）
internal/alert         告警归一化、精确一次投递、崩溃恢复与跨进程去重
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
# （如历史结论被强制覆盖、存在未回收的中断批次）

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

批次状态保存在 `--state-dir`（原子写 `tmp + rename`；互斥按批次粒度：
每个批次一把 `flock` 锁文件 + 进程内 keyed mutex，没有全局锁）：

```
(不存在) ──Begin──▶ running(携带租约 token) ──Commit(token)──▶ committed
                         │ 崩溃/失败
                         └─ 留在 running（租约清空/过期）── 仅允许 --force 回收重跑
```

- **迟到批次（乱序到达）**：批次 ID 只标识批次本身，不参与任何顺序约束。
  一个从未处理过的批次——无论其 ID 比已提交批次新还是旧——都会被接受并
  独立评估、独立通知，不会顶替或改写任何已有批次的历史。
- **重复执行（幂等重放）**：已提交批次再次触发不会重新评估，而是复核输入
  快照后返回历史结论（`status=replay`），指纹不变、不重复告警。
- **历史结论何时仍有效**：提交时会记录输入快照（判定时间、规则集哈希、
  每个数据文件的内容/形态哈希与记录数）。重放时重新采样比对，完全一致才
  返回历史结论；任何差异都返回 `DriftError` 并逐条说明差异来自数据侧
  （内容变化、形态变化、源文件消失/恢复）还是规则侧（规则声明、数据集
  声明、判定时间变化），绝不把过期结论当作当前结论。
- **并发触发同一批次**：进程内触发共享同一次执行的结果；跨进程触发由
   durable 租约仲裁——一个进程执行，另一个轮询等待其提交后返回同一份
  结论（`status=replay`）。任何时刻同一批次只有一份结论、一套通知。
- **不同批次与只读查询互不阻塞**：锁是按批次的，一个批次还在跑时，其他
  进程可以正常执行别的批次；`replay`/`history`（Get/List）完全无锁
  （批次文件原子替换，读到的始终是完整快照），不会被运行中的批次挡住。
- **中途失败重跑**：崩溃留下的 `running` 批次默认返回 `ErrStaleBatch`；
  必须 `--force` 才会丢弃中断的中间状态并签发新租约重新开始。提交时校验
  租约 token，token 不匹配返回 `ErrLease`；对已提交批次使用 `--force`
  返回 `ErrAlreadyConcluded`，历史结论永不被覆盖。
- **告警精确一次**：`(batch_id, rule_id)` 的投递同时由两道记录保证——
  append-only 日志 `alert-journal.jsonl`（投递成功后落日志并 fsync），
  以及 sink 自身的投递记录（`FileSink` 的 alerts 文件可回读，单条写入 +
  fsync）。因此进程在投递前、投递途中、投递完成后任意点被中断：已送出的
  通知重启后都能被识别、绝不重发；未送出的在后续任意一次触发中自动补投。
  整个"检查-投递-落日志"循环持有跨进程 `flock`，两个进程同时补投同一
  批次也会串行，不会各自发一遍。被崩溃撕裂的尾部半行不算投递，会补发一
  条完整行。
- **补投可见性**：每次运行的 `alert_report` 明确给出 `expected`（应发）、
  `already_delivered`（此前已送达）、`sent_this_run`（本次补发）、
  `pending`（仍未送达），恒满足
  `expected = already_delivered + sent_this_run + pending`。

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

批次收尾另有 `batch committed`（指纹、通过/失败/错误计数、本次告警发送与
待补投条数）；重放时有 `replaying committed batch` 与
`alert backlog reconciliation`（本次补投几条、还剩几条）；历史结论被拒绝时
`historical conclusion rejected` 逐条给出数据侧/规则侧差异。

## 并发与崩溃演练（本地复现）

```bash
# 两个进程几乎同时触发同一批次：一个执行，另一个等待后拿到同一份结论；
# alerts.jsonl 中每个 (批次, 规则) 恰好一行
go build -o /tmp/dqm ./cmd/dqmonitor
/tmp/dqm run --config examples/config.json --batch-id xp-1 --state-dir .dq-state \
  --eval-time 2026-09-23T10:00:00Z &
/tmp/dqm run --config examples/config.json --batch-id xp-1 --state-dir .dq-state \
  --eval-time 2026-09-23T10:00:00Z &
wait

# 一个批次在跑时，别的批次与只读查询照常工作
/tmp/dqm run --config examples/config.json --batch-id xp-2 --state-dir .dq-state \
  --eval-time 2026-09-23T10:00:00Z &
/tmp/dqm history --state-dir .dq-state   # 不被阻塞

# 模拟"通知已发出但进程在收尾前被中断"：删掉投递日志（等价于崩溃丢日志），
# 重启再跑同一批次 -> 已送达的不重发，未送达的自动补投，
# 输出 JSON 的 alert_report 显示 sent_this_run / pending
rm -f .dq-state/alert-journal.jsonl
/tmp/dqm run --config examples/config.json --batch-id xp-1 --state-dir .dq-state \
  --eval-time 2026-09-23T10:00:00Z

# 自动化验证（含竞态检测；跨进程并发、崩溃恢复、失败重试组合场景）：
go test ./... -race -count=1
go test ./internal/e2e/ -race -run 'TestCrossProcess' -v
```

## 兼容范围

- **平台**：Linux/macOS 提供完整的跨进程 `flock` 语义；Windows 下文件锁为
  no-op，仅保证进程内互斥（本地单进程演练不受影响）。
- **状态目录**：`batches/<id>.json`（批次状态与历史结论）、
  `batches/<id>.lock`（按批次锁文件，可安全忽略）、`alert-journal.jsonl`
  （投递日志）、`alerts.jsonl`（通知本体，同时充当投递记录）。旧版本遗留的
  全局 `lock` 文件不再使用，残留无害。
- **行为兼容**：已提交批次的历史结论永不被覆盖；`--force` 仅能回收中断的
  `running` 批次，对 committed 批次返回 `ErrAlreadyConcluded`；重放只在输入
  快照完全一致时返回历史结论，否则报 `DriftError`。

## 示例数据说明

`examples/data/users.csv` 与 `examples/data/events.jsonl` 内置了重复主键、
域外形如 `banned` 的状态、非数字年龄、空年龄、null `user_id`、过期事件等，
配合 `examples/config.json` 可一次性观察到 `duplicate_value`、
`value_outside_set`、`type_mismatch`、`null_value`、`stale_record` 等各类
可区分结论。
