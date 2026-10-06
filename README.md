# data-quality-monitor-go

一套**本地数据质量校验与监控**流程：用本地 CSV / JSONL 文件模拟多个来源数据集，
基于声明式规则执行完整性、唯一性、取值一致性与新鲜度校验，并提供可增量、可复现、
并发可控的批次运行能力。

- 纯 Go 标准库实现，无第三方依赖（Go 1.25）
- 失败原因全部归类为稳定的 `ReasonCode`，底层错误不透出
- 批次状态持久化：重复执行幂等；历史结论不可覆盖；崩溃遗留的 running 批次必须显式 `--force` 回收
- **迟到批次一等公民**：从未处理过的批次无论 ID 新旧都正常受理，不存在"高水位回退拒绝"
- 规则并发执行、信号量限流、单规则超时/取消/panic 隔离，结果顺序稳定
- 告警按 `(批次, 规则)` 精确一次：投递意图（pending）先落盘并 fsync，崩溃在投递前/投递中/投递后都不重不漏；内置文件通道按 `DeliveryID` 幂等去重
- **同批次跨进程只执行一次**：一个进程执行，并发触发的其它进程等待并拿到同一份结论与同一份通知；**不同批次与只读查询互不阻塞**
- 新鲜度判定时间可注入，相同输入与判定时间结论完全一致、可复现

## 目录结构

```
cmd/dqmonitor          CLI 入口（run / replay / history）
internal/model         核心领域模型与稳定的失败原因码（ReasonCode）
internal/config        声明式配置加载与校验
internal/datasource    CSV / JSONL 加载、字段级校验、形态漂移检测
internal/rule          四类规则检查器（completeness/uniqueness/consistency/freshness）
internal/state         批次状态存储（每批次短租约锁、历史结论、迟到批次受理）
internal/alert         告警归一化、两阶段投递日志与跨进程精确一次补投
internal/engine        并发编排、资源限流、超时/取消/panic 隔离、指纹、跨进程单飞
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

# 退出码：0 全部通过（或纯重放）；1 存在 failed/errored 规则；2 用法或状态流转错误
# （如历史结论不可覆盖、存在未回收的中断批次、无效租约）

# 2. 重复执行同一批次 -> 幂等重放，结论/指纹不变，不重复告警
go run ./cmd/dqmonitor run --config examples/config.json \
  --batch-id 20260923-001 --state-dir .dq-state --eval-time 2026-09-23T10:00:00Z

# 3. 中断后重跑：崩溃遗留的 running 批次默认拒绝，需显式回收
go run ./cmd/dqmonitor run --config examples/config.json \
  --batch-id 20260923-002 --state-dir .dq-state --force

# 3b. 从未处理过的更老批次迟到：无需 --force，正常受理、独立产出
go run ./cmd/dqmonitor run --config examples/config.json \
  --batch-id 20260920-999 --state-dir .dq-state --eval-time 2026-09-20T10:00:00Z

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

批次状态保存在 `--state-dir/batches/<batch-id>.json`（原子写 `tmp + rename`）。
**锁是"每批次、短临界区"的**：跨进程互斥通过 `flock` 落在
`--state-dir/locks/<batch-id>.lock`，仅在 Begin/Commit/Heartbeat/Fail 一次
状态流转期间持有；`Get`/`List`（即 `replay`/`history`）只读不加锁。因此：

- 不同批次永远不会互相阻塞；
- 一个批次还在跑时，其它批次与只读的 `history` / `replay` 照常进行；
- 同一批次的并发触发才会被串行化。

```
(不存在) ──Begin──▶ running(携带租约 token) ──Commit(token)──▶ committed
                         │ 崩溃 / Fail
                         └─ 留在 running（租约失效）── 仅允许 --force 回收重跑
```

- **迟到批次（late arrival）**：判定只看"这个 batch id 是否处理过"，**不看 ID
  字典序/新旧**。一个从未出现过的更老批次照常受理，独立产出自己的结论与通知；
  已经存在的两个批次互不覆盖。系统里**没有高水位、没有回退拒绝**。
- **重复执行（幂等）**：已提交批次再次 `Begin` 直接返回存储的历史结论
  （`status=replay`），不重新校验、不覆盖、不重复告警；历史结论可随时复现。
  对已提交批次使用 `--force` 会返回 `ErrAlreadyConcluded`，历史结论不可改写。
- **并发触发同一批次（单飞）**：
  - 同进程：后到的触发等待，返回与先到触发**完全相同**的结果/错误；
  - 跨进程：发现该批次被一份**未过期租约**持有时返回 `ErrBatchRunning`，引擎
    会轮询等待，待对方提交后返回**同一份结论**（`status=replay`）。最终只有
    一份 committed 结论、每条规则一条物理通知。
- **中途失败重跑**：崩溃留下的 `running` 批次在租约过期后默认返回
  `ErrStaleBatch`；必须 `--force` 才会丢弃中断的中间状态并签发新租约重新开始。
  提交时校验租约 token，token 不匹配返回 `ErrLease`，其它进程无法劫持提交。
  正常运行期间引擎按周期 Heartbeat 续租，长跑批次不会被误判为崩溃遗留。

### 历史结论什么情况下还算有效

重放并不盲目返回旧结论：提交时会记录一份**输入快照**（判定时间、完整规则
声明的哈希、每个数据集的文件内容哈希/解析形态哈希/记录数/读取失败码）。重放时
对当前输入重新快照比较：

- 判定时间（`--eval-time`）一致、规则声明未变、每个数据集的文件内容与形态都
  未变 → 历史结论**有效**，直接重放（并顺带补投未送达的告警）；
- 任一不同（数据文件内容变、字段/列形态漂移、文件从有到无/从无到有、数据集
  声明增删、规则集或参数或严重度变化、判定时间不同）→ 返回 **`DriftError`**，
  变更区分为 `data` / `rule` / `eval` 三类，拒绝把陈旧结论当作当前结论。

### 通知补投规则（精确一次）

告警投递由 append-only 日志 `alert-journal.jsonl` 协调，采用**两阶段协议**：

1. **投递前**：先把一条 `pending` 记录（含稳定的 `DeliveryID`）写入日志并
   `fsync`，使"投递意图"在崩溃后仍然存在；
2. **投递**：调用通知通道；内置文件通道（`alerts.jsonl`）实现幂等接口，按
   `DeliveryID` 去重，已收过的同 ID 通知不会再写第二次；
3. **投递成功后**：追加一条 `delivered` 记录并 `fsync`。

重启/重放时对每个 `(batch, rule)` 做对账：

- 已有 `delivered` → **永不重发**；
- 只有 `pending`（进程在投递前/投递中/投递后确认前被中断）→ 用**同一个
  DeliveryID** 重试：通道若其实已经收到，则把重复抑制掉（不算新投递），再把
  该意图标记 delivered；通道若确实没收到，则真正补发；
- 没有任何记录 → 新建 pending 意图并投递。

这样无论进程在**投递前、投递途中还是投递完成之后**被中断，重启后都不会给幂等
通道发第二次；当时真正没送出去的会在后续任意触发里自动补齐。每次调用返回的
`alert_report` 都会给出本次对账结果：

```json
{ "expected": 5, "already_delivered": 3, "sent_this_run": 1, "pending": 1 }
```

即"共应有 5 条；此前已送达 3 条（本次未重发）；本次新送出/补齐 1 条；还剩 1 条
未送达"，恒有 `expected = already_delivered + sent_this_run + pending`。投递在
**批次提交之后**进行，不改变判定结论；通道故障只反映在报告和日志里。

**兼容范围**：内置的 `alert.FileSink` 与实现了 `TokenSink`
（`SendToken(ctx, alert) (sent bool, err error)`）的自定义通道享受完整的"中断
窗口也不重复"语义。只实现 `Sink.Send` 的自定义通道拿不到幂等键，因此在"已成功
送出但 delivered 记录尚未落盘"这一极窄窗口内崩溃，重启最多可能向该通道重复一次；
正常关停后的重放/重跑仍然不重复。需要严格精确一次时，请让自定义通道实现
`TokenSink`。旧版本写下的、无 `status` 字段的日志行会被当作 `delivered` 兼容读取。

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
  底层 error 文本；
- 同批次的多次触发单飞（同进程共享结果，跨进程靠每批次 `flock` + 租约等待同一份
  结论）；不同批次使用不同锁文件，彼此与只读查询都不阻塞。

## 日志

运行时使用 `log/slog` 打印判定依据：

- `input summary`：数据集名、格式、声明字段数、记录数、形态问题数、加载耗时；
- `rule verdict`：规则、数据集、类型、状态、finding 编码、证据条数、耗时；
- 告警对账依据：`alert delivered`（本次新送达，含 `delivery_id`）、
  `crash-interrupted alert confirmed; duplicate suppressed by sink`
  （崩溃窗口恢复、通道侧抑制了重复）、`alert already delivered; skipping`
  （此前已送达）、`alert delivery failed; left pending for later backfill`
  （本次未送出、保留待补，附原因）。

## 本地复现验证

下面命令用两个独立的 CLI 进程模拟并发与中断（也可用
`go run ./cmd/dqmonitor ...` 代替已编译二进制）。

```bash
go build -o /tmp/dqmon ./cmd/dqmonitor
rm -rf /tmp/dq && mkdir -p /tmp/dq
RUN='/tmp/dqmon run --config examples/config.json --state-dir /tmp/dq/state --eval-time 2026-09-23T10:00:00Z'

# 1) 两个进程几乎同时触发同一批次 -> 恰好一个 committed、一个 replay，
#    指纹相同，alerts.jsonl 每条规则只有一条。
$RUN --batch-id B & $RUN --batch-id B & wait
wc -l /tmp/dq/state/alerts.jsonl        # = 失败/错误规则数
python3 - <<'PY'
import json,collections
print(collections.Counter(json.loads(l)["rule_id"] for l in open("/tmp/dq/state/alerts.jsonl")))
PY

# 2) 通知已发出、确认前被中断：跑完后把 journal 里该批次的 delivered 行删掉、
#    只留 pending，再重启 -> alerts.jsonl 行数不变（无重复），报告 pending=0。
$RUN --batch-id C
python3 - <<'PY'
import json
p="/tmp/dq/state/alert-journal.jsonl"
out=[l for l in open(p) if not (json.loads(l)["batch_id"]=="C" and json.loads(l)["status"]=="delivered")]
open(p,"w").write("".join(out))
PY
before=$(wc -l < /tmp/dq/state/alerts.jsonl)
$RUN --batch-id C                        # 重启重放
test "$(wc -l < /tmp/dq/state/alerts.jsonl)" = "$before" && echo "no duplicate"

# 3) 一个批次在跑时，另一进程跑别的批次 / 查历史都不被挡：
$RUN --batch-id RUNNING &                # 长任务进行中
$RUN --batch-id OTHER                    # 不同批次照常完成
/tmp/dqmon history --state-dir /tmp/dq/state   # 只读查询照常返回
```

自动化测试（含竞态检测，覆盖以上组合场景）：

```bash
go test ./... -race -count=1
# 关注点：
#   internal/state  不同批次/只读不阻塞、同批次跨句柄串行
#   internal/alert  投递前/中/后中断恢复、失败补齐、同批次并发仅一次
#   internal/e2e    跨进程同批次单飞、崩溃重启不重复、失败跨进程补齐、
#                   在途批次不挡其它批次与历史查询
```

## 示例数据说明

`examples/data/users.csv` 与 `examples/data/events.jsonl` 内置了重复主键、
域外形如 `banned` 的状态、非数字年龄、空年龄、null `user_id`、过期事件等，
配合 `examples/config.json` 可一次性观察到 `duplicate_value`、
`value_outside_set`、`type_mismatch`、`null_value`、`stale_record` 等各类
可区分结论。
