# Lease & Rate Limit API — 分布式独占任务租约与滑动窗口限流服务

供多个工作进程协调独占任务的租约 API。客户端按任务键（`task_key`）申请带 TTL 的
租约；仅当任务未被占用或旧租约已过期时申请成功，并返回**所有权令牌**与**单调递增
的防栅序号（fencing token）**。续租与释放必须同时校验令牌与防栅序号，旧持有者在
超时后无法影响新租约；另提供校验接口，供下游判断一次操作是否仍属于当前租约。
当前持有者还可通过**交接**接口把单个任务租约原子交给服务端签发的新凭据
（新 token、防栅序号递增、保留原过期时刻不续期），交接后旧凭据立即失效。

每次**申请（含成功与被拒）、续租（单个 / 批量）、交接（成功 / 失败）、释放、
结果提交以及批量申请的逐任务结果**都会在状态转换的同一个 Lua 脚本内原子写入按
任务键分组的
**Redis Stream 审计事件**，事件包含操作结果、
关联防栅序号与 Redis 服务器时间；通过
`GET /v1/leases/{task_key}/events?cursor=&limit=` 可游标分页读取（返回
`next_cursor`），每个任务键默认保留最新 1000 条，超出按最旧事件裁剪（条数可由
环境变量配置）。

还提供**按资源键分配并发容量的槽位 API**：多个工作进程按 `resource_key` 共享
有上限的处理槽位。先 `PUT /v1/capacity/{resource_key}` 设置整数容量（权重
上限），再 `POST /v1/capacity/slots/acquire` 申请占用——申请可声明整数
**权重** `weight`（默认 1，如批量条数 / GPU 卡数），成功返回**独立凭据**
`credential` 与到期时间 `expires_at_ms`。"按 Redis 服务器时间清理过期占用 →
核算有效占用权重 → 判定剩余 → 写入"在**单个 Lua 脚本**内原子完成，**并发申请
绝不超配**；容量不足返回 `429` 与建议重试时间。有效占用权重由脚本在同一 Lua
操作内维护的**已占用权重汇总**给出：清理到期成员时逐笔扣减，申请 / 释放同步
增减，常规请求不再遍历资源键下的全部占用；首次遇到升级前遗留的"有占用集合、
缺汇总"数据时在脚本内重建一次，集合自然过期或清空后汇总同步失效。续期
（`POST /v1/capacity/slots/renew`）与释放
（`POST /v1/capacity/slots/release`）**只接受该占用的凭据**，凭据错误或占用
已过期一律无效（续期 `409 not_found`、释放幂等忽略），**过期或旧凭据不可能
影响过期后新申请得到的新占用**。容量调整若**低于当前有效占用量**会被拒绝
（`409 below_active`）且**保留原配置**。占用到期后由后续操作按服务器时间自动
清理，槽位自动回收。

另提供 **Redis Lua 原子滑动窗口（成本）限流 API**：`POST /v1/limits/check`
按 Redis 服务器时间累计滑动窗口内的请求成本，未超限则原子扣减并返回剩余额度，
超限返回 `429` 与建议重试时间——按记录到期顺序累计释放的成本、首次足以容纳
本次 `cost` 的等待毫秒数（同毫秒到期合并判断），避免加权场景下"只等最老一笔、
届时额度仍不足"的过早重试；同一 `client_key` 的并发请求不会超额，过期窗口
数据自动清理。同一 `client_key` 还可交替使用不同的 `window_ms`：成本记录按
服务端允许的最大窗口（`LIMITS_MAX_WINDOW_MS`）保留，每次请求只按本次窗口
筛选判定，短窗口检查不会删除仍属于长窗口的记录，切回长窗口后旧记录仍被正确
计入。请求还可携带可选 `request_id` 实现**窗口内请求幂等**：相同
`client_key` + `request_id` 的并发或重试请求直接返回首次判定结果且不重复扣减，
`request_id` 仅在对应窗口内存活，跨窗口可再次使用。

还提供**任务结果提交 API**：`POST /v1/results/commit` 接收 `task_key`、`token`、
`fencing_token` 与任意 JSON 结果，**租约校验与结果写入在同一个 Redis Lua 脚本内
原子完成**，只有当前未过期租约的凭据可以提交，杜绝"校验后租约易主、旧持有者覆盖
结果"。同一防栅序号首次提交后：相同内容的重试原样返回首次结果并标记重放（
`replayed=true`，响应头 `Result-Replay: true`）；不同内容返回 `409`
（`result_conflict`）且不改动已存结果；租约到期被新持有者接管后，新防栅序号可
提交并覆盖为新结果。`GET /v1/results/{task_key}` 返回最近一次已提交结果及其防栅
序号；**结果不设 TTL，不随租约到期删除**。

还提供**任务依赖门禁 API**：`POST /v1/dependencies/{task_key}` 整体保存一个任务
的前置任务键（按首次出现顺序**去重**、最多 **16** 个，空列表清除定义），
**禁止自身依赖**；环检测在 **Redis Lua** 中先遍历**现有**依赖定义，从每个前置键
出发判断能否回到本任务键，**单次最多访问 1024 个节点**，发现环或访问超限即拒绝
（`409`）且**保留原定义**，通过后再原子替换。
`GET /v1/dependencies/{task_key}/ready` 依据**现有持久任务结果**（即
`/v1/results` 的无 TTL 结果键）判断所有直接前置任务是否都已提交，返回 `ready`
与尚未完成的键列表 `pending`；**没有定义的任务视为可执行**。依赖键沿用租约 /
结果的 `LEASE_KEY_PREFIX` 前缀（`:deps:` 命名空间）。

在上述门禁之上还提供**前置结果版本保护**：`GET
/v1/dependencies/{task_key}/snapshot` 在**单个只读 Lua 脚本**内一次性读取当前
直接前置列表与每个前置已提交结果的 `fencing_token`（结果版本），按依赖定义顺序
返回一致快照；有未完成前置时返回 `pending`、**不生成可提交快照**。任务执行完后
调用 `POST /v1/results/commit-checked`，把租约凭据、结果与该快照一并提交，服务端
在**同一个 Lua 操作**内核对有效租约、当前前置列表及各前置结果版本——全部一致才
沿用现有结果提交的防栅与幂等语义写入；拍快照后前置定义被替换或任一前置结果被新
租约提交覆盖时返回 `409 stale_dependencies` 且**不改结果**。无前置任务使用空快照
（`snapshot=[]`）。原 `POST /v1/results/commit` 接口保持不变。

还提供**任务结果变更订阅**：普通提交与携带前置快照的受保护提交，仅在**首次写入
新结果版本**时，于结果写入的**同一个 Lua 脚本**内原子向**所有任务键共用的一条
全局 Redis Stream** 追加一条变更，记录 `task_key`、防栅序号、提交时间与当次
JSON 结果，跨任务键按写入全局定序；**提交失败（凭据无效 / 内容冲突 / 前置过期）
与相同内容重放都不追加**。下游通过
`GET /v1/results/changes?cursor=&limit=` 以 Stream 条目 ID 为游标按写入顺序
分页读取（返回 `next_cursor`）；不传游标从当前保留的最早变更读起，游标早于最早
保留条目（已被裁剪）时返回 **410**，避免静默漏读。"最早保留条目校验 → 按游标
读取页面 → 恰满一页时探测后续条目决定 `next_cursor`"由**单个只读 Lua 脚本**在
同一次 Redis 原子执行内完成，三步共用同一份流快照，并发提交触发的 MAXLEN 裁剪
不可能落在校验与读取之间。全局流默认保留最新 **10000**
条，条数可由环境变量配置。

技术栈：Python 3.12 · FastAPI · Redis 7（Lua 脚本保证原子性）

## 快速开始（一条命令）

```bash
docker compose up --build
```

启动后：

| 组件 | 地址 |
| --- | --- |
| API | http://localhost:8000 |
| 交互式文档 (Swagger UI) | http://localhost:8000/docs |
| 健康检查 | http://localhost:8000/healthz |
| Redis | localhost:6379 |

Compose 会同时拉起 Redis 与 API 两个容器，API 等待 Redis 健康检查通过后启动。

## 配置

全部通过环境变量配置（见 `docker-compose.yml` 中的示例）：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis 连接串 |
| `LEASE_KEY_PREFIX` | `lease` | Redis 键前缀，便于多环境共用实例 |
| `LEASE_MIN_TTL_MS` | `100` | 允许的最小 TTL（毫秒）；租约与容量占用共用 |
| `LEASE_MAX_TTL_MS` | `300000` | 允许的最大 TTL（毫秒）；租约与容量占用共用 |
| `LEASE_DEFAULT_TTL_MS` | `10000` | 请求未指定 `ttl_ms` 时的默认值；租约与容量占用共用 |
| `LEASE_EVENT_RETENTION` | `1000` | 每个任务键的租约审计 Stream 保留的最新事件条数；超出按最旧事件裁剪。`0` 表示不限制 |
| `LEASE_RESULT_CHANGE_RETENTION` | `10000` | 全局任务结果变更 Stream（`/v1/results/changes`，所有任务键共用）保留的最新变更条数；超出按最旧变更裁剪。`0` 表示不限制 |
| `LIMIT_KEY_PREFIX` | `ratelimit` | 限流 Redis 键前缀，与租约键隔离 |
| `LIMIT_IDEM_KEY_PREFIX` | `ratelimit-idem` | 限流幂等记录 Redis 键前缀，与限流 ZSet 隔离 |
| `LIMITS_MIN_WINDOW_MS` | `1` | 允许的最小限流窗口（毫秒） |
| `LIMITS_MAX_WINDOW_MS` | `86400000` | 允许的最大限流窗口（毫秒，默认 1 天）；同时决定限流成本记录的保留时长与限流键的兜底 TTL |

服务端口固定为 **8000**（容器内），可在 `docker-compose.yml` 的 `ports` 中调整宿主机映射。

## API

所有端点位于 `/v1` 前缀下，请求与响应均为 JSON。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/leases/acquire` | 申请租约，成功 200 / 被占用 409 |
| POST | `/v1/leases/acquire-batch` | 批量申请 2～32 个不重复任务键（统一 TTL），全成全败 |
| POST | `/v1/leases/renew` | 续租，成功 200 / 失败 409（reason: `not_found` / `conflict` / `expired`） |
| POST | `/v1/leases/renew-batch` | 批量续租 2～32 个不重复任务键（各自 token / fencing_token，统一 TTL），全成全败、逐项失败原因 |
| POST | `/v1/leases/transfer` | 当前持有者交接租约：换新 token、防栅序号递增、保留原过期时刻，成功 200 / 失败 409（reason: `not_found` / `conflict` / `expired`） |
| POST | `/v1/leases/release` | 释放租约（幂等，始终 200） |
| POST | `/v1/leases/validate` | 校验 (令牌, 防栅序号) 是否仍属当前租约 |
| GET | `/v1/leases/{task_key}` | 查看任务键租约状态（不暴露令牌） |
| GET | `/v1/leases/{task_key}/events` | 分页读取租约审计事件（`cursor` / `limit`，返回 `next_cursor`） |
| POST | `/v1/results/commit` | 提交任务结果（租约凭据原子校验 + 防栅序号幂等），首次写入 / 相同内容重放 200，凭据无效或同序号不同内容 409 |
| POST | `/v1/results/commit-checked` | 携带前置版本快照提交结果：同一 Lua 操作内核对租约 + 当前前置列表及各结果版本，全部一致才沿用防栅与幂等语义写入；不一致 409 `stale_dependencies` 且不改结果；无前置用空快照 |
| GET | `/v1/results/changes` | 按全局写入顺序订阅任务结果变更（跨任务键，`cursor` / `limit`，返回 `next_cursor`）；仅首次写入新结果版本追加；不传游标从最早保留条目读起，游标早于最早保留条目返回 **410**；游标校验、分页读取与 `next_cursor` 判定在**单个只读 Lua 脚本**内原子完成，并发裁剪不会导致静默漏读 |
| GET | `/v1/results/{task_key}` | 读取最近一次已提交结果及防栅序号（持久保留，不随租约到期删除） |
| POST | `/v1/dependencies/{task_key}` | 保存前置依赖定义（去重、最多 16 个，空列表清除），Lua 遍历现有定义检测环、单次最多访问 1024 个节点，成环 / 超限 409 且保留原定义；自身依赖 422 |
| GET | `/v1/dependencies/{task_key}/ready` | 门禁判定：所有直接前置任务均已提交持久结果则 `ready=true`，否则返回未完成键 `pending`；无定义视为可执行 |
| GET | `/v1/dependencies/{task_key}/snapshot` | 单个只读 Lua 操作返回当前直接前置列表及各结果 `fencing_token`（按定义顺序）；全部完成返回可提交快照，有未完成前置返回 `pending`（不出快照），无定义返回空快照 |
| POST | `/v1/limits/check` | 滑动窗口成本限流检查（通过即扣减，可选 `request_id` 幂等），放行 200 / 超限 429 |
| PUT | `/v1/capacity/{resource_key}` | 设置资源键并发容量（整数权重上限）；低于当前有效占用量时 409 `below_active` 且保留原配置 |
| POST | `/v1/capacity/slots/acquire` | 申请加权占用（可声明 `weight`，默认 1），返回独立凭据与到期时间；未配置 409、容量不足 429、权重超容量 409 |
| POST | `/v1/capacity/slots/renew` | 续期占用（仅接受该占用凭据）；成功 200 / 凭据错误或已过期 409 `not_found` |
| POST | `/v1/capacity/slots/release` | 释放占用（仅接受该占用凭据，幂等，始终 200） |
| GET | `/healthz` | 健康检查 |

### 示例

```bash
# 1. 申请租约（TTL 5 秒）
curl -s -X POST localhost:8000/v1/leases/acquire \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "ttl_ms": 5000}'
# => {"acquired":true,"task_key":"report:daily","token":"9f3c...","fencing_token":1,"expires_at_ms":1758...}

# 占用期间他人申请 => 409 {"acquired":false,...}

# 2. 批量申请（2～32 个不重复 task_key，统一 TTL，全成全败）
curl -s -X POST localhost:8000/v1/leases/acquire-batch \
  -H 'Content-Type: application/json' \
  -d '{"task_keys": ["report:daily", "report:weekly", "report:monthly"], "ttl_ms": 5000}'
# 成功 200（leases 顺序与请求一致；expires_at_ms 为同一次服务器 TIME 的基准）：
# => {"acquired":true,"leases":[
#   {"task_key":"report:daily","token":"a1b2...","fencing_token":1,"expires_at_ms":1758...},
#   {"task_key":"report:weekly","token":"c3d4...","fencing_token":1,"expires_at_ms":1758...},
#   {"task_key":"report:monthly","token":"e5f6...","fencing_token":1,"expires_at_ms":1758...}
# ],"conflicting_task_keys":[]}
#
# 任一任务仍被占用 => 409，返回全部冲突键，不创建任何租约、不递增任何防栅计数器：
# => {"acquired":false,"leases":[],"conflicting_task_keys":["report:weekly"]}
#
# 过期租约与不存在的键同样视为可用，可被批量接管。
# 批量所得 (token, fencing_token) 与单个申请完全等价，
# 可直接用于下面的续租 / 校验 / 释放接口（每个任务键独立调用）。

# 3. 续租（必须同时带上令牌与防栅序号）
curl -s -X POST localhost:8000/v1/leases/renew \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "token": "9f3c...", "fencing_token": 1, "ttl_ms": 5000}'

# 4. 批量续租（2～32 个不重复 task_key；各自携带 token / fencing_token，统一 ttl_ms）
curl -s -X POST localhost:8000/v1/leases/renew-batch \
  -H 'Content-Type: application/json' \
  -d '{"items": [
    {"task_key": "report:daily",   "token": "9f3c...", "fencing_token": 1},
    {"task_key": "report:weekly",  "token": "c3d4...", "fencing_token": 1},
    {"task_key": "report:monthly", "token": "e5f6...", "fencing_token": 1}
  ], "ttl_ms": 5000}'
# 全部有效 => 200，leases 按输入顺序给出新 expires_at_ms（同一次服务器 TIME 基准）：
# => {"renewed":true,"leases":[
#   {"task_key":"report:daily","expires_at_ms":1758...},
#   {"task_key":"report:weekly","expires_at_ms":1758...},
#   {"task_key":"report:monthly","expires_at_ms":1758...}
# ],"failures":[]}
#
# 任一租约不存在 / 凭据不匹配 / 已过期 => 409 整批失败，
# 不改变任何 TTL（成功项也不续租），failures 仅含失败项且按输入顺序给出原因：
# => {"renewed":false,"leases":[],"failures":[
#   {"index":1,"task_key":"report:weekly","reason":"not_found"},
#   {"index":2,"task_key":"report:monthly","reason":"conflict"}
# ]}
# reason 取值与单项续租一致：not_found / conflict / expired。
# 每个任务的结果（renewed 或失败原因）都原子写入各自的审计 Stream，带 batch_size。

# 5. 交接租约（当前持有者原子换新凭据；保留原过期时刻，不续期）
curl -s -X POST localhost:8000/v1/leases/transfer \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "token": "9f3c...", "fencing_token": 1}'
# 成功 200：服务端生成新 token、防栅序号递增（1 -> 2），expires_at_ms 与交接前相同：
# => {"transferred":true,"task_key":"report:daily",
#     "token":"7d8e...","fencing_token":2,"expires_at_ms":1758...,"reason":null}
#
# 交接后旧 (token, fencing_token) 在续租 / 释放 / 校验接口立即失效，
# 新凭据立即可用（可直接用于下面的校验 / 释放，以及再次续租 / 再次交接）。
# 交接不携带、也不接受 ttl_ms：原 expires_at 保持不变。
#
# 失败统一返回 409 且不改动租约、不递增防栅序号：
#   not_found  租约不存在
#   conflict   令牌或防栅序号不匹配（旧持有者交接后再用旧凭据交接即为此类）
#   expired    租约已过期（过期后须重新申请，不能交接）
# => HTTP/1.1 409 Conflict
# {"transferred":false,...,"reason":"conflict"}

# 6. 下游校验某次操作是否仍属当前租约（交接后须使用新凭据）
curl -s -X POST localhost:8000/v1/leases/validate \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "token": "7d8e...", "fencing_token": 2}'
# => {"valid":true,"remaining_ms":4210}

# 7. 释放租约
curl -s -X POST localhost:8000/v1/leases/release \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "token": "7d8e...", "fencing_token": 2}'
```

### 任务结果

持有有效租约（未过期的 `token` + `fencing_token`）时可提交任务结果。
租约校验与结果写入在同一个 Lua 脚本内原子完成。

```bash
# 1. 申请租约后提交结果（任意 JSON：对象 / 数组 / 字符串 / 数字 / 布尔 / null）
curl -s -X POST localhost:8000/v1/results/commit \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "token": "9f3c...", "fencing_token": 1,
       "result": {"status": "done", "rows": 42}}'
# => 200 首次提交：
# {"committed":true,"replayed":false,"task_key":"report:daily",
#  "fencing_token":1,"result":{"status":"done","rows":42},
#  "committed_at_ms":1758...,"reason":null}

# 2. 网络超时 / 客户端重试，同一防栅序号 + 相同内容再次提交：
#    原样返回首次结果并标记重放，不写入、不刷新 committed_at_ms
curl -i -X POST localhost:8000/v1/results/commit \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "token": "9f3c...", "fencing_token": 1,
       "result": {"rows": 42, "status": "done"}}'   # 对象键顺序不同也视为相同
# => HTTP/1.1 200 OK
#    Result-Replay: true
# {"committed":true,"replayed":true,...,"committed_at_ms":1758...}  # 与首次一致

# 3. 同一防栅序号提交不同内容 => 409 result_conflict，已存结果不变
curl -s -X POST localhost:8000/v1/results/commit \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "token": "9f3c...", "fencing_token": 1,
       "result": {"status": "done", "rows": 43}}'
# => HTTP/1.1 409 Conflict
# {"committed":false,...,"reason":"result_conflict"}

# 4. 凭据无效同样 409 且不写结果：
#    not_found（租约不存在）/ conflict（令牌或防栅序号不匹配）/ expired（已过期）
#    ——旧持有者在租约到期 / 被接管后无法覆盖新结果

# 5. 租约到期后被新持有者接管（fencing_token 递增为 2），新凭据可提交新结果
curl -s -X POST localhost:8000/v1/results/commit \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "token": "1a2b...", "fencing_token": 2,
       "result": {"status": "done", "rows": 44}}'
# => 200 committed=true, replayed=false，结果覆盖为新版本

# 6. 读取最近一次已提交结果：结果不设 TTL，租约到期 / 释放后仍可读
curl -s localhost:8000/v1/results/report:daily
# => {"task_key":"report:daily","committed":true,"fencing_token":2,
#     "result":{"status":"done","rows":44},"committed_at_ms":1758...}
#
# 从未提交过结果 => 200 {"task_key":"...","committed":false,
#                        "fencing_token":null,"result":null,"committed_at_ms":null}
```

字段说明：

| 字段 | 方向 | 说明 |
| --- | --- | --- |
| `task_key` | 请求 / 响应 | 任务键（1～512 字符） |
| `token` | 请求 | 当前有效租约的所有权令牌 |
| `fencing_token` | 请求 / 响应 | 当前有效租约的防栅序号，作为结果的提交版本 |
| `result` | 请求 / 响应 | 任意 JSON 值。内容按规范化 JSON 比较（排序对象键、忽略空白），语义相同即视为相同内容 |
| `committed` | 响应 | 凭据为当前有效租约时为 `true`（首次写入与相同内容重放均为 `true`）；409 时为 `false` |
| `replayed` | 响应 | 同一防栅序号已提交且内容一致时为 `true`，响应头同时带 `Result-Replay: true`；不刷新 `committed_at_ms` |
| `committed_at_ms` | 响应 | 结果首次提交时刻（Redis 服务器时钟，Unix 毫秒），重放 / 覆盖语义见上 |
| `reason` | 响应 | 仅 409 时返回：`not_found` / `conflict` / `expired` / `result_conflict` |

### 任务结果变更订阅（全局顺序）

下游需要按**全局顺序**处理所有任务已提交的结果时，轮询
`GET /v1/results/changes` 即可。普通提交与携带前置快照的受保护提交，仅在
**首次写入新结果版本**时（结果 Hash 覆盖写入、响应 `committed=true`、
`replayed=false`），在结果写入的同一个 Lua 脚本内原子向**全局**变更流
（`{prefix}:result-changes`，所有任务键共用一条）追加一条变更；提交失败
（`not_found` / `conflict` / `expired` / `result_conflict` /
`stale_dependencies`）与相同内容重放（`replayed=true`）**都不追加**，因此一条
变更恰好对应一次结果版本推进，不会因重试产生重复消息。

```bash
# 首次拉取（不传 cursor）：从当前保留的最早变更开始，默认每页 50 条（limit 1～200）
curl -s 'localhost:8000/v1/results/changes?limit=20'
# => {"changes":[
#   {"id":"1758000000123-0","task_key":"report:daily","fencing_token":1,
#    "committed_at_ms":1758000000123,"result":{"status":"done","rows":42}},
#   {"id":"1758000000789-0","task_key":"report:weekly","fencing_token":1,
#    "committed_at_ms":1758000000789,"result":{"rows":7}}
# ],"next_cursor":"1758000000789-0"}

# 用上次返回的 next_cursor 继续翻页（游标为闭区间尾部，已自动去重，不重不漏）
curl -s 'localhost:8000/v1/results/changes?cursor=1758000000789-0&limit=20'
# 没有更多变更时 next_cursor 为 null

# 游标早于当前保留的最早条目（已被 MAXLEN 裁剪）-> 410 Gone，
# 不会静默从更新的位置开始而漏掉中间变更；调用方应不带 cursor 从最早可用
# 条目重新追赶（或改走全量对账）：
curl -i 'localhost:8000/v1/results/changes?cursor=1700000000000-0'
# => HTTP/1.1 410 Gone
# {"detail":"cursor is older than the oldest retained change; ..."}

# 非法 cursor（不是 <数字>-<数字>）-> 400；limit 越界 -> 422
```

分页读取的"最早保留条目校验 → 按 `(cursor, limit]` 读取页面 → 恰满一页时
探测后续条目决定 `next_cursor`"由**单个只读 Lua 脚本**在同一次 Redis 原子
执行内完成：三步共用同一份流快照，并发提交触发的 `MAXLEN` 裁剪不可能落在
校验与读取之间，因此失效游标只会得到 **410**，不会得到 200 而静默漏读；
未失效游标的页面按写入顺序衔接且不重复（恰满一页且后面还有条目时
`next_cursor` 指向本页末条，满页恰好是最后一页时为 `null`）。

字段与规则说明：

| 字段 / 规则 | 说明 |
| --- | --- |
| `id` | Redis Stream 条目 ID（`<毫秒时间>-<序号>`），即分页游标；全局严格升序 |
| `task_key` | 提交该结果的任务键 |
| `fencing_token` | 该结果对应的防栅序号（结果版本），与 `GET /v1/results/{task_key}` 同源 |
| `committed_at_ms` | 结果提交时刻（Redis 服务器时钟，Unix 毫秒） |
| `result` | 当次提交的 JSON 结果（按规范化 JSON 存储，读回时反序列化为 JSON） |
| `next_cursor` | 下一页游标（本页最后一条变更 ID）；本页不足 `limit` 或没有更多变更时为 `null` |

- 只追加，不反映结果删除（结果本身无 TTL）；同一任务键被新租约提交新版本时
  会再追加一条，下游可凭 `(task_key, fencing_token)` 识别版本推进。
- 全局流以 `XADD MAXLEN n` 在写入同时精确裁剪，只保留最新
  `LEASE_RESULT_CHANGE_RETENTION` 条（默认 **10000**，`0` 为不限制），超出从最旧
  变更丢弃；其键与结果 Hash 共享 `LEASE_KEY_PREFIX` 前缀（`:result-changes`）。
- 该路由注册在按 `task_key` 捕获的 `GET /v1/results/{task_key}` **之前**，
  因此 `changes` 不会被当作任务键。

### 任务依赖门禁

为任务声明一组**前置任务键**，只有所有直接前置任务都已提交持久结果时，该任务才
"就绪可执行"。依赖定义保存在 Redis List（`{prefix}:deps:{task_key}`，沿用
`LEASE_KEY_PREFIX` 前缀），环检测由单个 Lua 脚本原子完成。

```bash
# 1. 保存 task-c 的前置任务（整体替换）。服务端按首次出现顺序去重，最多 16 个
curl -s -X POST localhost:8000/v1/dependencies/task-c \
  -H 'Content-Type: application/json' \
  -d '{"dependencies": ["task-a", "task-b"]}'
# => 200 {"saved":true,"task_key":"task-c","dependencies":["task-a","task-b"],"reason":null}

# 2. 查询门禁：task-a / task-b 尚未提交结果 -> ready=false，pending 给出未完成键
curl -s localhost:8000/v1/dependencies/task-c/ready
# => {"task_key":"task-c","ready":false,"pending":["task-a","task-b"]}

# 3. 前置任务通过 /v1/results/commit 提交结果后（结果持久保留，提交即"完成"）
#    门禁自动就绪，无需额外确认：
curl -s localhost:8000/v1/dependencies/task-c/ready
# => {"task_key":"task-c","ready":true,"pending":[]}

# 4. 没有定义的任务视为可执行
curl -s localhost:8000/v1/dependencies/never-defined/ready
# => {"task_key":"never-defined","ready":true,"pending":[]}

# 5. 重复键服务端去重；原始 17 个但去重后 16 个仍可保存；去重后超过 16 个 -> 422
curl -s -X POST localhost:8000/v1/dependencies/task-c \
  -H 'Content-Type: application/json' \
  -d '{"dependencies": ["task-a", "task-b", "task-a"]}'
# => 200，dependencies 为 ["task-a","task-b"]

# 6. 自身依赖 -> 422
curl -s -X POST localhost:8000/v1/dependencies/task-c \
  -H 'Content-Type: application/json' \
  -d '{"dependencies": ["task-c"]}'
# => 422 {"detail":"task must not depend on itself"}

# 7. 成环或遍历超限 -> 409，且原定义原样保留（响应回带当前生效定义）
#    现有 task-c -> [task-a]，再让 task-a -> task-c 会形成 c->a->c：
curl -s -X POST localhost:8000/v1/dependencies/task-a \
  -H 'Content-Type: application/json' \
  -d '{"dependencies": ["task-c"]}'
# => 409 {"saved":false,"task_key":"task-a","dependencies":[],
#         "reason":"cycle"}
#    单次环检测去重后最多访问 1024 个现有节点，超过返回：
#    {"saved":false,...,"reason":"too_many_nodes"}（同样不改动原定义）

# 8. 空列表清除定义（之后该任务视为无门禁、可执行）
curl -s -X POST localhost:8000/v1/dependencies/task-c \
  -H 'Content-Type: application/json' -d '{"dependencies": []}'
# => 200 {"saved":true,...,"dependencies":[]}
```

字段与规则说明：

| 字段 / 规则 | 方向 | 说明 |
| --- | --- | --- |
| `dependencies` | 请求 | 前置任务键列表（每个 1～512 字符）；服务端按首次出现顺序**去重**，去重后最多 **16** 个，否则 `422`；空列表表示**清除**定义 |
| `task_key`（路径） | 请求 | 目标任务键；出现在 `dependencies` 中即自身依赖，返回 `422` 且不触达 Redis |
| `saved` | 响应 | 定义是否已整体替换；被环 / 遍历上限拒绝时为 `false`（HTTP 409） |
| `dependencies` | 响应 | 成功时为本次写入（去重后、按请求顺序）；409 时回带**当前仍生效的旧定义**（无定义为空列表） |
| `reason` | 响应 | 仅 409 时返回：`cycle`（写入后将形成依赖环，含自身依赖的 Lua 兜底）/ `too_many_nodes`（环检测去重访问节点数超过 **1024**） |
| `ready` | 响应 | 全部直接前置任务均已提交持久结果为 `true`；**无定义任务恒为 `true`** |
| `pending` | 响应 | 尚未提交结果的直接前置任务键（按依赖定义顺序）；就绪时为空列表 |

环检测语义：保存 `T -> [d...]` 前，Lua 从每个 `d` 出发，沿**当前已持久化**的
依赖边（只看现有定义，不含本次待写入内容）判断是否能回到 `T`；能回到 `T` 则
本次写入必成环，拒绝。跨多个起点的遍历共享去重访问集合，**去重后访问节点数以
1024 为硬上限**，达到即判 `too_many_nodes` 拒绝。环与超限都发生在任何写入之前，
因此被拒时原有依赖定义**一字不改**；只有全部检查通过后才在同一脚本内
`DEL + RPUSH` 原子替换。

### 前置结果版本保护（快照 + 受保护提交）

`/ready` 只回答"现在能不能执行"，但"检查 → 执行 → 提交"之间前置结果可能被新
租约重新提交（版本漂移）、依赖定义也可能被替换。**快照 + 受保护提交**把"基于哪
一版前置结果计算"这件事钉死：拍快照后到提交前，只要直接前置列表或任一前置结果
版本发生变化，提交即被原子拒绝。

推荐调用顺序：

1. `GET /v1/dependencies/{task_key}/snapshot` 取快照；
2. `status=pending` 说明仍有前置未完成（`pending` 按定义顺序给出键），等待后
   **重新拉取**；`status=snapshot` 时 `dependencies` 即按定义顺序排列的
   `{task_key, fencing_token}` 列表，作为本次计算的输入版本依据；
3. 申请本任务租约并执行计算；
4. `POST /v1/results/commit-checked`，把响应中的 `dependencies` **原样**放入
   请求体 `snapshot`（无前置任务放 `[]`，或省略该字段）。

```bash
# 0. 前置定义：task-c 依赖 task-a / task-b（与保存依赖接口相同）
curl -s -X POST localhost:8000/v1/dependencies/task-c \
  -H 'Content-Type: application/json' \
  -d '{"dependencies": ["task-a", "task-b"]}'

# 1. 前置尚未全部完成 -> status=pending，不生成可提交快照
curl -s localhost:8000/v1/dependencies/task-c/snapshot
# => {"task_key":"task-c","status":"pending","dependencies":[],
#     "pending":["task-a","task-b"]}

# 2. 前置各自通过 /v1/results/commit 提交持久结果后，重新拉取快照：
#    dependencies 按定义顺序给出前置键及其结果版本（结果 Hash 的 fencing）
curl -s localhost:8000/v1/dependencies/task-c/snapshot
# => {"task_key":"task-c","status":"snapshot",
#     "dependencies":[
#       {"task_key":"task-a","fencing_token":1},
#       {"task_key":"task-b","fencing_token":1}],
#     "pending":[]}

# 3. 申请 task-c 租约、执行任务，然后把快照原样随结果一起提交
curl -s -X POST localhost:8000/v1/results/commit-checked \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "task-c", "token": "9f3c...", "fencing_token": 1,
       "result": {"status": "done", "output": "s3://..."},
       "snapshot": [
         {"task_key": "task-a", "fencing_token": 1},
         {"task_key": "task-b", "fencing_token": 1}]}'
# => 200（与 /v1/results/commit 相同的响应契约与防栅 / 幂等语义）：
# {"committed":true,"replayed":false,"task_key":"task-c",
#  "fencing_token":1,"result":{"status":"done","output":"s3://..."},"reason":null}

# 4a. 网络超时重试，相同快照 + 相同内容 -> 200 replayed=true，
#     响应头 Result-Replay: true，不刷新 committed_at_ms
# 4b. 同一防栅序号不同内容 -> 409 result_conflict，已存结果不变

# 5. 拍快照之后、提交之前，task-a 被新持有者接管并提交了新版本结果
#    （fencing 1 -> 2）-> 旧快照过期，提交被原子拒绝且不改 task-c 的结果：
curl -s -X POST localhost:8000/v1/results/commit-checked \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "task-c", "token": "9f3c...", "fencing_token": 1,
       "result": {"status": "done", "output": "s3://..."},
       "snapshot": [
         {"task_key": "task-a", "fencing_token": 1},
         {"task_key": "task-b", "fencing_token": 1}]}'
# => HTTP/1.1 409 Conflict
# {"committed":false,...,"reason":"stale_dependencies"}
# 调用方应重新拉取快照（必要时基于新版本重算）后再提交。

# 6. 依赖定义被整体替换（增删前置 / 调整顺序）、或快照顺序与当前定义不一致，
#    同样返回 409 stale_dependencies；伪造的版本号也会被 Redis 内当前值驳回。

# 7. 无前置任务（没有定义或空定义）使用空快照：
curl -s localhost:8000/v1/dependencies/task-solo/snapshot
# => {"task_key":"task-solo","status":"snapshot","dependencies":[],"pending":[]}
curl -s -X POST localhost:8000/v1/results/commit-checked \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "task-solo", "token": "1a2b...", "fencing_token": 1,
       "result": {"ok": true}, "snapshot": []}'
# snapshot 字段也可直接省略（缺省即空快照）；空快照下行为与普通提交一致。

# 8. 租约凭据类失败先于前置核对返回，语义与普通提交完全相同：
#    not_found（无租约）/ conflict（令牌或防栅序号不匹配）/ expired（已过期）
```

字段与规则说明（在[任务结果字段表](#任务结果)之上追加）：

| 字段 / 规则 | 方向 | 说明 |
| --- | --- | --- |
| `status` | 快照响应 | `snapshot`（全部直接前置已完成，`dependencies` 可提交）/ `pending`（有未完成前置，`dependencies` 为空） |
| `dependencies`（快照） | 快照响应 | 按依赖定义顺序排列的 `{task_key, fencing_token}`；版本取自前置结果 Hash 的 `fencing`，与 `GET /v1/results/{task_key}` 同源；无前置为空列表 |
| `snapshot` | 提交请求 | `GET .../snapshot` 返回的 `dependencies` 原样带回；无前置任务用 `[]`（缺省值）。列表内键不允许重复、最多 16 个，`fencing_token ≥ 1`，否则 `422` |
| `stale_dependencies` | 409 原因 | 当前前置列表与快照不一致（数量 / 顺序 / 键名），或某前置结果版本与快照不同；**不写入结果**，仅在该任务审计流落一条 `result_commit / stale_dependencies` 事件 |

核对语义：

- 快照由**单个只读 Lua 脚本**生成（一次 `LRANGE` 依赖 List + 逐前置 `HGET`
  结果版本），读定义与读版本之间不可能插入并发的定义替换或结果提交。
- `commit-checked` 的租约校验头部、防栅幂等写入尾部与
  `POST /v1/results/commit` **共用同一段 Lua 代码**（见 `app/lua.py` 中的
  `_RESULT_COMMIT_HEAD_LUA` / `_RESULT_COMMIT_TAIL_LUA`），两者凭据校验、
  重放（`replayed` / `Result-Replay`）、`result_conflict`、审计落账行为逐字一致；
  checked 版本仅在头与尾之间插入前置列表 / 版本核对。
- 前置核对只比较**直接前置**（与 `/ready` 一致，不递归间接依赖）；快照不可跨
  任务使用——服务端始终按路径中的 `task_key` 读取其当前依赖定义。

### 租约审计事件

申请（成功 / 被占用 409）、续租（单个 / 批量）、交接（成功 / 失败）、释放、
结果提交（首次写入 / 重放 / 内容冲突 / 凭据无效）与批量申请都会按**任务键**向对应的
Redis Stream 追加一条事件，与租约状态转换 / 结果写入在同一个 Lua 脚本中原子提交。

```bash
# 读取最早的审计事件（默认每页 50 条，limit 允许 1～200）
curl -s 'localhost:8000/v1/leases/report:daily/events?limit=20'
# => {"task_key":"report:daily","events":[
#   {"id":"1758000000123-0","op":"acquire","result":"acquired",
#    "fencing_token":1,"time_ms":1758000000123,"batch_size":null},
#   {"id":"1758000000456-0","op":"renew","result":"renewed",
#    "fencing_token":1,"time_ms":1758000000456,"batch_size":null},
#   {"id":"1758000000612-0","op":"transfer","result":"transferred",
#    "fencing_token":2,"time_ms":1758000000612,"batch_size":null},
#   {"id":"1758000000789-0","op":"release","result":"released",
#    "fencing_token":2,"time_ms":1758000000789,"batch_size":null}
# ],"next_cursor":"1758000000789-0"}

# 用上一次返回的 next_cursor 继续翻页（游标为闭区间尾部，已自动去重）
curl -s 'localhost:8000/v1/leases/report:daily/events?cursor=1758000000789-0&limit=20'
# 没有更多事件时 next_cursor 为 null

# 被拒 / 幂等忽略的操作同样落账（续租冲突、交接失败、释放不匹配等）
# 交接成功落 transferred（fencing 为递增后的新序号）；交接失败按原因落
# not_found / conflict / expired（fencing 为当前序号，不存在时为 null）
# 批量申请时每个任务键各自落一条 acquired；冲突批次仅冲突任务落 conflict，
# 事件带 batch_size 字段（如 3）
# 批量续租成功时每个任务各落一条 renewed；失败批次仅失败任务按原因落
# not_found / conflict / expired，成功项无状态变化不落账；batch_size 为批次大小
```

字段说明：

| 字段 | 说明 |
| --- | --- |
| `id` | Redis Stream 条目 ID（`<毫秒时间>-<序号>`），即分页游标 |
| `op` | 操作：`acquire` / `acquire_batch` / `renew` / `renew_batch` / `transfer` / `release` / `result_commit` |
| `result` | 操作结果：`acquired` / `conflict` / `renewed` / `transferred` / `released` / `ignored` / `committed` / `replayed` / `result_conflict` / `stale_dependencies` / `not_found` / `expired` |
| `fencing_token` | 事件关联的防栅序号；租约不存在等无关联场景为 `null` |
| `time_ms` | 事件落账时刻（Redis 服务器时钟，Unix 毫秒） |
| `batch_size` | 仅批量申请 / 批量续租事件携带：该批次的任务数量 |

- 游标非法（不是 `<数字>-<数字>`）返回 `400`；`limit` 越界返回 `422`。
- 每个任务键的流默认只保留最新 **1000** 条（精确 `MAXLEN`，从最旧事件裁剪），
  可用 `LEASE_EVENT_RETENTION` 调整，设为 `0` 表示不限制。
- 事件流不会过期，仅供查询；其键与租约 Hash、防栅计数器共享
  `LEASE_KEY_PREFIX` 前缀（`:events:` 命名空间）。

### 并发容量槽位（按资源键，多进程共享有上限槽位）

多个工作进程按 `resource_key` 共享一组有上限的加权处理槽位：先设置容量，
再申请占用，工作完成后续期或释放。所有判定都在 **Redis Lua** 内按服务器时间
原子完成，并发申请绝不超配。

```bash
# 0. 未设置容量的资源键不能申请 -> 409 not_configured
curl -s -X POST localhost:8000/v1/capacity/slots/acquire \
  -H 'Content-Type: application/json' \
  -d '{"resource_key": "gpu-pool"}'
# => HTTP/1.1 409 Conflict
# {"acquired":false,"resource_key":"gpu-pool","weight":1,
#  "capacity":null,"active_weight":0,"remaining":0,"retry_after_ms":0,
#  "reason":"not_configured"}

# 1. 设置容量（整数权重上限）。首次配置 / 调高 / 不低于有效占用的调低均 200
curl -s -X PUT localhost:8000/v1/capacity/gpu-pool \
  -H 'Content-Type: application/json' -d '{"capacity": 10}'
# => {"resource_key":"gpu-pool","configured":true,"capacity":10,
#     "active_weight":0,"reason":null}

# 2. 申请占用（weight 默认 1）。返回独立凭据 credential 与到期时间
curl -s -X POST localhost:8000/v1/capacity/slots/acquire \
  -H 'Content-Type: application/json' \
  -d '{"resource_key": "gpu-pool", "weight": 4, "ttl_ms": 30000}'
# => 200
# {"acquired":true,"resource_key":"gpu-pool",
#  "credential":"9f3c2a...e1:4","weight":4,
#  "expires_at_ms":1758...,"capacity":10,"active_weight":4,"remaining":6,
#  "retry_after_ms":0,"reason":null}
#
# credential 为服务端签发的不透明凭据（内含本次权重），续期 / 释放必须原样带回。

# 3. 剩余容量不足（再申请 weight 7，只剩 6）-> 429，不占用，带 Retry-After
curl -i -X POST localhost:8000/v1/capacity/slots/acquire \
  -H 'Content-Type: application/json' \
  -d '{"resource_key": "gpu-pool", "weight": 7}'
# => HTTP/1.1 429 Too Many Requests
#    Retry-After: 30
# {"acquired":false,...,"capacity":10,"active_weight":10,"remaining":6,
#  "retry_after_ms":29871,"reason":"capacity_exhausted"}
#
# 申请权重本身超过容量（永远无法满足）-> 409 weight_exceeds_capacity（不可重试）：
#   PUT 容量为 5 后申请 "weight": 9 即如此。

# 4. 续期（只认该占用凭据；TTL 缺省取 LEASE_DEFAULT_TTL_MS）
curl -s -X POST localhost:8000/v1/capacity/slots/renew \
  -H 'Content-Type: application/json' \
  -d '{"resource_key": "gpu-pool",
       "credential": "9f3c2a...e1:4", "ttl_ms": 30000}'
# => 200 {"renewed":true,"credential":"9f3c2a...e1:4",
#         "expires_at_ms":1758...,"remaining":6,"reason":null}
#
# 凭据错误或占用已过期 -> 409 {"renewed":false,...,"reason":"not_found"}，
# 不修改任何占用，旧凭据无法复活已过期占用、也无法影响新占用。

# 5. 释放（提前归还权重；凭据错误 / 已过期幂等忽略，始终 200）
curl -s -X POST localhost:8000/v1/capacity/slots/release \
  -H 'Content-Type: application/json' \
  -d '{"resource_key": "gpu-pool", "credential": "9f3c2a...e1:4"}'
# => {"released":true,"weight":4,"remaining":10}
#
# 再次释放同一凭据 -> {"released":false,"weight":0,"remaining":10}
```

调整容量（调高 / 收紧）：

```bash
# 当前有效占用为 10 时，把容量下调到 8 -> 409 below_active，原配置保留
curl -i -X PUT localhost:8000/v1/capacity/gpu-pool \
  -H 'Content-Type: application/json' -d '{"capacity": 8}'
# => HTTP/1.1 409 Conflict
# {"resource_key":"gpu-pool","configured":false,"capacity":10,
#  "active_weight":10,"reason":"below_active"}
# 拒绝时容量键不写入：容量仍为 10（响应 capacity 回带当前生效值，从未配置为 null）。

# 待占用释放到 <= 8 后即可下调成功；上调或设为恰好等于有效占用总是允许
curl -s -X PUT localhost:8000/v1/capacity/gpu-pool -d '{"capacity": 16}'
# => 200 {"configured":true,"capacity":16,"active_weight":...}
```

字段与规则说明：

| 字段 / 规则 | 方向 | 说明 |
| --- | --- | --- |
| `resource_key` | 路径 / 请求 | 容量资源键（1～512 字符，允许 `/`）；多进程按同一资源键共享槽位 |
| `capacity` | 设置请求 / 响应 | 有效占用**权重总量**上限（正整数，最大 10 亿）。容量键无 TTL，整体替换 |
| `weight` | 请求 / 响应 | 本次占用权重（正整数，默认 1，最大 10 亿）；`weight > capacity` 返回 409 |
| `ttl_ms` | 请求 | 占用 / 续期 TTL，区间 `LEASE_MIN_TTL_MS`～`LEASE_MAX_TTL_MS`，缺省 `LEASE_DEFAULT_TTL_MS` |
| `credential` | 响应 / 请求 | 服务端签发的独立凭据（不透明，形如 `<uuid32>:<weight>`）；续期 / 释放原样带回 |
| `expires_at_ms` | 响应 | 到期时间（Redis 服务器时钟，Unix 毫秒） |
| `active_weight` | 响应 | 清理过期占用后当前有效占用权重总量（被拒申请不计入） |
| `remaining` | 响应 | 放行后剩余容量；超限时为不占用情况下的剩余值 |
| `retry_after_ms` | 响应 | 仅 429 时非 0：建议等待毫秒数（至最早一笔占用到期）；响应头同时带 `Retry-After`（秒，向上取整） |
| 失败原因 | 响应 | `not_configured`（未设置容量，409）/ `weight_exceeds_capacity`（权重超容量，409）/ `capacity_exhausted`（容量不足，429）/ `below_active`（下调低于有效占用，409）/ `not_found`（续期凭据错误或已过期，409） |

### 限流示例

```bash
# 窗口 1 秒、成本上限 3、本次成本 1：放行并返回剩余额度
curl -s -X POST localhost:8000/v1/limits/check \
  -H 'Content-Type: application/json' \
  -d '{"client_key": "tenant-42:send-sms", "limit": 3, "window_ms": 1000, "cost": 1}'
# => 200
# {"allowed":true,"client_key":"tenant-42:send-sms","limit":3,"window_ms":1000,
#  "remaining":2,"reset_at_ms":1758...,"retry_after_ms":0}

# 连续打满窗口后再次请求 => 429（不扣减任何成本）
curl -i -X POST localhost:8000/v1/limits/check \
  -H 'Content-Type: application/json' \
  -d '{"client_key": "tenant-42:send-sms", "limit": 3, "window_ms": 1000, "cost": 1}'
# => HTTP/1.1 429 Too Many Requests
#    Retry-After: 1
# {"allowed":false,"client_key":"tenant-42:send-sms","limit":3,"window_ms":1000,
#  "remaining":0,"reset_at_ms":1758...,"retry_after_ms":812}

# cost 可表达加权成本（如批量条数、令牌数）；cost 不得大于 limit，否则 422
curl -s -X POST localhost:8000/v1/limits/check \
  -H 'Content-Type: application/json' \
  -d '{"client_key": "tenant-42:bulk", "limit": 100, "window_ms": 60000, "cost": 30}'
# => 200 {"allowed":true,...,"remaining":70,...}

# 加权拒绝的重试等待：limit=5 的窗口内先后通过成本 1 与 4（已用 5），
# 再请求成本 3 被拒。retry_after_ms 不是"最早一笔（成本 1）滑出"——
# 届时已用 4 仍放不下 3——而是按记录到期顺序累计释放、首次足以容纳
# 本次 cost 的等待毫秒数（成本 1 与 4 都滑出，累计释放 5 >= 3）：
curl -i -X POST localhost:8000/v1/limits/check \
  -H 'Content-Type: application/json' \
  -d '{"client_key": "tenant-42:weighted", "limit": 5, "window_ms": 60000, "cost": 3}'
# => HTTP/1.1 429 Too Many Requests
#    Retry-After: 58
# {"allowed":false,...,"remaining":0,"reset_at_ms":1758...,"retry_after_ms":57980}
# reset_at_ms 仍是"最老一笔滑出"的时刻（语义不变）；retry_after_ms 可能更晚，
# 同毫秒到期的记录合并累计判断。按它等待后重试不会因额度仍不足而再次 429。

# 同一 client_key 交替使用不同 window_ms：成本记录按服务端允许的最大窗口
# （LIMITS_MAX_WINDOW_MS）保留，每次请求只按本次 window_ms 筛选判定。
# 先用 60 秒长窗口打满额度：
curl -s -X POST localhost:8000/v1/limits/check \
  -H 'Content-Type: application/json' \
  -d '{"client_key": "tenant-42:mixed", "limit": 2, "window_ms": 60000, "cost": 2}'
# => 200 {"allowed":true,...,"remaining":0,...}
# 1 秒后用 500ms 短窗口检查：首笔记录已滑出短窗口 -> 放行，
# 且该记录不会被短窗口检查删除：
curl -s -X POST localhost:8000/v1/limits/check \
  -H 'Content-Type: application/json' \
  -d '{"client_key": "tenant-42:mixed", "limit": 2, "window_ms": 500, "cost": 1}'
# => 200 {"allowed":true,...,"remaining":1,...}
# 再切回 60 秒长窗口：两笔记录都在窗内（累计 3 > 2）-> 429，
# remaining 按 0 返回（窗口内累计已超限）：
curl -s -X POST localhost:8000/v1/limits/check \
  -H 'Content-Type: application/json' \
  -d '{"client_key": "tenant-42:mixed", "limit": 2, "window_ms": 60000, "cost": 1}'
# => HTTP/1.1 429 Too Many Requests
# {"allowed":false,...,"remaining":0,"reset_at_ms":1758...,"retry_after_ms":58...}

# request_id 幂等：相同 client_key + request_id 在窗口内只扣减一次。
# 首次请求正常判定（假设通过）：
curl -s -X POST localhost:8000/v1/limits/check \
  -H 'Content-Type: application/json' \
  -d '{"client_key": "tenant-42:send-sms", "request_id": "req-7f3a2c91",
       "limit": 3, "window_ms": 1000, "cost": 1}'
# => 200
# {"allowed":true,...,"request_id":"req-7f3a2c91","idempotent_replay":false,
#  "remaining":2,...}

# 网络超时 / 客户端重试，携带相同 request_id 再发一次：
# 直接回放首次结果，不再扣减（即使窗口额度已被其他请求占满）：
curl -i -X POST localhost:8000/v1/limits/check \
  -H 'Content-Type: application/json' \
  -d '{"client_key": "tenant-42:send-sms", "request_id": "req-7f3a2c91",
       "limit": 3, "window_ms": 1000, "cost": 1}'
# => HTTP/1.1 200 OK
#    Idempotent-Replay: true
# {"allowed":true,...,"request_id":"req-7f3a2c91","idempotent_replay":true,
#  "remaining":2,...}            # 与首次完全一致，窗口内实际只扣了一笔

# 首次判定为 429 时同样会记录：重试得到相同的 429 快照（同 reset_at_ms /
# retry_after_ms），不会因为时间推移或参数变化重新判定；
# 待窗口结束、request_id 自动失效后，同一 request_id 可再次用于新请求。
```

字段说明：

| 字段 | 方向 | 说明 |
| --- | --- | --- |
| `client_key` | 请求 | 限流对象标识（1～512 字符），任意维度（租户 / 用户 / API） |
| `request_id` | 请求 | 可选幂等标识（1～128 字符，仅允许字母、数字、`._:-`）。同一 `client_key` + `request_id` 在首次请求的窗口内只扣减一次，重试 / 并发请求回放首次结果；窗口结束后自动失效、可再次使用。不传时行为与旧版本一致 |
| `limit` | 请求 | 单个窗口内允许累计的成本上限（≥1） |
| `window_ms` | 请求 | 滑动窗口长度（毫秒，服务端限定 1～86400000），同时决定幂等记录的存活时长；同一 `client_key` 可交替使用不同窗口，成本记录按最大允许窗口保留、每次只按本次窗口判定 |
| `cost` | 请求 | 本次成本（≥1，默认 1，且不得大于 `limit`） |
| `request_id` | 响应 | 原样回显请求携带的幂等标识；未携带时为 `null` |
| `idempotent_replay` | 响应 | `true` 表示本次为幂等回放（未重复扣减），响应头同时带 `Idempotent-Replay: true`；默认 `false` |
| `remaining` | 响应 | 放行后窗口内剩余成本；超限时为未扣减情况下的剩余值（混用不同 `window_ms` 致窗口内累计已超限时为 0）；回放时为首次结果快照 |
| `reset_at_ms` | 响应 | 窗口内最老一笔成本滑出的时刻（Redis 服务器时钟，Unix 毫秒）；仅表示最早释放点，不代表届时额度一定够本次请求 |
| `retry_after_ms` | 响应 | 仅 429 时非 0：建议等待毫秒数——按记录到期顺序累计释放的成本首次足以容纳本次 `cost` 的等待时长（同毫秒到期的记录合并判断，可能晚于 `reset_at_ms`）；响应头同时给出标准 `Retry-After`（秒，向上取整） |

## 设计要点

- **原子性**：申请 / 批量申请 / 续租 / 批量续租 / 交接 / 释放 / 校验全部状态
  转换均由 Redis Lua 脚本（`app/lua.py`）在服务端原子执行，不存在"检查后修改"的
  竞态窗口。批量申请在同一脚本内先检查全部任务、再统一写入，任一任务被占用则整体
  失败——不创建任何租约，也不递增任何防栅计数器（全成全败）；过期租约可被接管。
  批量续租在同一脚本内基于单次服务器 TIME 对全部任务逐项预检，任一租约不存在、
  凭据不匹配或已过期则整体失败——不更新任何 TTL（成功项也不续租），并逐项返回
  失败原因；全部有效时一次性更新所有 TTL。
- **交接不续期、旧凭据即失效**：`/v1/leases/transfer` 由单个 Lua 脚本按 Redis
  服务器时间校验"租约仍有效 + token 与防栅序号双凭据匹配"，随后在同一原子操作内
  替换为服务端生成的新 token、防栅计数器 INCR 一次；`expires_at` 与键 TTL 原样
  保留（交接不携带 TTL、不续期）。因此交接后旧 (token, fencing_token) 在续租 /
  批量续租 / 释放 / 校验接口立即失效，新凭据立即可用；租约不存在、已过期或凭据
  不符返回 409（`not_found` / `expired` / `conflict`），且不改变租约、不递增
  防栅计数器。交接可连续进行，防栅序号持续单调递增。
- **过期判断使用 Redis 服务器时间**：Lua 内通过 `TIME` 命令取 Redis 服务器时钟，
  不依赖各客户端本地时间，避免时钟漂移导致的误判。批量脚本只调用一次 `TIME`，
  整批租约的 `expires_at_ms` 基于同一时刻计算。
- **防栅序号单调递增**：每个任务键对应一个独立的 `INCR` 计数器键且**永不过期**，
  即使租约键被 TTL 清除，序号也不会回退或复用。下游系统可用它拒绝迟到的旧操作
  （fencing token 经典用法）。
- **旧持有者隔离**：续租（单个 / 批量）、交接与释放要求令牌和防栅序号**同时**
  匹配；租约过期后不允许续租或交接（必须重新申请）。因此旧持有者在超时后、
  或在交接失去凭据后的任何写操作都不会影响当前租约。
- **双重过期保障**：租约 Hash 内记录 `expires_at`（判断依据），同时设置
  `PEXPIRE`（自动清理残留键）；判断以 `expires_at` 与服务器时间的比较为准。
- **租约审计流**：申请（成功 / 被拒）、续租（单个 / 批量）、交接（成功 / 失败）、
  释放与批量申请的逐任务结果，均在状态转换的同一个 Lua 脚本内通过 `XADD` 写入该
  任务键的审计 Stream——操作与事件原子提交，不会出现"操作成功但事件缺失"。批量
  申请全败时仅被占用的冲突任务各落一条 `conflict`（未冲突任务无任何状态变化，
  与"不创建不递增"一致）；批量续租全败时仅失败任务按原因各落一条（成功项 TTL
  未变，不落账），全成时每项各落一条 `renewed`；交接成功落一条 `transferred`
  （fencing 为递增后的新序号），失败按 not_found / conflict / expired 各落一条
  且不改动租约与防栅计数器。
  事件含操作结果、关联防栅序号、Redis 服务器时间（`time_ms`）与批次大小；
  `XADD MAXLEN n` 在写入同时精确裁剪，每个任务键只保留最新
  `LEASE_EVENT_RETENTION` 条（默认 1000，`0` 为不限制），超出从最旧事件丢弃。
  查询侧以 Stream 条目 ID 为游标用 `XRANGE` 升序翻页，`next_cursor` 为
  `null` 即没有更多事件。

### 任务结果设计要点

- **校验与写入同脚本原子完成**：`/v1/results/commit` 由单个 Lua 脚本基于
  Redis 服务器时间依次完成"租约存在性 → token 与防栅序号双凭据匹配 → 未过期 →
  幂等 / 冲突判定 → 结果写入"，全程不可分割。因此即使提交请求在校验通过的瞬间
  租约恰好到期并被新持有者接管，旧持有者的写入也不可能发生——它会在同一脚本内
  被判定为凭据不符（`conflict`）或已过期（`expired`），不存在"先校验后写"的
  竞态窗口。
- **防栅序号即结果版本，重放 / 冲突按序号判定**：结果 Hash 记录
  `fencing`（提交版本）、`result`（规范化 JSON）与 `committed_at_ms`。
  同一防栅序号只接受一次内容：相同内容重试直接回放首次结果
  （`replayed=true`、响应头 `Result-Replay: true`、不刷新提交时刻、不写数据），
  不同内容返回 409 `result_conflict` 且已存结果不变。新租约意味着更大的防栅
  序号，持有者可正常提交并覆盖为新结果；序号比较由 Redis 单线程串行化，
  并发提交不会交叉覆盖。
- **内容规范化比较**：服务端将结果以排序键、紧凑分隔符的规范化 JSON 字符串
  存放与比较（`json.dumps(..., sort_keys=True, separators=(",", ":"))`），
  因此仅对象键顺序或空白差异的重试视为相同内容；数字与字符串等类型差异仍算不同。
  GET 读回时反序列化为 JSON 返回。
- **结果持久保留**：结果键**不设置任何 TTL**，租约到期、释放、被接管均不删除
  结果；`GET /v1/results/{task_key}` 始终返回最近一次已提交结果。结果键与租约
  Hash 共享 `LEASE_KEY_PREFIX` 前缀（`:result:` 命名空间），互不影响。
- **凭据口径与续租一致**：结果提交的拒绝原因同样是
  `not_found` / `conflict` / `expired`（凭据类 409）外加
  `result_conflict`（同序号内容冲突），所有尝试（含重放与拒绝）都在同一脚本内
  向该任务的审计 Stream 落一条 `result_commit` 事件。
  `POST /v1/results/commit-checked` 与此**共用同一段租约校验 Lua 头部与幂等写入
  Lua 尾部**（`app/lua.py` 中拼装），凭据、防栅、重放、冲突与审计语义逐字一致，
  仅在两者之间增加前置列表 / 版本核对，失败时多一种
  `stale_dependencies`（409）原因。
- **全局结果变更流，与结果写入同脚本原子追加**：普通提交与受保护提交仅在首次
  写入新结果版本（幂等写入尾部的覆盖提交分支）时，于 `HSET` 结果 Hash 的同一
  Lua 脚本内 `XADD` 一条变更到所有任务键共用的全局 Stream
  （`{prefix}:result-changes`），字段为 `task_key` / `fencing` /
  `committed_at_ms` / `result`；条目 ID 由 Redis 服务器时间生成，跨任务键按写入
  全局定序。失败分支（凭据 / `result_conflict` / `stale_dependencies`）与相同
  内容重放分支均在到达该 `XADD` 之前返回，**绝不追加**，故每条变更恰好对应一次
  结果版本推进，重试不会产生重复消息。`XADD MAXLEN n` 同时把全局流裁剪为最新
  `LEASE_RESULT_CHANGE_RETENTION` 条（默认 10000，`0` 不限制）。读取侧
  `GET /v1/results/changes` 由**单个只读 Lua 脚本**在同一次 Redis 原子执行内
  完成"最早保留条目校验 → 按 `(cursor, limit]` 读取页面 → 恰满一页时探测后续
  条目决定 `next_cursor`"：三步共用同一份流快照，并发提交触发的 MAXLEN 裁剪
  不可能落在校验与读取之间，失效游标不会得到 200 而静默漏读。不传游标从当前
  最早保留条目起读（空流返回空页），游标早于最早保留条目时返回 **410**，
  非法游标 400、`limit` 越界 422。

### 任务依赖门禁设计要点

- **保存：去重 + 16 上限 + 禁止自身依赖**：`POST /v1/dependencies/{task_key}`
  对请求列表按首次出现顺序去重，去重后最多 16 个前置键（超限 `422`）；任务键
  出现在依赖中即自身依赖，返回 `422`。定义存为 Redis List
  `{prefix}:deps:{task_key}`，整体替换语义；空列表即 `DEL` 清除。
- **环检测只遍历"现有"定义**：保存 `T -> [d...]` 时，Lua 从每个前置键 `d`
  出发沿**当前已落库**的依赖边做可达性判断——若能回到 `T`，写入后必成环
  （`cycle`，409）。检测全程在单个 Lua 脚本内、在任何写入之前完成，因此"检测 →
  拒绝"不会与并发的其它保存产生半写状态；通过后才在同一脚本内 `DEL + RPUSH`
  原子替换。
- **1024 节点硬上限，拒绝保留原定义**：跨多个前置起点共享同一"去重访问集合"，
  去重后访问的现有节点数最多 **1024**，超过即判 `too_many_nodes`（409）并立刻
  停止。环与超限都在写入前返回，被拒时旧定义**原样保留**，响应体 `dependencies`
  回带当前生效定义，便于调用方核对。
- **ready 只看直接前置的持久结果**：`GET .../ready` 读取本任务的依赖 List，
  逐个检查对应结果键 `{prefix}:result:{dep}` 是否存在（与
  `GET /v1/results/{task_key}` 同源；结果无 TTL，提交即"已完成"）。
  全部存在 -> `ready=true`；否则 `ready=false` 且 `pending` 按定义顺序列出
  未完成键。判定**不递归**间接依赖，且**没有定义的任务恒为可执行**。
- **版本快照单脚本读取**：`GET .../snapshot` 在单个只读 Lua 脚本内读取依赖
  List 与每个前置结果 Hash 的 `fencing`，返回按定义顺序排列的
  `{task_key, fencing_token}`；任一前置结果键不存在即返回 `pending`（不给出
  可提交快照）。读取全程不可分割，快照反映某一确定时刻的"定义 + 结果版本"。
- **受保护提交单脚本核对**：`POST /v1/results/commit-checked` 先在同一 Lua
  脚本内完成与普通提交相同的租约校验，再比对当前依赖 List 与快照的数量、顺序、
  键名，以及每个前置结果 Hash 的当前 `fencing` 与快照版本；任一不符返回
  **409 `stale_dependencies`** 且不进入写入、不改结果，并在该任务审计流落
  `result_commit / stale_dependencies` 事件。全部一致才执行与普通提交共享的
  防栅 / 幂等写入段（首次写入、相同内容重放、同序号内容冲突语义不变）。
  **无前置任务（无定义或空定义）使用空快照 `snapshot=[]`**，前置核对直接通过。
- **沿用现有前缀**：依赖定义与租约 Hash、防栅计数器、审计 Stream、结果 Hash
  共享 `LEASE_KEY_PREFIX`，独立使用 `:deps:` 命名空间，互不影响；环检测与
  快照 / 受保护提交在 Lua 内以前缀拼出相邻依赖 / 结果键读取，无需把全图键都
  声明进 `KEYS`（受保护任务自身的依赖 List 仍作为真实 `KEYS` 声明）。

### 并发容量槽位设计要点

- **清旧与汇总核算同脚本原子完成，绝不超配**：每次申请由单个 Lua 脚本先按
  Redis 服务器时间清理到期占用，再依据**已占用权重汇总**判定
  `汇总已用 + 本次权重 <= 容量` 才 `ZADD` 写入。Redis 单线程串行执行脚本，
  同一资源键的并发申请严格串行，**并发绝不超配**；容量不足时不写入任何占用，
  返回 429 与"最早一笔占用到期"的 `retry_after_ms`。
- **已占用权重汇总随每次操作增量维护，常规请求不扫描全部占用**：汇总保存在
  独立 String 键 `{prefix}:slots-used:{resource_key}`，四个容量脚本在同一
  Lua 操作内联动维护——清旧时先 `ZRANGE ... BYSCORE` 取出到期 member、逐笔
  解析凭据尾部权重后统一 `ZREMRANGEBYSCORE` 删除并一次性扣减汇总；申请成功
  `INCRBY` 加本次权重；释放 member 时按该笔权重扣减；续期权重不变。因此
  常规申请 / 续期 / 释放读取汇总即可核算剩余，时间复杂度与有效占用笔数无关。
  首次遇到**已有占用集合但缺少汇总**（升级前写入的数据）或汇总损坏时，脚本
  在当次调用内遍历集合**重建一次**汇总，兼容升级；重建之后即回归增量路径。
  汇总与占用集合同生共灭：集合清空（最后一笔释放 / 到期被清理）时两个键一起
  `DEL`，非空时设置相同的兜底 `PEXPIRE`——集合自然过期后汇总同步消失，
  **不会残留旧计数阻挡后续申请**。
- **每占用独立凭据，续期 / 释放只认凭据**：凭据由服务端对每次申请生成
  （`<uuid32>:<weight>`，即占用 ZSet 的 member，权重编码在尾部），故每次申请
  天然独立、不可能碰撞。续期仅当凭据对应的 member 仍存在才把到期时刻刷新为
  `now + ttl`；释放仅当 member 仍存在才删除。凭据错误或占用已过期（已被清旧
  删除）时，续期返回 409 `not_found`、释放幂等忽略，**不触碰任何其它占用**，
  因此过期占用或旧凭据的续期 / 释放不可能影响过期后新申请得到的新占用。
- **服务器时间 + 双重过期回收**：到期判定一律使用 Lua 内 `TIME` 的 Redis
  服务器时钟，不依赖客户端时钟；占用 score 即到期毫秒。除每次操作即时清旧外，
  占用集合非空时还按 `(最大到期 - now + 1)` 给集合键与汇总键设置相同的
  `PEXPIRE` 兜底回收无流量残留键（比较口径比 score 判定多 1ms，不会提前
  删除），集合为空立即同时 `DEL` 两个键。是否有效始终以 score 与服务器时间
  比较为准。
- **容量调整不得低于当前有效占用，拒绝保留原配置**：设置容量在同一脚本内先
  清旧（同步扣减汇总）、读取汇总核算当前有效占用权重，新容量更低时直接返回
  409 `below_active`，**不写容量键、不动任何占用与汇总**，响应回带当前仍
  生效的旧容量与有效占用量；调高或设为不低于有效占用的值才原子整体替换。
  容量键 `SET` 无 TTL，未配置的资源键申请一律 409 `not_configured`。
- **加权语义**：容量与申请都以整数权重计（默认权重 1，可表达批量条数 /
  GPU 卡数等），剩余 = 容量 − 有效占用权重和；单次 `weight > capacity` 永远
  无法满足，按 409 `weight_exceeds_capacity`（不可重试）区别于可等待恢复的
  429 `capacity_exhausted`。
- **沿用现有前缀与 TTL 配置**：容量、占用与汇总键沿用 `LEASE_KEY_PREFIX`
  （`:capacity:` / `:slots:` / `:slots-used:` 独立命名空间），申请 / 续期的
  TTL 范围与缺省值复用 `LEASE_MIN_TTL_MS` / `LEASE_MAX_TTL_MS` /
  `LEASE_DEFAULT_TTL_MS`，不引入新的必配项。

### 限流设计要点

- **原子滑动窗口**：每次检查由单个 Lua 脚本依次完成
  `ZREMRANGEBYSCORE` 清旧（仅删除滑出最大允许窗口的记录）→ 按本次
  `window_ms` 筛出在窗记录并累计成本 → 判定 → `ZADD` 扣减，
  Redis 单线程执行脚本，同一 `client_key` 的并发请求严格串行判定，
  不会出现"检查后扣减"的竞态，**并发请求绝不超额**（超限时不写入任何成本）。
- **成本加权**：每笔请求携带整数 `cost`（批量条数、令牌数等），
  member 形如 `<uuid>:<cost>`，cost 编码在 member 尾部由脚本解析累计；
  uuid 保证同一毫秒并发请求的 member 不冲突。
- **同一 client_key 可混用不同 window_ms**：成本记录按**最大允许窗口**
  （`LIMITS_MAX_WINDOW_MS`）保留——清旧只删除连最大允许窗口都已滑出的
  记录，键的兜底 `PEXPIRE` 也按最大允许窗口设置；每次请求只按本次
  `window_ms` 筛选（`score > now - window_ms`）并累计成本、计算
  `reset_at_ms` 与 `retry_after_ms`，窗口外被保留的记录不参与本次判定。
  因此短窗口检查不会删除仍属于更长窗口的记录，切回长窗口后旧记录仍被
  正确计入；混用不同窗口导致窗口内累计已超限时，`remaining` 按 0 返回。
- **Redis 服务器时间**：窗口边界由 Lua 内 `TIME` 命令的服务器时钟确定，
  不依赖调用方本地时间；`reset_at_ms` / `retry_after_ms` 均基于该时钟计算。
- **过期数据自动清理**：滑出**最大允许窗口**（而非本次窗口）的记录在下次
  检查时被 `ZREMRANGEBYSCORE` 即时删除；键的兜底 `PEXPIRE` 同样按最大
  允许窗口设置，无流量时键自动消失——此时最新记录对任何合法窗口都已
  滑出，清理与 TTL 都不会提前删除仍可被后续长窗口请求计入的记录。
- **超限语义**：超限时返回 `429`，响应体给出 `remaining`、`reset_at_ms`
  （最老一笔成本滑出的时刻，语义不变）与 `retry_after_ms`，并携带标准
  `Retry-After` 头（秒，向上取整）。`retry_after_ms` 按记录到期顺序累计
  将释放的成本，取**首次足以容纳本次 `cost`** 的等待毫秒数——同毫秒到期
  的记录在同一时刻一起滑出，合并累计判断；因此加权场景下它可能晚于
  `reset_at_ms`（最老一笔滑出未必释放足够额度，如窗口内 1+4、limit=5、
  本次 cost=3 时须等到成本 4 那笔也滑出），按它重试不会因额度仍不足而
  再次 429。
- **request_id 幂等**：携带 `request_id` 时，"幂等命中判定 → 清旧 → 累计 →
  判定 → 扣减 → 记录完整响应"在**同一个 Lua 脚本**内完成，因此相同
  `client_key` + `request_id` 的并发请求也只有首个完成判定与扣减，其余直接
  回放首次结果（200 或 429 均会记录），回放响应带 `idempotent_replay=true`
  与 `Idempotent-Replay: true` 响应头，且**不刷新任何 TTL**。幂等记录的 TTL
  等于首次请求的 `window_ms`，窗口结束后自动删除——`request_id` 仅在对应窗口
  内存活，跨窗口可再次使用。重试请求以首次结果为准，不重新判定额度或参数
  （`limit` / `window_ms` 也回放首次值），发起新的逻辑请求请使用新的
  `request_id`。幂等键以 `client_key` 长度定界，不同 `client_key` 间不可能
  碰撞；`request_id` 仅允许字母、数字与 `._:-`，不携带 `request_id` 的请求
  行为与旧版本完全一致。

### Redis 数据模型

| 键 | 类型 | 说明 |
| --- | --- | --- |
| `{prefix}:lease:{task_key}` | Hash（`token` / `fencing` / `expires_at`），带 PEXPIRE | 当前租约 |
| `{prefix}:fencing:{task_key}` | String 计数器，无 TTL | 防栅序号，只增不减 |
| `{prefix}:events:{task_key}` | Stream，条目含 `op` / `result` / `fencing` / `time_ms` / `batch_size`，`XADD MAXLEN` 裁剪 | 租约审计事件，按任务键分组，保留最新 `LEASE_EVENT_RETENTION` 条 |
| `{prefix}:result:{task_key}` | Hash（`result` / `fencing` / `committed_at_ms`），**无 TTL** | 最近一次已提交任务结果及其防栅序号，不随租约到期删除 |
| `{prefix}:result-changes` | Stream（所有任务键共用），条目含 `task_key` / `fencing` / `committed_at_ms` / `result`，`XADD MAXLEN` 裁剪 | 全局任务结果变更流：仅首次写入新结果版本时于提交同一 Lua 脚本内原子追加，按写入全局定序，保留最新 `LEASE_RESULT_CHANGE_RETENTION` 条 |
| `{prefix}:deps:{task_key}` | List（元素为前置任务键，去重、最多 16 个，按请求顺序；键不存在即无定义） | 任务依赖门禁定义；保存前 Lua 沿现有 List 做环检测（去重访问 ≤1024），拒绝保留原定义 |
| `{limit_prefix}:{client_key}` | ZSet，score=请求毫秒时间戳，member=`<uuid>:<cost>`，PEXPIRE=最大允许窗口 | 最大允许窗口（`LIMITS_MAX_WINDOW_MS`）内的各笔成本；每次请求只按本次 `window_ms` 筛选计入，清旧与键 TTL 均按最大允许窗口计算，短窗口请求不会提前删除长窗口仍需要的记录 |
| `{idem_prefix}:<len>:{client_key}:{request_id}` | Hash（`allowed` / `remaining` / `reset_at` / `retry_after` / `limit` / `window`），PEXPIRE=首次 `window_ms` | request_id 首次判定的完整响应；窗口内回放、窗口结束自动删除 |
| `{prefix}:capacity:{resource_key}` | String（整数权重容量），**无 TTL** | 资源键的并发容量配置；未配置即不可申请；下调低于有效占用时拒绝、保留原值 |
| `{prefix}:slots:{resource_key}` | ZSet，member=凭据 `<uuid32>:<weight>`，score=到期毫秒，带兜底 PEXPIRE | 资源键当前有效占用；凭据即 member，续期 / 释放只认完整凭据 |
| `{prefix}:slots-used:{resource_key}` | String（整数权重和），与占用集合相同的兜底 PEXPIRE | 已占用权重汇总：清旧逐笔扣减、申请 / 释放同步增减，常规请求读取它核算剩余容量，不再遍历全部占用；有集合无汇总（升级前数据）时脚本内一次性重建，集合清空 / 过期时随集合一并 DEL 失效 |

## 冒烟测试

服务启动后执行（仅依赖 Python 标准库）：

```bash
python3 scripts/smoke_test.py            # 默认打 http://localhost:8000
```

覆盖：获取冲突、错误令牌/序号被拒绝、防栅序号单调递增、旧持有者超时后无法
续租/释放/干扰新租约等场景；**租约交接：换新凭据且防栅序号递增、保留原过期
时刻不续期、旧凭据在续租/释放/校验立即失效而新凭据立即可用、连续交接、
not_found / conflict / expired 三类 409 不改变租约与防栅序号且逐因落审计
事件**；以及批量申请的全成全败、冲突时不写不递增、
过期接管与凭据兼容；以及批量续租的全成全败、逐项失败原因（not_found /
conflict / expired）、失败批次不改变任何 TTL、成功 / 失败结果逐任务写入
审计 Stream；以及租约审计流：四类操作（含被拒 / 忽略）均按任务键
落账、事件字段（op/result/fencing/time_ms/batch_size）正确、游标分页
（cursor + next_cursor 翻页不重不漏）、非法 cursor 返回 400 / limit
越界返回 422、按 `LEASE_EVENT_RETENTION` 裁剪最旧事件；以及**任务结果提交**：
仅有效租约凭据可写入、相同内容重放（`replayed` / `Result-Replay` 头）、
同序号不同内容 409 `result_conflict` 且不改结果、旧持有者过期 / 被接管后无法
覆盖、新租约可提交新结果、结果不随租约到期删除、并发相同内容恰好一次提交、
提交尝试逐次落审计事件；以及**全局任务结果变更流**：仅首次写入新结果版本
（普通提交 / commit-checked）追加变更、失败与相同内容重放不追加、变更含
task_key / 防栅序号 / 提交时间 / 当次 JSON、跨任务键按写入全局定序、游标分页
不重不漏、早于最早保留条目的游标 410、非法 cursor 400 / limit 越界 422、
路由优先于 `{task_key}` 捕获，以及**游标裁剪竞态**：恰满一页（末页
`next_cursor=null`、满页可继续翻页、按写入顺序衔接不重复）、小保留窗口下并发
`XADD MAXLEN` 持续裁剪时任何 200 页面都与读取前快照逐条衔接且失效游标仍
返回 410（不静默漏读）、空流（含带游标）空页 200；以及限流的剩余额度递减、成本加权、超限 429
（含 `Retry-After` 头与不扣减语义）、窗口滑动后恢复、**20 并发恰好放行
limit 笔不超额**、参数校验和过期数据自动清理；以及**加权拒绝的重试等待**：
limit=5 窗口内先后通过成本 1 与 4 后成本 3 被拒，`retry_after_ms` 按记录
到期顺序累计释放、首次足以容纳本次成本（等到成本 4 滑出而非仅最早的成本 1，
`reset_at_ms` 仍为最老记录滑出），按旧等待时长重试仍 429、等到位后放行，
同毫秒到期记录合并判断（直连 Redis 构造同分记录验证）；以及**不同
window_ms 混用**：长窗口放入的记录不被随后的短窗口检查删除、切回长窗口
仍被正确计入并 429（窗口内累计超限时 `remaining` 按 0 返回、`reset_at_ms`
仍跟踪最老记录）、最老记录滑出长窗口后恢复放行；以及 `request_id` 幂等：
重试回放首次结果且不重复扣减、回放响应带 `Idempotent-Replay` 头、
**20 个携带相同 request_id 的并发请求只扣减一次**、429 结果原样回放、
`request_id` 跨窗口可再次使用、幂等作用域按 `client_key` 隔离、
非法 `request_id` 返回 422；以及**任务依赖门禁**：无定义即可执行、
保存按首次出现顺序去重且去重后限 16 个、自身依赖 422、
环检测只遍历现有定义（成环 409 `cycle`）、单次去重访问节点上限 1024
（超限 409 `too_many_nodes`）且环 / 超限都保留原定义、空列表清除定义、
ready 依据持久任务结果判定直接前置并按顺序返回未完成键；以及**前置结果版本
保护**：snapshot 在单个只读 Lua 中读取当前直接前置列表及各结果版本
（pending 不出快照、无定义空快照、版本与 GET results 同源、按定义顺序），
commit-checked 在同一 Lua 操作内核租约 + 前置列表 + 各结果版本，
全部一致才按普通提交的防栅与幂等语义写入，版本漂移 / 定义变更 / 乱序 /
伪造版本均 409 `stale_dependencies` 且不改结果，重新拉快照后成功，
空快照（含缺省字段）等价普通提交，快照参数 422，并发同内容恰好一次提交，
拒绝尝试逐次落审计事件；以及**按资源键分配并发容量**：未配置即申请 409、
加权申请剩余递减、容量不足 429（带 `Retry-After`、不占用）、权重超容量 409、
下调低于有效占用 409 且保留原配置、续期 / 释放仅认凭据（错误 / 过期无效、
旧凭据不影响新占用、释放幂等）、短 TTL 过期自动回收且续期可延长、
参数校验，以及 40 并发（含加权并发）恰好放行容量笔、绝不超配；以及**已占用
权重汇总**：升级前遗留的"有占用集合、缺汇总"数据在首次操作时于脚本内一次性
重建（占用核算不受影响）、到期清理逐笔扣减汇总、集合自然过期 / 清空后汇总
同步失效且不被旧计数阻挡（直连 Redis 构造升级前数据布局验证）。

## 本地开发（不用 Docker）

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# 需要本地或远程有可用的 Redis，并通过 REDIS_URL 指向它
uvicorn app.main:app --reload --port 8000
```

## 项目结构

```
app/
  config.py    # 环境变量配置（pydantic-settings，含审计流与全局结果变更流保留条数；容量占用 TTL 复用租约 TTL 配置）
  lua.py       # 申请/批量申请/续租/批量续租/交接/释放/校验七个租约 Lua 脚本（租约脚本内原子写审计 Stream）+ 租约凭据原子校验的结果提交脚本（普通提交与 commit-checked 共用同一租约校验头 / 幂等写入尾；首次写入新结果版本时同脚本原子追加全局结果变更流）+ 全局结果变更流分页只读 Lua 脚本（最早条目校验 / 按游标读取页面 / next_cursor 探测单次原子完成，失效游标 410、并发裁剪不静默漏读）+ 前置版本快照与受保护提交 Lua 脚本 + 滑动窗口限流（含 request_id 幂等）Lua 脚本 + 依赖保存（现有定义环检测 / 1024 节点上限）Lua 脚本 + 按资源键分配并发容量四个 Lua 脚本（服务器时间清旧、已占用权重汇总增量维护与一次性重建、独立凭据续期 / 释放、下调容量保护）
  schemas.py   # 请求 / 响应模型（含审计事件分页、任务结果与全局结果变更分页模型、任务依赖门禁与前置版本快照模型、并发容量槽位模型）
  main.py      # FastAPI 应用、路由、Redis 生命周期（含 /events 游标分页、任务结果提交 / 读取、/v1/results/changes 全局变更游标分页（单 Lua 原子读取：410 过期游标，并发裁剪不静默漏读）、依赖保存 / ready / snapshot 门禁、commit-checked 受保护提交、按资源键的容量设置与加权槽位申请 / 续期 / 释放）
scripts/
  smoke_test.py
Dockerfile
docker-compose.yml
requirements.txt
```
