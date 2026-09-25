"""租约状态机与滑动窗口限流的全部写/判操作都在 Redis Lua 中原子完成。

租约约定：
- 租约键（KEYS[1] 或 lease key）是一个 Hash：
    token       所有权令牌（UUID）
    fencing     防栅序号（单调递增）
    expires_at  过期时间，Unix 毫秒，基于 Redis 服务器时钟
- 防栅计数器键独立存放、永不过期，保证同一任务键的 fencing 只增不减。
- 过期判断一律使用 redis.call('TIME')，即 Redis 服务器时间，
  避免各客户端时钟不一致导致的问题。

租约审计约定：
- 每个任务键对应一个 Redis Stream（事件流键），申请（成功 / 被拒）、
  续租（单个 / 批量）、交接（成功 / 失败）、释放以及批量申请的逐任务结果
  都在状态转换的同一脚本内 XADD 落账，与状态转换原子提交，不可能出现
  "操作成功但事件丢失"。
- 条目字段：op（acquire / acquire_batch / renew / renew_batch /
  transfer / release / result_commit）、result（操作结果；交接成功为
  transferred，失败为 not_found / conflict / expired；结果提交为
  committed / replayed / result_conflict / stale_dependencies）、
  fencing（相关防栅序号，无则空串）、
  time_ms（Redis 服务器毫秒，与状态判定同一时钟）、
  batch_size（仅批量申请 / 批量续租携带）。
- retention > 0 时以 XADD MAXLEN 精确裁剪，每个流只保留最新的 retention 条，
  超出部分从最旧事件开始丢弃；retention = 0 表示不限制。

限流约定：
- 限流键是一个 ZSet：score 为请求时刻（Redis 服务器毫秒），
  member 形如 "<uuid>:<cost>"，cost 编码在 member 尾部，
  uuid 保证同毫秒并发请求的 member 不冲突。
- 同一 client_key 可交替使用不同的 window_ms：ZSet 保留"最大允许窗口"
  （LIMITS_MAX_WINDOW_MS）内的全部成本记录，每次请求只按本次 window_ms
  筛选（score > now - window_ms）并累计成本、计算 reset_at_ms 与
  retry_after_ms；过期清理（ZREMRANGEBYSCORE）与键的兜底 PEXPIRE 都按
  最大允许窗口计算，短窗口请求不会删除 / 提前过期仍属于更长窗口的记录，
  切回长窗口后旧记录仍被正确计入。
- 拒绝时的 retry_after_ms 不是"最老一笔滑出"：按记录到期顺序累计
  将释放的成本（同毫秒到期的记录同一时刻一起滑出，合并累计判断），
  取首次足以容纳本次 cost 的等待毫秒数，避免过早重试再次 429；
  reset_at_ms 仍是最老一笔滑出的时刻，两者语义各自独立。
- 幂等键（可选 request_id）是一个 Hash，保存首次判定的完整响应，
  TTL 等于首次请求的 window_ms：窗口内同键请求直接回放首次结果、
  不重复扣减；窗口结束后 Hash 自动删除，request_id 可再次使用。
- 窗口判断、累计、扣减与幂等判定全部基于 redis.call('TIME') 的
  服务器时间，单脚本原子执行，并发请求不会超额、不会重复扣减。

任务依赖约定：
- 每个任务键的前置依赖定义是一个 Redis List：
  {prefix}:deps:{task_key}，元素即前置任务键（请求顺序，已去重，
  最多 16 个）；未定义（键不存在）表示该任务没有门禁、随时可执行。
- 保存定义的环检测只遍历"现有"定义：从本次提交的每个前置键出发，
  沿现有依赖边能否回到本任务键；去重后访问节点数上限 1024。
  发现环或访问超限即拒绝，且发生在任何写入之前，原定义原样保留；
  通过后 DEL + RPUSH 原子替换（空列表即删除定义）。
- 就绪判定（GET ready）只检查直接前置任务键是否已提交持久结果
  （{prefix}:result:{task_key} 是否存在）：全部存在则 ready=true，
  否则 ready=false 并返回未完成的前置键；结果不设 TTL，提交即完成。
- 版本快照（GET snapshot）在单个只读脚本内一并读取依赖 List 与各前置
  结果 Hash 的 fencing 版本，按定义顺序返回一致快照；有前置未提交结果时
  返回 pending（不可提交）；无定义返回空快照。
- 受保护提交（POST /v1/results/commit-checked）在单个脚本内先做与普通
  结果提交相同的租约校验，再核对当前前置列表与各结果版本是否与快照一致：
  全部一致才沿用防栅与幂等语义写入；任何不一致返回 stale_dependencies
  （409）且不改结果。

任务结果变更流约定（全局订阅）：
- 所有任务键共用一条全局 Redis Stream：{prefix}:result-changes。
  普通结果提交与携带前置快照的受保护提交，在**首次写入新结果版本**（即结果
  Hash 覆盖写入、返回 committed）的同一个 Lua 脚本内，与结果写入原子地向该
  全局流 XADD 一条变更，条目 ID 按 Redis 服务器时间生成，故跨任务键严格按
  写入全局定序，供下游顺序消费。
- 变更字段：task_key（任务键）、fencing（该结果的防栅序号）、
  committed_at_ms（提交时间，Redis 服务器毫秒）、result（当次规范化
  JSON 结果）。
- 提交失败（凭据类 not_found / conflict / expired、内容冲突
  result_conflict、前置过期 stale_dependencies）以及相同内容重放
  （replayed）都**不追加**变更；只有真正写入新结果版本才追加，
  因此一条变更恰好对应结果 Hash 的一次版本推进。
- retention > 0 时以 XADD MAXLEN 精确裁剪，全局流只保留最新的 retention 条，
  超出部分从最旧变更丢弃；retention = 0 表示不限制。
- 分页读取（GET /v1/results/changes）由单个只读 Lua 脚本在同一次 Redis
  原子执行内完成"最早保留条目校验（游标更早即 410）-> 按 (cursor, limit]
  读取页面 -> 恰满一页时探测后续条目决定 next_cursor"：三步共用同一份流
  快照，并发提交触发的 MAXLEN 裁剪不可能落在校验与读取之间，失效游标不会
  得到 200 而静默漏读。

按资源键分配并发容量约定（多工作进程共享有上限的处理槽位）：
- 容量配置键 {prefix}:capacity:{resource_key} 是一个 String（整数权重容量，
  无 TTL；未配置即未启用，不能申请）。
- 占用集合键 {prefix}:slots:{resource_key} 是一个 ZSet：
    member 即凭据（credential），形如 "<uuid32>:<weight>"，
      weight 编码在 member 尾部（与限流 member 同一约定）；
    score 为该占用的到期时刻（Redis 服务器毫秒）。
  凭据由服务端生成（uuid + 本次权重），每次申请独立、不可能碰撞。
- 已占用权重汇总键 {prefix}:slots-used:{resource_key} 是一个 String
  （整数权重和），由四个容量脚本在同一 Lua 操作内维护：
    * 清理到期占用时先用 ZRANGE 取出到期 member、逐笔解析权重后再
      ZREMRANGEBYSCORE 删除，并按到期权重和一次性扣减汇总（KEEPTTL
      保留汇总原有 TTL）；
    * 申请写入时 INCRBY 加本次权重；释放 member 时按该笔权重重新写入扣减后的值。
  因此常规申请 / 续期 / 释放只需读取汇总即可核算剩余容量，不再遍历
  占用集合下的全部有效 member。
- 首次遇到"占用集合非空但缺少汇总"（升级前写入的数据）时，脚本在同一次
  调用内遍历一次重建汇总，兼容升级；汇总损坏（非整数或为负）同样重建。
- 占用集合自然过期（兜底 PEXPIRE 到期）或清空时，汇总键与集合键同时
  DEL（非空时二者设置相同的兜底 PEXPIRE），汇总不会在集合消失后残留，
  避免后续申请被旧计数阻挡。
- 续期与释放都以凭据（完整 ZSet member）为唯一凭证：member 不存在即 not_found
  （从未签发或已过期清理），不做任何修改；故过期占用或旧凭据的续期 / 释放
  不可能影响过期后新申请得到的新占用（新占用是全新的 member）。
- 是否有效一律以 score 与服务器时间比较为准，PEXPIRE 仅回收无流量残留键
  （比较口径比 score 判定多 1ms，不会提前删除），与租约的
  expires_at/PEXPIRE 双重保障同构。
"""

# 租约审计公共函数：拼接到每个需要落账的租约脚本前。
# 条目 ID 由 XADD '*' 按 Redis 服务器时间生成（毫秒-序号），
# 同时在字段里显式记录 time_ms，便于消费方直接读取。
# retention > 0 时精确保留最新 retention 条（裁剪最旧事件）；= 0 不裁剪。
_AUDIT_LUA = """
local function audit_add(stream, retention, fields)
  local keep = tonumber(retention)
  if keep and keep > 0 then
    return redis.call('XADD', stream, 'MAXLEN', keep, '*', unpack(fields))
  end
  return redis.call('XADD', stream, '*', unpack(fields))
end
"""

# 申请租约：仅当租约不存在或已过期时成功。
# KEYS[1] = 租约键, KEYS[2] = 防栅计数器键, KEYS[3] = 审计事件流键
# ARGV[1] = 新令牌, ARGV[2] = ttl_ms, ARGV[3] = 事件保留条数
# 返回 {是否成功(1/0), fencing, expires_at}
ACQUIRE_LUA = _AUDIT_LUA + """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local ttl = tonumber(ARGV[2])
local retention = ARGV[3]

local expires_at = redis.call('HGET', KEYS[1], 'expires_at')
if expires_at and tonumber(expires_at) > now then
  local fencing = tonumber(redis.call('HGET', KEYS[1], 'fencing'))
  audit_add(KEYS[3], retention,
    {'op', 'acquire', 'result', 'conflict',
     'fencing', tostring(fencing), 'time_ms', tostring(now)})
  return {0, fencing, tonumber(expires_at)}
end

local fencing = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1],
  'token', ARGV[1],
  'fencing', fencing,
  'expires_at', now + ttl)
redis.call('PEXPIRE', KEYS[1], ttl)
audit_add(KEYS[3], retention,
  {'op', 'acquire', 'result', 'acquired',
   'fencing', tostring(fencing), 'time_ms', tostring(now)})
return {1, fencing, now + ttl}
"""

# 批量申请（2～32 个任务键，统一 TTL），全成全败：
# 先统一检查所有任务，任一仍被有效租约占用即整体失败，
# 不创建任何租约、不递增任何防栅计数器；
# 全部可用（不存在或已过期，过期可被接管）时再统一写入。
# 全程只调用一次 TIME，所有成功租约的 expires_at 基于同一基准时刻。
# 审计事件逐任务写入各自的事件流：
#   成功 -> 每个任务一条 acquired；冲突 -> 仅每个被占用任务一条 conflict
#   （未冲突任务在失败批次中没有任何状态变化，与"不创建不递增"一致）。
# KEYS[1..n]       = 租约键
# KEYS[n+1..2n]    = 防栅计数器键（与租约键同序）
# KEYS[2n+1..3n]   = 审计事件流键（与租约键同序）
# ARGV[1]          = 任务数 n
# ARGV[2..n+1]     = 各任务的新令牌（与输入同序）
# ARGV[n+2]        = ttl_ms
# ARGV[n+3]        = 事件保留条数
# 成功返回 {1, fencing_1, expires_1, ..., fencing_n, expires_n}
# 冲突返回 {0, 冲突下标_1, ...}（1 基下标，按输入顺序）
ACQUIRE_BATCH_LUA = _AUDIT_LUA + """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local n = tonumber(ARGV[1])
local ttl = tonumber(ARGV[n + 2])
local retention = ARGV[n + 3]

local conflicts = {}
for i = 1, n do
  local expires_at = redis.call('HGET', KEYS[i], 'expires_at')
  if expires_at and tonumber(expires_at) > now then
    conflicts[#conflicts + 1] = i
  end
end
if #conflicts > 0 then
  for _, i in ipairs(conflicts) do
    local fencing = tonumber(redis.call('HGET', KEYS[i], 'fencing'))
    audit_add(KEYS[2 * n + i], retention,
      {'op', 'acquire_batch', 'result', 'conflict',
       'fencing', tostring(fencing), 'time_ms', tostring(now),
       'batch_size', tostring(n)})
  end
  local reply = {0}
  for _, i in ipairs(conflicts) do
    reply[#reply + 1] = i
  end
  return reply
end

local expires = now + ttl
local reply = {1}
for i = 1, n do
  local fencing = redis.call('INCR', KEYS[n + i])
  redis.call('HSET', KEYS[i],
    'token', ARGV[i + 1],
    'fencing', fencing,
    'expires_at', expires)
  redis.call('PEXPIRE', KEYS[i], ttl)
  audit_add(KEYS[2 * n + i], retention,
    {'op', 'acquire_batch', 'result', 'acquired',
     'fencing', tostring(fencing), 'time_ms', tostring(now),
     'batch_size', tostring(n)})
  reply[#reply + 1] = fencing
  reply[#reply + 1] = expires
end
return reply
"""

# 续租：必须同时匹配令牌与防栅序号，且租约尚未过期。
# 已过期的租约不允许续租（持有者必须重新申请），
# 防止旧持有者在超时后"复活"自己的租约。
# KEYS[1] = 租约键, KEYS[2] = 审计事件流键
# ARGV[1] = 令牌, ARGV[2] = fencing, ARGV[3] = ttl_ms
# ARGV[4] = 事件保留条数
# 返回 {是否成功(1/0), 原因, 新 expires_at}
RENEW_LUA = _AUDIT_LUA + """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local retention = ARGV[4]

local token = redis.call('HGET', KEYS[1], 'token')
if not token then
  audit_add(KEYS[2], retention,
    {'op', 'renew', 'result', 'not_found',
     'fencing', '', 'time_ms', tostring(now)})
  return {0, 'not_found', 0}
end
local current_fencing = redis.call('HGET', KEYS[1], 'fencing')
if token ~= ARGV[1] or current_fencing ~= ARGV[2] then
  local f = tonumber(current_fencing)
  audit_add(KEYS[2], retention,
    {'op', 'renew', 'result', 'conflict',
     'fencing', f and tostring(f) or '', 'time_ms', tostring(now)})
  return {0, 'conflict', 0}
end

local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at'))
if expires_at <= now then
  audit_add(KEYS[2], retention,
    {'op', 'renew', 'result', 'expired',
     'fencing', current_fencing, 'time_ms', tostring(now)})
  return {0, 'expired', 0}
end

local ttl = tonumber(ARGV[3])
local new_expires = now + ttl
redis.call('HSET', KEYS[1], 'expires_at', new_expires)
redis.call('PEXPIRE', KEYS[1], ttl)
audit_add(KEYS[2], retention,
  {'op', 'renew', 'result', 'renewed',
   'fencing', ARGV[2], 'time_ms', tostring(now)})
return {1, 'ok', new_expires}
"""

# 批量续租（2～32 个任务，统一 TTL），全成全败：
# 先用同一次 TIME 取到的服务器时间对全部任务逐项预检，
# 任一租约不存在（not_found）、令牌或防栅序号不匹配（conflict）、
# 已过期（expired）即整体失败：只给每个失败任务各落一条审计事件，
# 不 HSET 任何 expires_at、不 PEXPIRE 任何租约键，成功项也不落账
# （与批量申请全败时的落账口径一致：无状态变化即不写事件）；
# 全部有效时再统一写入，所有新 expires_at 基于同一基准时刻。
# 判定顺序与单项续租一致：存在性 -> 凭据 -> 过期。
# KEYS[1..n]      = 租约键
# KEYS[n+1..2n]   = 审计事件流键（与租约键同序）
# ARGV[1]         = 任务数 n
# ARGV[2..n+1]    = 各任务的令牌（与输入同序）
# ARGV[n+2..2n+1] = 各任务的防栅序号（与输入同序）
# ARGV[2n+2]      = ttl_ms（全批统一）
# ARGV[2n+3]      = 事件保留条数
# 成功返回 {1, expires_1, ..., expires_n}
# 失败返回 {0, {下标, 原因}, ...}（下标为 1 基，按输入顺序，每项一个嵌套数组）
RENEW_BATCH_LUA = _AUDIT_LUA + """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local n = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2 * n + 2])
local retention = ARGV[2 * n + 3]

local failures = {}
local reasons = {}
for i = 1, n do
  local token = redis.call('HGET', KEYS[i], 'token')
  if not token then
    reasons[i] = 'not_found'
    failures[#failures + 1] = i
  elseif token ~= ARGV[i + 1]
      or redis.call('HGET', KEYS[i], 'fencing') ~= ARGV[n + 1 + i] then
    reasons[i] = 'conflict'
    failures[#failures + 1] = i
  elseif tonumber(redis.call('HGET', KEYS[i], 'expires_at')) <= now then
    reasons[i] = 'expired'
    failures[#failures + 1] = i
  end
end

if #failures > 0 then
  for _, i in ipairs(failures) do
    local f = tonumber(redis.call('HGET', KEYS[i], 'fencing'))
    audit_add(KEYS[n + i], retention,
      {'op', 'renew_batch', 'result', reasons[i],
       'fencing', f and tostring(f) or '', 'time_ms', tostring(now),
       'batch_size', tostring(n)})
  end
  local reply = {0}
  for _, i in ipairs(failures) do
    reply[#reply + 1] = {i, reasons[i]}
  end
  return reply
end

local new_expires = now + ttl
local reply = {1}
for i = 1, n do
  redis.call('HSET', KEYS[i], 'expires_at', new_expires)
  redis.call('PEXPIRE', KEYS[i], ttl)
  audit_add(KEYS[n + i], retention,
    {'op', 'renew_batch', 'result', 'renewed',
     'fencing', ARGV[n + 1 + i], 'time_ms', tostring(now),
     'batch_size', tostring(n)})
  reply[#reply + 1] = new_expires
end
return reply
"""

# 交接（transfer）：当前持有者把单个任务租约交给新凭据。
# 必须同时匹配旧令牌与旧防栅序号，且租约尚未过期（判定口径与续租一致：
# 存在性 -> 凭据 -> 过期）。成功后：
#   - token 原子替换为服务端生成的新令牌；
#   - 防栅计数器 INCR 一次（旧序号立即失效，新序号单调递增）；
#   - expires_at 与键的 PEXPIRE 均保持不变——交接不续期；
#   - 与上述状态转换同一脚本原子写一条 transferred 审计事件。
# 失败（not_found / conflict / expired）不改动租约 Hash、不递增防栅计数器，
# 仅按现有规则落一条对应原因的审计事件。
# KEYS[1] = 租约键, KEYS[2] = 防栅计数器键, KEYS[3] = 审计事件流键
# ARGV[1] = 旧令牌, ARGV[2] = 旧 fencing, ARGV[3] = 新令牌
# ARGV[4] = 事件保留条数
# 成功返回 {1, 'ok', 新 fencing, expires_at}
# 失败返回 {0, 原因, 当前 fencing（无则 0）, expires_at（无则 0）}
TRANSFER_LUA = _AUDIT_LUA + """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local retention = ARGV[4]

local token = redis.call('HGET', KEYS[1], 'token')
if not token then
  audit_add(KEYS[3], retention,
    {'op', 'transfer', 'result', 'not_found',
     'fencing', '', 'time_ms', tostring(now)})
  return {0, 'not_found', 0, 0}
end
local current_fencing = redis.call('HGET', KEYS[1], 'fencing')
if token ~= ARGV[1] or current_fencing ~= ARGV[2] then
  audit_add(KEYS[3], retention,
    {'op', 'transfer', 'result', 'conflict',
     'fencing', current_fencing, 'time_ms', tostring(now)})
  return {0, 'conflict', tonumber(current_fencing),
    tonumber(redis.call('HGET', KEYS[1], 'expires_at'))}
end

local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at'))
if expires_at <= now then
  audit_add(KEYS[3], retention,
    {'op', 'transfer', 'result', 'expired',
     'fencing', current_fencing, 'time_ms', tostring(now)})
  return {0, 'expired', tonumber(current_fencing), expires_at}
end

-- 保留原 expires_at，仅替换 token 并递增防栅序号；不重新 PEXPIRE，交接不续期
local new_fencing = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1], 'token', ARGV[3], 'fencing', new_fencing)
audit_add(KEYS[3], retention,
  {'op', 'transfer', 'result', 'transferred',
   'fencing', tostring(new_fencing), 'time_ms', tostring(now)})
return {1, 'ok', new_fencing, expires_at}
"""

# 释放租约：令牌与防栅序号同时匹配才删除，否则静默忽略。
# 旧持有者超时后的释放请求不会误删新持有者的租约。
# KEYS[1] = 租约键, KEYS[2] = 审计事件流键
# ARGV[1] = 令牌, ARGV[2] = fencing, ARGV[3] = 事件保留条数
# 返回 1（已释放）或 0（无操作）
RELEASE_LUA = _AUDIT_LUA + """
local token = redis.call('HGET', KEYS[1], 'token')
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local retention = ARGV[3]

if not token then
  audit_add(KEYS[2], retention,
    {'op', 'release', 'result', 'ignored',
     'fencing', '', 'time_ms', tostring(now)})
  return 0
end
local current_fencing = redis.call('HGET', KEYS[1], 'fencing')
if token ~= ARGV[1] or current_fencing ~= ARGV[2] then
  local f = tonumber(current_fencing)
  audit_add(KEYS[2], retention,
    {'op', 'release', 'result', 'ignored',
     'fencing', f and tostring(f) or '', 'time_ms', tostring(now)})
  return 0
end
redis.call('DEL', KEYS[1])
audit_add(KEYS[2], retention,
  {'op', 'release', 'result', 'released',
   'fencing', ARGV[2], 'time_ms', tostring(now)})
return 1
"""

# 校验：判断 (令牌, 防栅序号) 是否仍对应当前有效租约。
# KEYS[1] = 租约键
# ARGV[1] = 令牌, ARGV[2] = fencing
# 返回 {是否有效(1/0), 剩余毫秒}
VALIDATE_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local token = redis.call('HGET', KEYS[1], 'token')
if not token then
  return {0, 0}
end
if token ~= ARGV[1] or redis.call('HGET', KEYS[1], 'fencing') ~= ARGV[2] then
  return {0, 0}
end

local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at'))
if expires_at <= now then
  return {0, 0}
end
return {1, expires_at - now}
"""

# 提交任务结果：租约校验与结果写入在同一个脚本内原子完成，
# 不存在"先校验通过、随后租约易主、旧持有者覆盖新结果"的竞态窗口。
# 校验口径与续租一致：存在性 -> 凭据（token + fencing）-> 过期。
# 仅当前未过期租约的凭据可提交；已过期 / 不存在 / 凭据不符一律拒绝。
#
# 幂等 / 防栅重放语义（以防栅序号为提交版本）：
#   - 同一防栅序号首次提交：写入结果 Hash（不设 TTL，结果不随租约到期删除），
#     返回 committed；
#   - 该序号已提交过结果：
#       内容一致（与首次提交的规范化 JSON 相同）-> 原样返回已存结果并标记
#       replayed，不写入、不更新任何时间戳；
#       内容不一致 -> 返回 result_conflict（HTTP 409），不改动已存结果；
#   - 凭据属于更新的租约（提交序号与当前 fencing 相同，且与已存序号不同）
#     即新租约提交新结果：覆盖写入。
#
# 结果键是一个不带 TTL 的 Hash：
#   result     规范化后的结果 JSON 字符串（内容比较与读取均以其为准）
#   fencing    该结果对应的防栅序号
#   committed_at_ms 首次提交时刻（Redis 服务器毫秒）
#
# KEYS[1] = 租约键, KEYS[2] = 结果键, KEYS[3] = 审计事件流键
# KEYS[4] = 全局结果变更流键（{prefix}:result-changes，所有任务键共用）
# ARGV[1] = 令牌, ARGV[2] = fencing, ARGV[3] = 规范化结果 JSON
# ARGV[4] = 审计事件保留条数, ARGV[5] = 任务键 task_key（写入全局变更流）
# ARGV[6] = 全局结果变更流保留条数
# 首次提交 / 新租约覆盖返回 {1, 'committed', fencing, committed_at_ms}
# 相同内容重放返回   {2, 'replayed', fencing, committed_at_ms}
# 凭据不符返回       {0, 'conflict', 当前 fencing（无则 0）, 0}
# 内容冲突返回       {0, 'result_conflict', 已存 fencing, 已存 committed_at}
# 租约不存在 / 已过期返回 {0, 'not_found'/'expired', 当前 fencing（无则 0）, 0}
#
# 普通提交与"前置版本保护提交"（RESULT_COMMIT_CHECKED_LUA）共享同一段
# 租约校验头部与幂等写入尾部（Python 字符串拼合），保证两者的凭据校验、
# 防栅与幂等语义逐字一致；checked 版本只是在头与尾之间插入前置核对。
_RESULT_COMMIT_HEAD_LUA = _AUDIT_LUA + """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local retention = ARGV[4]
local task_key = ARGV[5]
local change_retention = ARGV[6]

local token = redis.call('HGET', KEYS[1], 'token')
if not token then
  audit_add(KEYS[3], retention,
    {'op', 'result_commit', 'result', 'not_found',
     'fencing', '', 'time_ms', tostring(now)})
  return {0, 'not_found', 0, 0}
end
local current_fencing = redis.call('HGET', KEYS[1], 'fencing')
if token ~= ARGV[1] or current_fencing ~= ARGV[2] then
  audit_add(KEYS[3], retention,
    {'op', 'result_commit', 'result', 'conflict',
     'fencing', current_fencing, 'time_ms', tostring(now)})
  return {0, 'conflict', tonumber(current_fencing), 0}
end

local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at'))
if expires_at <= now then
  audit_add(KEYS[3], retention,
    {'op', 'result_commit', 'result', 'expired',
     'fencing', current_fencing, 'time_ms', tostring(now)})
  return {0, 'expired', tonumber(current_fencing), 0}
end
"""

# 租约校验通过后的幂等 / 防栅写入尾部（head 与本段共享 current_fencing 等局部变量）
_RESULT_COMMIT_TAIL_LUA = """
-- 凭据有效且租约未过期：同一脚本内判定幂等 / 冲突并写入结果
local stored = redis.call('HMGET', KEYS[2], 'fencing', 'committed_at_ms')
if stored[1] then
  if tonumber(stored[1]) == tonumber(ARGV[2]) then
    if redis.call('HGET', KEYS[2], 'result') == ARGV[3] then
      audit_add(KEYS[3], retention,
        {'op', 'result_commit', 'result', 'replayed',
         'fencing', ARGV[2], 'time_ms', tostring(now)})
      return {2, 'replayed', tonumber(ARGV[2]), tonumber(stored[2])}
    end
    audit_add(KEYS[3], retention,
      {'op', 'result_commit', 'result', 'result_conflict',
       'fencing', ARGV[2], 'time_ms', tostring(now)})
    return {0, 'result_conflict', tonumber(stored[1]), tonumber(stored[2])}
  end
end

-- 首次提交（该序号）或更新租约（fencing 更大）提交新结果：覆盖写入。
-- 结果键不设置 TTL：结果持久保留，不随租约到期而删除。
redis.call('HSET', KEYS[2],
  'result', ARGV[3],
  'fencing', ARGV[2],
  'committed_at_ms', now)
audit_add(KEYS[3], retention,
  {'op', 'result_commit', 'result', 'committed',
   'fencing', ARGV[2], 'time_ms', tostring(now)})
-- 与结果写入同一脚本原子地向全局变更流追加一条：
-- 记录任务键、防栅序号、提交时间与当次 JSON 结果，跨任务键按写入全局定序。
-- 仅首次写入新结果版本到达此处：失败与相同内容重放均不追加。
audit_add(KEYS[4], change_retention,
  {'task_key', task_key,
   'fencing', ARGV[2],
   'committed_at_ms', tostring(now),
   'result', ARGV[3]})
return {1, 'committed', tonumber(ARGV[2]), now}
"""

RESULT_COMMIT_LUA = _RESULT_COMMIT_HEAD_LUA + _RESULT_COMMIT_TAIL_LUA

# 前置结果版本保护的提交：在普通提交的租约校验头与幂等写入尾之间，于同一脚本内
# 额外核对"当前直接前置列表 + 各前置结果版本（fencing_token）"。全部一致才进入
# 现有结果提交的防栅与幂等写入；任何不一致（定义变更 / 缺失 / 多出前置、
# 某前置结果版本漂移）都返回 409 stale_dependencies，不改结果。
# 租约凭据类失败（not_found / conflict / expired）在前置核对之前按普通提交的
# 同一头部原样返回，同样不写入结果。
#
# KEYS[1] = 租约键, KEYS[2] = 结果键, KEYS[3] = 审计事件流键
# KEYS[4] = 全局结果变更流键, KEYS[5] = 本任务的依赖定义 List 键
# ARGV[1..6] 与 RESULT_COMMIT_LUA 相同（令牌 / fencing / 规范化结果 JSON /
#           审计保留条数 / 任务键 / 全局变更流保留条数）
# ARGV[7] = 结果键前缀（形如 "<prefix>:result:"）
# ARGV[8] = 快照前置数 n
# ARGV[9..n+8]     = 快照前置任务键（快照定义顺序）
# ARGV[n+9..2n+8]  = 快照中各前置结果的 fencing_token（与前一组同序）
# 前置不一致返回 {0, 'stale_dependencies', current_fencing, 0}（不写入、只落审计）
#
# 本任务的依赖定义 List 键（{prefix}:deps:{task_key}）声明为真实键（KEYS[5]）；
# 各前置的结果键与环检测脚本一样按前缀在脚本内拼出
RESULT_COMMIT_CHECKED_LUA = (
    _RESULT_COMMIT_HEAD_LUA
    + """
-- 前置核对：当前定义必须与快照逐项一致（数量、顺序、键名），
-- 且每个前置的已提交结果版本（结果 Hash 的 fencing）必须与快照相同。
local result_prefix = ARGV[7]
local n = tonumber(ARGV[8])
local current = redis.call('LRANGE', KEYS[5], 0, -1)
if #current ~= n then
  audit_add(KEYS[3], retention,
    {'op', 'result_commit', 'result', 'stale_dependencies',
     'fencing', ARGV[2], 'time_ms', tostring(now)})
  return {0, 'stale_dependencies', tonumber(current_fencing), 0}
end
for i = 1, n do
  local snap_dep = ARGV[8 + i]
  if current[i] ~= snap_dep then
    audit_add(KEYS[3], retention,
      {'op', 'result_commit', 'result', 'stale_dependencies',
       'fencing', ARGV[2], 'time_ms', tostring(now)})
    return {0, 'stale_dependencies', tonumber(current_fencing), 0}
  end
  -- 快照中该前置的结果版本（ARGV[n+8+i]）；可提交快照中每项恒为真实版本，
  -- "0" 仅作为防御性兜底
  local snap_f = tonumber(ARGV[n + 8 + i])
  local cur_f = tonumber(redis.call('HGET', result_prefix .. snap_dep, 'fencing'))
  if (cur_f or 0) ~= snap_f then
    audit_add(KEYS[3], retention,
      {'op', 'result_commit', 'result', 'stale_dependencies',
       'fencing', ARGV[2], 'time_ms', tostring(now)})
    return {0, 'stale_dependencies', tonumber(current_fencing), 0}
  end
end
"""
    + _RESULT_COMMIT_TAIL_LUA
)

# 全局结果变更流的分页读取（只读）：最早保留条目校验、按 (cursor, limit] 读取
# 页面与 next_cursor 探测在同一次脚本执行内完成。Lua 脚本原子执行，三步之间
# 不可能插入并发的 XADD MAXLEN 裁剪，因此"游标早于当前最早保留条目"的判定与
# 页面读取基于同一份流快照，不会出现"校验时未过期、读取时已被裁剪"的静默
# 漏读窗口。游标格式（<毫秒>-<序号>）由服务端先行校验，本脚本只负责比较与读取。
# KEYS[1] = 全局结果变更流键
# ARGV[1] = 游标（空串表示无游标，从最早保留条目读起）
# ARGV[2] = limit（1～200）
# 游标过期返回 {'410'}（空流沿用空页语义，不判 410）；
# 正常返回 {'200', 条目数, id_1, task_key_1, fencing_1, committed_at_ms_1,
#   result_1, ..., next_cursor（无下一页为空串）}
RESULT_CHANGES_PAGE_LUA = """
local stream = KEYS[1]
local cursor = ARGV[1]
local limit = tonumber(ARGV[2])

local function id_tuple(id)
  local dash = string.find(id, '-', 1, true)
  return tonumber(string.sub(id, 1, dash - 1)),
    tonumber(string.sub(id, dash + 1))
end

local function id_before(a, b)
  local ams, aseq = id_tuple(a)
  local bms, bseq = id_tuple(b)
  return ams < bms or (ams == bms and aseq < bseq)
end

-- 最早保留条目校验与后续读取共用同一份流快照：游标更早即 410，调用方应
-- 不带游标从当前最早条目重新追赶；空流不判 410（沿用空页语义）
local min_id = '-'
if cursor ~= '' then
  local oldest = redis.call('XRANGE', stream, '-', '+', 'COUNT', 1)
  if #oldest > 0 and id_before(cursor, oldest[1][1]) then
    return {'410'}
  end
  min_id = '(' .. cursor
end

-- 游标语义为"上次最后一条"，'(' 排除其本身，避免重复 / 漏读
local page = redis.call('XRANGE', stream, min_id, '+', 'COUNT', limit)

local next_cursor = ''
if #page == limit then
  -- 本页恰好满 limit：同一原子读内探测其后是否还有条目可翻
  local probe = redis.call('XRANGE', stream,
    '(' .. page[#page][1], '+', 'COUNT', 1)
  if #probe > 0 then
    next_cursor = page[#page][1]
  end
end

local reply = {'200', #page}
for _, entry in ipairs(page) do
  local fields = {}
  local f = entry[2]
  for i = 1, #f, 2 do
    fields[f[i]] = f[i + 1]
  end
  reply[#reply + 1] = entry[1]
  reply[#reply + 1] = fields['task_key']
  reply[#reply + 1] = fields['fencing']
  reply[#reply + 1] = fields['committed_at_ms']
  reply[#reply + 1] = fields['result']
end
reply[#reply + 1] = next_cursor
return reply
"""

# 保存任务依赖定义：先按"现有定义"做环检测，再整体替换，全成全败。
# 依赖定义是一个 Redis List（每项为一个前置任务键，顺序即请求顺序）：
#   {prefix}:deps:{task_key}
# 环检测只遍历当前已持久化的依赖定义（不把本次待写入的定义计入），
# 从本次提交的每个前置任务键出发，沿"前置 -> 前置"方向深度遍历：
#   - 一旦沿当前路径回到起点 task_key，说明写入后将成环 -> 拒绝；
#   - 去重后访问节点数超过 1024 -> 拒绝（遍历上界，防止大图拖垮脚本）。
# 环 / 超限均发生在任何写入之前：拒绝时原有依赖定义原样保留；
# 全部通过后再 DEL + RPUSH 原子替换为新定义（空列表即删除定义）。
# 脚本只允许声明自身依赖键（KEYS[1]），遍历中读取的其他依赖键通过
# 前缀在服务端拼出（{prefix}:deps:{task_key}），不改变现有键布局约定。
# KEYS[1] = 本任务依赖定义键
# ARGV[1] = 依赖键前缀（形如 "<prefix>:deps:"）
# ARGV[2] = 本任务键 task_key
# ARGV[3] = 前置任务数 m（已在服务端去重，m <= 16）
# ARGV[4..m+3] = 去重后的前置任务键（请求顺序）
# 成功返回   {1}
# 环拒绝返回 {0, 'cycle'}
# 超限拒绝返回 {0, 'too_many_nodes'}
DEPENDENCY_SET_LUA = """
local prefix = ARGV[1]
local target = ARGV[2]
local m = tonumber(ARGV[3])
local MAX_NODES = 1024

local visited = {}
local count = 0

-- 从起点 dep 出发沿现有定义遍历，返回是否能到达 target（成环）。
-- visited / count 在多个起点间共享：同一节点只计一次、只走一次。
local function reaches_target(dep)
  local stack = {dep}
  while #stack > 0 do
    local node = table.remove(stack)
    if node == target then
      return true
    end
    if not visited[node] then
      -- 严格上限：在访问（标记 + 读取）第 1025 个节点之前判定超限，
      -- 保证单次环检测最多只访问 1024 个现有节点。
      if count >= MAX_NODES then
        return false, true
      end
      visited[node] = true
      count = count + 1
      local prereqs = redis.call('LRANGE', prefix .. node, 0, -1)
      for _, p in ipairs(prereqs) do
        if p == target then
          return true
        end
        if not visited[p] then
          stack[#stack + 1] = p
        end
      end
    end
  end
  return false, false
end

for i = 1, m do
  local dep = ARGV[i + 3]
  if dep == target then
    return {0, 'cycle'}     -- 自身依赖：直接成环（防御性检查，服务端已先拒绝）
  end
  local cyclic, overflow = reaches_target(dep)
  if cyclic then
    return {0, 'cycle'}
  end
  if overflow then
    return {0, 'too_many_nodes'}
  end
end

-- 检测通过（且未做任何写入）：原子替换为新定义；空列表即清除定义
redis.call('DEL', KEYS[1])
if m > 0 then
  local args = {}
  for i = 1, m do
    args[i] = ARGV[i + 3]
  end
  redis.call('RPUSH', KEYS[1], unpack(args))
end
return {1}
"""

# 前置结果版本快照：在同一个只读 Lua 脚本内一次性读取
#   - 当前直接前置定义 List（{prefix}:deps:{task_key}，定义顺序）
#   - 每个前置任务已提交结果的版本（{prefix}:result:{dep} Hash 的 fencing）
# 因此"读定义 + 读各结果版本"之间不可能插入并发的定义替换或结果提交，
# 调用方拿到的是一致快照，可原样带回 /v1/results/commit-checked 做提交核对。
# 有任一前置尚未提交结果（结果键不存在）时返回 pending，不给出可提交快照：
# 此时调用方应先等待前置完成，再重新拉取快照。
# 结果版本 fencing 取自结果 Hash（即该结果提交时的租约防栅序号，
# 与 GET /v1/results/{task_key} 同源）；前置结果被新租约覆盖时版本递增，
# 旧快照即过期。
# 脚本不做任何写入。依赖 / 结果键不在 KEYS 中声明，按既有环检测脚本的约定
# 由服务端以前缀在脚本内拼出（集群部署这些键需位于同一 hash 路由策略下）。
# ARGV[1] = 依赖键前缀（形如 "<prefix>:deps:"）
# ARGV[2] = 结果键前缀（形如 "<prefix>:result:"）
# ARGV[3] = 本任务键 task_key
# 全部前置就绪返回
#   {1, n, dep_1, fencing_1, dep_2, fencing_2, ..., dep_n, fencing_n}
# 有未完成前置返回
#   {0, pending_count, pending_dep_1, ...}（按定义顺序，调用方仅用于提示）
# 无定义（n=0）返回 {1, 0}，即可提交的空快照
DEPENDENCY_SNAPSHOT_LUA = """
local deps = redis.call('LRANGE', ARGV[1] .. ARGV[3], 0, -1)
local pending = {}
for _, dep in ipairs(deps) do
  local f = redis.call('HGET', ARGV[2] .. dep, 'fencing')
  if not f then
    pending[#pending + 1] = dep
  end
end
if #pending > 0 then
  local reply = {0, #pending}
  for _, dep in ipairs(pending) do
    reply[#reply + 1] = dep
  end
  return reply
end

local reply = {1, #deps}
for _, dep in ipairs(deps) do
  reply[#reply + 1] = dep
  reply[#reply + 1] = tonumber(redis.call('HGET', ARGV[2] .. dep, 'fencing'))
end
return reply
"""

# 滑动窗口限流（成本累计）+ 可选请求幂等：单脚本原子完成
# "幂等命中判定 -> 清旧 -> 按本次窗口筛选累计 -> 判定 -> 扣减 -> 记录完整响应"。
# 限流键是一个 ZSet：score 为请求时刻（Redis 服务器毫秒），
# member 形如 "<唯一标识>:<cost>"，同一毫秒内的并发请求也不会互相覆盖；
# 每笔成本直接编码在 member 尾部，无需额外的键。
# 同一 client_key 可交替使用不同的 window_ms：ZSet 保留"最大允许窗口"
# （ARGV[5] = 服务端 LIMITS_MAX_WINDOW_MS）内的全部成本记录——
# 清旧只删除连最大允许窗口都已滑出的记录，键的兜底 PEXPIRE 也按最大允许
# 窗口设置，短窗口请求不会删除 / 提前过期仍属于更长窗口的记录；
# 每次请求只按本次 window_ms 筛选（score > now - window_ms）并累计成本、
# 计算 reset_at_ms 与拒绝等待时间，窗口外被保留的记录不参与本次判定。
# 幂等键是一个 Hash（仅携带 request_id 时传入 KEYS[2]）：
#   allowed / remaining / reset_at / retry_after / limit / window
# 保存首次请求的完整判定结果，PEXPIRE = 首次请求的 window_ms，
# 因此 request_id 仅在对应窗口内存活，窗口结束后自动删除、可被再次使用；
# Redis 单线程执行脚本，携带相同 request_id 的并发请求也只有一个完成扣减。
# KEYS[1] = 限流 ZSet 键
# KEYS[2] = 幂等 Hash 键（可选；未携带 request_id 时不传入）
# ARGV[1] = limit（窗口内允许的成本上限）
# ARGV[2] = window_ms（本次请求的窗口）
# ARGV[3] = cost（本次请求成本）
# ARGV[4] = 唯一标识（uuid hex，保证 member 不冲突）
# ARGV[5] = max_window_ms（服务端允许的最大窗口，决定记录保留与键 TTL）
# 幂等命中返回 {2, allowed, remaining, reset_at, retry_after, limit, window}
#   （直接回传首次结果，不清理 / 不判定 / 不扣减 / 不刷新任何 TTL）
# 首次放行返回 {1, remaining, reset_at_ms}
# 首次超限返回 {0, remaining, reset_at_ms, retry_after_ms}
# reset_at_ms 为本次窗口内最老一笔成本滑出的时刻；窗口为空时取 now + window_ms。
# retry_after_ms 为按记录到期顺序累计释放的成本首次足以容纳本次 cost 的
# 等待毫秒数（同毫秒到期的记录合并累计判断），可能晚于 reset_at_ms：
# 最老一笔滑出释放的成本未必容得下本次 cost（如窗口内 1+4、limit=5、
# 本次 cost=3 时，只等成本 1 滑出后 used=4 仍超限）。
LIMIT_CHECK_LUA = """
local key = KEYS[1]

-- 幂等命中：原样返回首次请求记录的完整判定结果
if #KEYS >= 2 then
  local stored = redis.call('HMGET', KEYS[2],
    'allowed', 'remaining', 'reset_at', 'retry_after', 'limit', 'window')
  if stored[1] then
    return {2,
      tonumber(stored[1]), tonumber(stored[2]), tonumber(stored[3]),
      tonumber(stored[4]), tonumber(stored[5]), tonumber(stored[6])}
  end
end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local max_window = tonumber(ARGV[5])

local window_start = now - window

-- 清旧只删除"连最大允许窗口都已滑出"的记录：同一 client_key 可能交替使用
-- 不同的 window_ms，若按本次窗口清理，短窗口请求会删除仍属于更长窗口的
-- 记录，切回长窗口后会错误放行；此处只回收任何合法窗口都不可能再计入的记录
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - max_window)

-- 只按本次窗口筛选（score > window_start）取出在窗记录，按请求时刻升序：
-- 既从 member 尾部解析成本累计 used，也供拒绝时按到期顺序计算"累计释放
-- 多少成本"的等待时长；窗口外但仍被保留的记录不参与本次判定
local entries = redis.call('ZRANGE', key, '(' .. window_start, '+inf',
  'BYSCORE', 'WITHSCORES')
local used = 0
for i = 1, #entries, 2 do
  used = used + tonumber(string.match(entries[i], ':(%d+)$'))
end

local reset_at
if #entries == 0 then
  reset_at = now + window
else
  reset_at = tonumber(entries[2]) + window
end

local allowed, remaining, retry_after
if used + cost > limit then
  allowed = 0
  -- 不同 window_ms 混用时窗口内累计可能已超过 limit（短窗口放入的记录
  -- 在长窗口内仍然计入），剩余额度最小为 0
  remaining = limit - used
  if remaining < 0 then remaining = 0 end
  -- 需要窗口内累计滑出 need 成本才容得下本次请求：按到期时刻（score
  -- 升序）逐组合计将释放的成本——同毫秒（同 score）到期的记录在同一
  -- 时刻一起滑出，必须合并成一组判断——首个使累计释放量达到 need 的
  -- 到期时刻即为等待终点，避免"只等最老一笔、届时额度仍不足"的过早重试
  local need = used + cost - limit
  local released = 0
  retry_after = reset_at - now  -- 兜底；cost <= limit 保证下面循环必然命中
  local i = 1
  while i <= #entries do
    local expire_score = tonumber(entries[i + 1])
    local j = i
    while j <= #entries and tonumber(entries[j + 1]) == expire_score do
      released = released + tonumber(string.match(entries[j], ':(%d+)$'))
      j = j + 2
    end
    if released >= need then
      retry_after = expire_score + window - now
      break
    end
    i = j
  end
else
  allowed = 1
  remaining = limit - used - cost
  retry_after = 0
  redis.call('ZADD', key, now, ARGV[4] .. ':' .. cost)
end
-- 键的兜底 TTL 同样按最大允许窗口设置：窗口外记录仍可能被后续更长窗口的
-- 请求计入，若按本次 window_ms 设置 TTL，短窗口请求会让键提前过期、丢失
-- 长窗口仍需要的记录；无流量时键在 max_window 后自动消失，此时最新记录
-- 对任何合法窗口都已滑出，不会误删可用记录
redis.call('PEXPIRE', key, max_window)

-- 首次请求：连同 limit / window 一起记录完整响应，TTL = 首次请求的
-- window_ms，使 request_id 恰好存活到该窗口结束
if #KEYS >= 2 then
  redis.call('HSET', KEYS[2],
    'allowed', allowed,
    'remaining', remaining,
    'reset_at', reset_at,
    'retry_after', retry_after,
    'limit', limit,
    'window', window)
  redis.call('PEXPIRE', KEYS[2], window)
end

if allowed == 1 then
  return {1, remaining, reset_at}
end
return {0, remaining, reset_at, retry_after}
"""

# 并发容量公共头：服务器时间、占用 ZSet 与已占用权重汇总的联动维护
# （清旧逐笔扣减、申请 / 释放同步增减、缺汇总时一次性重建），
# 以及占用集合与汇总键的兜底 TTL 回收。拼接到每个容量脚本前，
# 保证四个脚本的"清旧 -> 核算 -> 回收"口径逐字一致。
_CAPACITY_LUA = """
-- Redis 服务器毫秒（与全部租约 / 限流脚本同一时钟口径）
local function cap_now()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

-- 从 member（凭据，形如 "<uuid32>:<weight>"）尾部解析本次占用权重
local function member_weight(member)
  return tonumber(string.match(member, ':(%d+)$'))
end

-- 遍历占用集合逐项累计权重（仅用于升级兼容的汇总一次性重建）
local function sum_slot_weights(key)
  local used = 0
  local members = redis.call('ZRANGE', key, 0, -1)
  for _, m in ipairs(members) do
    used = used + member_weight(m)
  end
  return used
end

-- 状态变更后的兜底回收：集合为空则同时 DEL 集合键与汇总键
-- （集合清空 / 自然过期后汇总必须一并失效，不得残留旧计数）；
-- 非空时按 (最大到期 - now + 1) 给两个键设置相同 PEXPIRE，
-- +1ms 使键 TTL 永远不短于最晚到期判定窗口，PEXPIRE 仅回收无流量残留键，
-- 占用是否有效一律以 score 与服务器时间比较为准。
local function finalize_slots(slot_key, used_key, now)
  local newest = redis.call('ZRANGE', slot_key, -1, -1, 'WITHSCORES')
  if #newest == 0 then
    redis.call('DEL', slot_key, used_key)
  else
    local ttl = tonumber(newest[2]) - now + 1
    redis.call('PEXPIRE', slot_key, ttl)
    redis.call('PEXPIRE', used_key, ttl)
  end
end

-- 给重建 / 扣减后的汇总键补齐 / 保留与占用集合一致的兜底 TTL：
-- KEEPTTL 使扣减写入不清空既有 TTL；集合键有 TTL（升级前数据同样带兜底
-- PEXPIRE）而汇总刚建立时，按集合键的剩余 PTTL 同步一份，保证两键一同
-- 自然过期——任何路径下汇总都不会成为无 TTL 的残留旧计数。
local function sync_used_ttl(slot_key, used_key)
  local ttl = redis.call('PTTL', slot_key)
  if ttl > 0 then
    redis.call('PEXPIRE', used_key, ttl)
  end
end

-- 按服务器时间清理 score <= now 的到期占用，并同步维护已占用权重汇总，
-- 返回清理后当前有效占用的权重之和。
-- 常规路径只取出到期 member 逐笔解析权重后一次性扣减，不遍历全部有效占用；
-- 仅当"占用集合非空但缺少汇总"（升级前数据）或汇总损坏时，
-- 才在本次调用内遍历集合重建一次汇总（兼容升级前数据）。
-- 清理后集合为空（本次到期即全部占用）时立即 DEL 两个键，
-- 保证集合清空与汇总失效在同一原子操作内完成。
local function prune_slots(slot_key, used_key, now)
  local head = redis.call('ZRANGE', slot_key, 0, 0, 'WITHSCORES')
  if #head > 0 and tonumber(head[2]) <= now then
    -- 存在到期占用：取出全部到期 member 逐笔解析权重，再统一删除、扣减
    local expired = redis.call('ZRANGE', slot_key, '-inf', now, 'BYSCORE')
    local expired_weight = 0
    for _, m in ipairs(expired) do
      expired_weight = expired_weight + member_weight(m)
    end
    redis.call('ZREMRANGEBYSCORE', slot_key, '-inf', now)
    if redis.call('ZCARD', slot_key) == 0 then
      redis.call('DEL', slot_key, used_key)
      return 0
    end
    local stored = redis.call('GET', used_key)
    local stored_num = stored and tonumber(stored)
    if stored_num and stored_num >= 0 then
      local used = stored_num - expired_weight
      if used < 0 then used = 0 end
      -- KEEPTTL：扣减不清空汇总既有 TTL（429 / 拒绝路径不调 finalize）
      redis.call('SET', used_key, used, 'KEEPTTL')
      return used
    end
    -- 有集合无汇总（升级前数据）或汇总损坏：本次调用内重建一次
    local used = sum_slot_weights(slot_key)
    redis.call('SET', used_key, used)
    sync_used_ttl(slot_key, used_key)
    return used
  end

  -- 无到期占用：有效占用量直接读取汇总，常规请求不扫描集合
  local stored = redis.call('GET', used_key)
  local used = stored and tonumber(stored)
  if used and used >= 0 then
    return used
  end
  if redis.call('ZCARD', slot_key) == 0 then
    -- 集合本就为空：清掉可能残留的汇总，后续申请从 0 起算
    redis.call('DEL', slot_key, used_key)
    return 0
  end
  -- 汇总缺失（升级前数据）或损坏（非整数 / 负数）：在本次调用内一次性重建
  local used = sum_slot_weights(slot_key)
  redis.call('SET', used_key, used)
  sync_used_ttl(slot_key, used_key)
  return used
end
"""

# 设置（调整）资源键的并发容量。先按服务器时间清理过期占用（同步扣减汇总）、
# 读取当前有效占用量，新容量低于有效占用量时拒绝且**原样保留**旧配置
# （不写容量键、不动占用集合与汇总）。
# 未配置（KEYS[1] 不存在）时当前容量返回 0、旧容量返回 nil（Python 侧转成 None）。
# KEYS[1] = 容量配置键 {prefix}:capacity:{resource_key}
# KEYS[2] = 占用集合键 {prefix}:slots:{resource_key}
# KEYS[3] = 已占用权重汇总键 {prefix}:slots-used:{resource_key}
# ARGV[1] = 新容量（整数权重，>0；上限由服务端 pydantic 校验）
# 成功返回   {1, 新容量, 当前有效占用量}
# 拒绝返回   {0, 'below_active', 旧容量（不存在为 false）, 当前有效占用量}
CAPACITY_SET_LUA = _CAPACITY_LUA + """
local now = cap_now()
local used = prune_slots(KEYS[2], KEYS[3], now)
local new_capacity = tonumber(ARGV[1])

if new_capacity < used then
  local old = redis.call('GET', KEYS[1])
  return {0, 'below_active', old, used}
end

redis.call('SET', KEYS[1], new_capacity)
-- 清旧可能删除 / 重建了汇总：非空时同步一次兜底 TTL；为空时 prune 已 DEL
if redis.call('EXISTS', KEYS[2]) == 1 then
  finalize_slots(KEYS[2], KEYS[3], now)
end
return {1, new_capacity, used}
"""

# 申请占用：服务器时间清旧（同步扣减汇总）-> 读容量 -> 读汇总核算剩余 ->
# 写入新占用并 INCRBY 增加汇总，单脚本原子完成。
# Redis 单线程串行执行脚本，同一资源键的并发申请不会超配；超限时不写入。
# 凭据 member = ARGV[2](uuid32) .. ':' .. weight，score = now + ttl，
# 每次申请独立，过期后旧 member 被清旧移除，新申请得到全新凭据。
# KEYS[1] = 容量配置键, KEYS[2] = 占用集合键, KEYS[3] = 已占用权重汇总键
# ARGV[1] = 本次权重 weight（正整数；服务端已保证 <= 容量上限）
# ARGV[2] = uuid（32 位十六进制）
# ARGV[3] = ttl_ms
# 成功返回 {1, 凭据, expires_at_ms, 剩余容量, 容量}
# 未配置返回 {0, 'not_configured'}
# 容量不足返回 {0, 'capacity_exhausted', 剩余容量, 最早到期, 重试毫秒, 容量}
CAPACITY_ACQUIRE_LUA = _CAPACITY_LUA + """
local now = cap_now()
local used = prune_slots(KEYS[2], KEYS[3], now)

local capacity = redis.call('GET', KEYS[1])
if not capacity then
  return {0, 'not_configured'}
end
capacity = tonumber(capacity)

local weight = tonumber(ARGV[1])
local remaining = capacity - used
if weight > remaining then
  local oldest = redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')
  local retry_after = 0
  if #oldest > 0 then
    retry_after = tonumber(oldest[2]) - now
    if retry_after < 0 then retry_after = 0 end
  end
  return {0, 'capacity_exhausted', remaining,
    #oldest > 0 and tonumber(oldest[2]) or 0, retry_after, capacity}
end

local credential = ARGV[2] .. ':' .. weight
local expires_at = now + tonumber(ARGV[3])
redis.call('ZADD', KEYS[2], expires_at, credential)
-- 汇总同步增加本次权重：prune 已确保汇总存在（集合此前为空时已被 DEL，
-- 此处随首笔占用重新建立）
redis.call('INCRBY', KEYS[3], weight)
finalize_slots(KEYS[2], KEYS[3], now)
return {1, credential, expires_at, capacity - used - weight, capacity}
"""

# 续期：仅当凭据（完整 member）仍存在于占用集合中才刷新其到期时刻。
# 凭据不存在（从未签发，或已过期被清旧删除）即 not_found，不做任何修改；
# 故过期占用 / 旧凭据的续期不可能影响其后新申请得到的新占用。
# KEYS[1] = 占用集合键, KEYS[2] = 已占用权重汇总键
# ARGV[1] = 凭据, ARGV[2] = ttl_ms, ARGV[3] = 容量配置键（用于回算剩余容量）
# 成功返回 {1, 'ok', 新 expires_at_ms, 剩余容量}
# 失败返回 {0, 'not_found', 0, 剩余容量}
CAPACITY_RENEW_LUA = _CAPACITY_LUA + """
local now = cap_now()
local used = prune_slots(KEYS[1], KEYS[2], now)

if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  local cap = tonumber(redis.call('GET', ARGV[3])) or 0
  local remaining = cap - used
  if remaining < 0 then remaining = 0 end
  return {0, 'not_found', 0, remaining}
end

local new_expires = now + tonumber(ARGV[2])
redis.call('ZADD', KEYS[1], 'XX', new_expires, ARGV[1])
-- 续期不改变权重，仅按新的最大到期刷新集合与汇总的兜底 TTL
finalize_slots(KEYS[1], KEYS[2], now)
local cap = tonumber(redis.call('GET', ARGV[3])) or 0
local remaining = cap - used
if remaining < 0 then remaining = 0 end
return {1, 'ok', new_expires, remaining}
"""

# 释放：仅当凭据（完整 member）仍存在时删除并从汇总 INCRBY 扣减该笔权重；
# 凭据错误 / 已过期则幂等忽略。
# 旧持有者在占用过期后调用释放不会误删新占用（新占用是全新的 member）。
# KEYS[1] = 占用集合键, KEYS[2] = 已占用权重汇总键
# ARGV[1] = 凭据, ARGV[2] = 容量配置键（用于回算剩余容量）
# 返回 {是否真正释放(1/0), 释放权重（未释放为 0）, 剩余容量}
CAPACITY_RELEASE_LUA = _CAPACITY_LUA + """
local now = cap_now()
local used = prune_slots(KEYS[1], KEYS[2], now)

local weight = 0
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  weight = member_weight(ARGV[1])
  redis.call('ZREM', KEYS[1], ARGV[1])
  used = used - weight
  if used < 0 then used = 0 end
  if redis.call('ZCARD', KEYS[1]) == 0 then
    -- 集合清空：汇总同时失效，后续申请从 0 起算
    redis.call('DEL', KEYS[1], KEYS[2])
  else
    redis.call('SET', KEYS[2], used)
  end
end
-- 未释放（凭据错误 / 已过期）时 prune 已处理过期回收与兜底 TTL；
-- 真正释放后集合仍非空时按新的最大到期刷新两个键的 TTL
if redis.call('EXISTS', KEYS[1]) == 1 then
  finalize_slots(KEYS[1], KEYS[2], now)
end
local cap = tonumber(redis.call('GET', ARGV[2])) or 0
local released = 0
if weight > 0 then released = 1 end
local remaining = cap - used
if remaining < 0 then remaining = 0 end
return {released, weight, remaining}
"""
