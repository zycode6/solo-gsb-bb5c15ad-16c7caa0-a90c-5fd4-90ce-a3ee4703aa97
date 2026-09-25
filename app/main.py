"""租约协调服务：多工作进程间的独占任务协调。

所有租约状态转换均由 Redis Lua 脚本原子完成（见 app/lua.py），
过期判断基于 Redis 服务器时间（TIME 命令）。
任务结果提交同样由单个 Lua 脚本原子完成租约校验与结果写入，
避免"校验后租约易主、旧持有者覆盖结果"。
"""
from __future__ import annotations

import json
import math
import re
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError

from .config import Settings, get_settings
from .lua import (
    ACQUIRE_BATCH_LUA,
    ACQUIRE_LUA,
    CAPACITY_ACQUIRE_LUA,
    CAPACITY_RELEASE_LUA,
    CAPACITY_RENEW_LUA,
    CAPACITY_SET_LUA,
    DEPENDENCY_SET_LUA,
    DEPENDENCY_SNAPSHOT_LUA,
    LIMIT_CHECK_LUA,
    RELEASE_LUA,
    RENEW_BATCH_LUA,
    RENEW_LUA,
    RESULT_CHANGES_PAGE_LUA,
    RESULT_COMMIT_CHECKED_LUA,
    RESULT_COMMIT_LUA,
    TRANSFER_LUA,
    VALIDATE_LUA,
)
from .schemas import (
    AcquireBatchRequest,
    AcquireBatchResponse,
    AcquiredLease,
    AcquireRequest,
    AcquireResponse,
    CapacitySetRequest,
    CapacitySetResponse,
    CheckedResultCommitRequest,
    DependencyReadyResponse,
    DependencySetRequest,
    DependencySetResponse,
    DependencySnapshotItem,
    DependencySnapshotResponse,
    LeaseEvent,
    LeaseEventPage,
    LeaseStateResponse,
    LimitCheckRequest,
    LimitCheckResponse,
    ReleaseRequest,
    ReleaseResponse,
    RenewBatchFailure,
    RenewBatchLease,
    RenewBatchRequest,
    RenewBatchResponse,
    RenewRequest,
    RenewResponse,
    ResultChange,
    ResultChangePage,
    ResultCommitRequest,
    ResultCommitResponse,
    SlotAcquireRequest,
    SlotAcquireResponse,
    SlotReleaseRequest,
    SlotReleaseResponse,
    SlotRenewRequest,
    SlotRenewResponse,
    TaskResultResponse,
    TransferRequest,
    TransferResponse,
    ValidateRequest,
    ValidateResponse,
    canonical_result_json,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.redis = redis
    # 启动时注册 Lua 脚本（EVALSHA 执行，脚本不存在时客户端自动回退 EVAL）
    app.state.scripts = SimpleNamespace(
        acquire=redis.register_script(ACQUIRE_LUA),
        acquire_batch=redis.register_script(ACQUIRE_BATCH_LUA),
        renew=redis.register_script(RENEW_LUA),
        renew_batch=redis.register_script(RENEW_BATCH_LUA),
        transfer=redis.register_script(TRANSFER_LUA),
        release=redis.register_script(RELEASE_LUA),
        validate=redis.register_script(VALIDATE_LUA),
        result_commit=redis.register_script(RESULT_COMMIT_LUA),
        result_commit_checked=redis.register_script(RESULT_COMMIT_CHECKED_LUA),
        result_changes_page=redis.register_script(RESULT_CHANGES_PAGE_LUA),
        dependency_snapshot=redis.register_script(DEPENDENCY_SNAPSHOT_LUA),
        limit_check=redis.register_script(LIMIT_CHECK_LUA),
        dependency_set=redis.register_script(DEPENDENCY_SET_LUA),
        capacity_set=redis.register_script(CAPACITY_SET_LUA),
        slot_acquire=redis.register_script(CAPACITY_ACQUIRE_LUA),
        slot_renew=redis.register_script(CAPACITY_RENEW_LUA),
        slot_release=redis.register_script(CAPACITY_RELEASE_LUA),
    )
    yield
    await redis.aclose()


app = FastAPI(
    title="Lease & Rate Limit API",
    version="1.9.1",
    description=(
        "供多个工作进程协调独占任务的租约服务、基于 Redis Lua 的"
        "原子滑动窗口成本限流，租约凭据保护的任务结果提交，"
        "基于持久任务结果的前置依赖门禁，前置结果版本快照保护的"
        "受保护结果提交，按全局写入顺序订阅已提交结果的"
        "结果变更流，以及按资源键分配加权并发容量、"
        "供多个工作进程共享有上限处理槽位的容量槽位服务"
        "（FastAPI + Redis Lua）"
    ),
    lifespan=lifespan,
)


@app.exception_handler(RedisError)
async def redis_error_handler(request: Request, exc: RedisError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "redis unavailable"},
    )


def get_redis(request: Request) -> Redis:
    return request.app.state.redis


def get_scripts(request: Request) -> SimpleNamespace:
    return request.app.state.scripts


RedisDep = Annotated[Redis, Depends(get_redis)]
ScriptsDep = Annotated[SimpleNamespace, Depends(get_scripts)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


def lease_key(settings: Settings, task_key: str) -> str:
    return f"{settings.key_prefix}:lease:{task_key}"


def fencing_key(settings: Settings, task_key: str) -> str:
    return f"{settings.key_prefix}:fencing:{task_key}"


def event_stream_key(settings: Settings, task_key: str) -> str:
    return f"{settings.key_prefix}:events:{task_key}"


def result_key(settings: Settings, task_key: str) -> str:
    # 结果持久保留（无 TTL），与租约 Hash / 事件流共享前缀、独立命名空间
    return f"{settings.key_prefix}:result:{task_key}"


def result_changes_key(settings: Settings) -> str:
    # 所有任务键共用的全局结果变更流（无 TTL，按保留条数裁剪），
    # 与单任务结果 Hash 共享前缀、独立命名空间
    return f"{settings.key_prefix}:result-changes"


def dependency_key(settings: Settings, task_key: str) -> str:
    # 前置依赖定义（Redis List），与结果 / 租约共享前缀、独立命名空间
    return f"{settings.key_prefix}:deps:{task_key}"


def capacity_key(settings: Settings, resource_key: str) -> str:
    # 资源键并发容量配置（Redis String，无 TTL），沿用租约键前缀、独立命名空间
    return f"{settings.key_prefix}:capacity:{resource_key}"


def slots_key(settings: Settings, resource_key: str) -> str:
    # 资源键的有效占用集合（Redis ZSet：member=凭据、score=到期毫秒）
    return f"{settings.key_prefix}:slots:{resource_key}"


def slots_used_key(settings: Settings, resource_key: str) -> str:
    # 资源键的已占用权重汇总（Redis String：当前有效占用权重之和，
    # 与占用 ZSet 在同一 Lua 操作内联动维护，随集合清空 / 过期而失效）
    return f"{settings.key_prefix}:slots-used:{resource_key}"


# Stream 条目 ID 形如 "<毫秒时间>-<序号>"，直接作为分页游标
_STREAM_ID_PATTERN = re.compile(r"^\d+-\d+$")


def resolve_ttl(ttl_ms: int | None, settings: Settings) -> int:
    ttl = ttl_ms if ttl_ms is not None else settings.default_ttl_ms
    if not settings.min_ttl_ms <= ttl <= settings.max_ttl_ms:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"ttl_ms must be within "
                f"[{settings.min_ttl_ms}, {settings.max_ttl_ms}]"
            ),
        )
    return ttl


def limit_key(settings: Settings, client_key: str) -> str:
    return f"{settings.limit_key_prefix}:{client_key}"


def idempotency_key(settings: Settings, client_key: str, request_id: str) -> str:
    # client_key 长度定界（4 位十六进制）后再拼接，保证不同 client_key
    # 即使内容包含分隔符也不可能映射到同一个幂等键
    length_tag = f"{len(client_key):04x}"
    return (
        f"{settings.limit_idem_key_prefix}:{length_tag}:{client_key}:{request_id}"
    )


def resolve_window(window_ms: int, settings: Settings) -> int:
    if not settings.limits_min_window_ms <= window_ms <= settings.limits_max_window_ms:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"window_ms must be within "
                f"[{settings.limits_min_window_ms}, {settings.limits_max_window_ms}]"
            ),
        )
    return window_ms


@app.get("/healthz")
async def healthz(redis: RedisDep) -> dict:
    await redis.ping()
    return {"status": "ok"}


@app.get("/")
async def root() -> dict:
    return {"service": "lease-rate-limit-api", "docs": "/docs"}


@app.post("/v1/leases/acquire", response_model=AcquireResponse)
async def acquire(
    req: AcquireRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> AcquireResponse:
    """申请租约。仅当任务未被占用或旧租约已过期时成功。

    成功返回 200 及所有权令牌 + 防栅序号；被占用返回 409。
    """
    ttl = resolve_ttl(req.ttl_ms, settings)
    token = uuid.uuid4().hex
    ok, fencing, expires_at = await scripts.acquire(
        keys=[
            lease_key(settings, req.task_key),
            fencing_key(settings, req.task_key),
            event_stream_key(settings, req.task_key),
        ],
        args=[token, ttl, settings.event_retention],
    )
    if ok:
        return AcquireResponse(
            acquired=True,
            task_key=req.task_key,
            token=token,
            fencing_token=fencing,
            expires_at_ms=expires_at,
        )
    response.status_code = status.HTTP_409_CONFLICT
    return AcquireResponse(
        acquired=False,
        task_key=req.task_key,
        fencing_token=fencing,
        expires_at_ms=expires_at,
    )


@app.post(
    "/v1/leases/acquire-batch",
    response_model=AcquireBatchResponse,
)
async def acquire_batch(
    req: AcquireBatchRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> AcquireBatchResponse:
    """批量申请租约（2～32 个不重复任务键，统一 TTL），全成全败。

    由单个 Lua 脚本、单次服务器 TIME 原子完成：任一任务仍被有效租约占用时
    返回 409 及全部冲突键，不创建任何租约、不递增任何防栅计数器；
    全部可用（含旧租约已过期可接管）时，按输入顺序返回每个任务独立的
    token、递增的 fencing_token 与同一基准计算的 expires_at_ms。
    所得凭据可直接用于现有的续租 / 释放 / 校验接口。
    """
    ttl = resolve_ttl(req.ttl_ms, settings)
    tokens = [uuid.uuid4().hex for _ in req.task_keys]
    lease_keys = [lease_key(settings, k) for k in req.task_keys]
    fencer_keys = [fencing_key(settings, k) for k in req.task_keys]
    event_keys = [event_stream_key(settings, k) for k in req.task_keys]

    result = await scripts.acquire_batch(
        keys=lease_keys + fencer_keys + event_keys,
        args=[
            str(len(req.task_keys)),
            *tokens,
            str(ttl),
            settings.event_retention,
        ],
    )

    if result[0]:
        leases = [
            AcquiredLease(
                task_key=req.task_keys[i],
                token=tokens[i],
                fencing_token=result[1 + 2 * i],
                expires_at_ms=result[2 + 2 * i],
            )
            for i in range(len(req.task_keys))
        ]
        return AcquireBatchResponse(acquired=True, leases=leases)

    conflicting = [req.task_keys[idx - 1] for idx in result[1:]]
    response.status_code = status.HTTP_409_CONFLICT
    return AcquireBatchResponse(
        acquired=False, conflicting_task_keys=conflicting
    )


@app.post("/v1/leases/renew", response_model=RenewResponse)
async def renew(
    req: RenewRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> RenewResponse:
    """续租。必须同时匹配令牌与防栅序号，且租约尚未过期。

    失败返回 409，reason 取值：not_found / conflict / expired。
    """
    ttl = resolve_ttl(req.ttl_ms, settings)
    ok, reason, expires_at = await scripts.renew(
        keys=[
            lease_key(settings, req.task_key),
            event_stream_key(settings, req.task_key),
        ],
        args=[req.token, str(req.fencing_token), ttl, settings.event_retention],
    )
    if ok:
        return RenewResponse(renewed=True, expires_at_ms=expires_at)
    response.status_code = status.HTTP_409_CONFLICT
    return RenewResponse(renewed=False, reason=reason)


@app.post(
    "/v1/leases/renew-batch",
    response_model=RenewBatchResponse,
    responses={
        status.HTTP_409_CONFLICT: {"model": RenewBatchResponse},
    },
)
async def renew_batch(
    req: RenewBatchRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> RenewBatchResponse:
    """批量续租（2～32 个不重复任务键，各自凭据，统一 TTL），全成全败。

    由单个 Lua 脚本、单次服务器 TIME 原子完成全量预检并统一提交：
    任一租约不存在（`not_found`）、令牌或防栅序号不匹配（`conflict`）、
    已过期（`expired`）时整批失败，返回 **409** 与逐项失败原因，
    不更新任何 TTL（成功项也不续租）；全部有效时一次性更新所有 TTL，
    `leases` 按输入顺序给出每个任务新的 `expires_at_ms`（同一时间基准）。
    每个任务的结果（成功或失败原因）都原子写入各自的审计 Stream。
    """
    ttl = resolve_ttl(req.ttl_ms, settings)
    items = req.items
    lease_keys = [lease_key(settings, item.task_key) for item in items]
    event_keys = [event_stream_key(settings, item.task_key) for item in items]

    result = await scripts.renew_batch(
        keys=lease_keys + event_keys,
        args=[
            str(len(items)),
            *[item.token for item in items],
            *[str(item.fencing_token) for item in items],
            str(ttl),
            settings.event_retention,
        ],
    )

    if result[0]:
        leases = [
            RenewBatchLease(
                task_key=items[i].task_key,
                expires_at_ms=result[1 + i],
            )
            for i in range(len(items))
        ]
        return RenewBatchResponse(renewed=True, leases=leases)

    failures = [
        RenewBatchFailure(
            index=idx - 1,
            task_key=items[idx - 1].task_key,
            reason=reason,
        )
        for idx, reason in result[1:]
    ]
    response.status_code = status.HTTP_409_CONFLICT
    return RenewBatchResponse(renewed=False, failures=failures)


@app.post(
    "/v1/leases/transfer",
    response_model=TransferResponse,
    responses={
        status.HTTP_409_CONFLICT: {"model": TransferResponse},
    },
)
async def transfer(
    req: TransferRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> TransferResponse:
    """交接租约：当前持有者把单个任务租约交给服务端签发的新凭据。

    必须同时匹配令牌与防栅序号，且租约尚未过期（按 Redis 服务器时间判定）。
    成功时原子完成：替换为服务端生成的新 token、防栅序号 INCR 一次，
    **保留原 expires_at 不续期**，并返回新凭据与 expires_at_ms。
    此后旧凭据在续租 / 释放 / 校验接口立即失效，新凭据立即可用。

    失败返回 409，reason 取值：not_found / conflict / expired；
    失败不改动租约、不递增防栅序号。成功交接原子写入该任务的审计 Stream。
    """
    new_token = uuid.uuid4().hex
    ok, _reason, fencing, expires_at = await scripts.transfer(
        keys=[
            lease_key(settings, req.task_key),
            fencing_key(settings, req.task_key),
            event_stream_key(settings, req.task_key),
        ],
        args=[
            req.token,
            str(req.fencing_token),
            new_token,
            settings.event_retention,
        ],
    )
    if ok:
        return TransferResponse(
            transferred=True,
            task_key=req.task_key,
            token=new_token,
            fencing_token=fencing,
            expires_at_ms=expires_at,
        )
    response.status_code = status.HTTP_409_CONFLICT
    return TransferResponse(
        transferred=False,
        task_key=req.task_key,
        fencing_token=fencing or None,
        expires_at_ms=expires_at or None,
        reason=_reason,
    )


@app.post("/v1/leases/release", response_model=ReleaseResponse)
async def release(
    req: ReleaseRequest, scripts: ScriptsDep, settings: SettingsDep
) -> ReleaseResponse:
    """释放租约。令牌与防栅序号同时匹配才删除，否则幂等忽略。

    旧持有者在超时后调用本接口不会影响新租约。
    """
    released = await scripts.release(
        keys=[
            lease_key(settings, req.task_key),
            event_stream_key(settings, req.task_key),
        ],
        args=[req.token, str(req.fencing_token), settings.event_retention],
    )
    return ReleaseResponse(released=bool(released))


@app.post("/v1/leases/validate", response_model=ValidateResponse)
async def validate(
    req: ValidateRequest, scripts: ScriptsDep, settings: SettingsDep
) -> ValidateResponse:
    """校验 (令牌, 防栅序号) 是否仍对应当前有效租约，供下游判断操作归属。"""
    valid, remaining_ms = await scripts.validate(
        keys=[lease_key(settings, req.task_key)],
        args=[req.token, str(req.fencing_token)],
    )
    return ValidateResponse(valid=bool(valid), remaining_ms=remaining_ms)


@app.get(
    "/v1/leases/{task_key:path}/events",
    response_model=LeaseEventPage,
)
async def list_events(
    task_key: str,
    redis: RedisDep,
    settings: SettingsDep,
    cursor: str | None = Query(
        default=None,
        description=(
            "分页游标：上次响应返回的 next_cursor（Redis Stream 条目 ID）；"
            "不传表示从最旧事件开始。"
        ),
    ),
    limit: int = Query(
        default=50,
        ge=1,
        le=200,
        description="单页最多返回的事件条数（1～200，默认 50）",
    ),
) -> LeaseEventPage:
    """读取任务键的租约审计事件（Redis Stream），按时间升序游标分页。

    事件在申请（成功 / 被拒）、续租（单个 / 批量）、释放与批量申请逐任务
    落账时，与租约状态转换在同一个 Lua 脚本内原子写入，包含操作结果、
    防栅序号与 Redis 服务器时间。每个任务键保留最新
    `LEASE_EVENT_RETENTION` 条，超出按最旧事件裁剪。
    """
    if cursor is not None and not _STREAM_ID_PATTERN.match(cursor):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="cursor must be a Redis Stream entry id (<ms>-<seq>)",
        )

    # cursor 语义为"上次最后一条"，用 '(' 排除其本身，避免重复 / 漏读
    min_id = f"({cursor}" if cursor is not None else "-"
    raw = await redis.xrange(
        event_stream_key(settings, task_key), min=min_id, max="+", count=limit
    )

    events = [
        LeaseEvent(
            id=entry_id,
            op=fields.get("op", ""),
            result=fields.get("result", ""),
            fencing_token=int(fields["fencing"]) if fields.get("fencing") else None,
            time_ms=int(fields["time_ms"]),
            batch_size=int(fields["batch_size"]) if fields.get("batch_size") else None,
        )
        for entry_id, fields in raw
    ]
    # 本页恰好满 limit 时补一次探测，判断后面是否还有事件可翻
    if len(events) < limit:
        next_cursor = None
    else:
        probe = await redis.xrange(
            event_stream_key(settings, task_key),
            min=f"({events[-1].id}",
            max="+",
            count=1,
        )
        next_cursor = events[-1].id if probe else None

    return LeaseEventPage(
        task_key=task_key, events=events, next_cursor=next_cursor
    )


@app.get("/v1/leases/{task_key:path}", response_model=LeaseStateResponse)
async def inspect(
    task_key: str, redis: RedisDep, settings: SettingsDep
) -> LeaseStateResponse:
    """查看任务键当前租约状态（只读，不暴露所有权令牌）。"""
    data = await redis.hgetall(lease_key(settings, task_key))
    if not data:
        return LeaseStateResponse(held=False)
    sec, usec = await redis.time()
    now = int(sec) * 1000 + int(usec) // 1000
    expires_at = int(data["expires_at"])
    held = expires_at > now
    return LeaseStateResponse(
        held=held,
        fencing_token=int(data["fencing"]),
        expires_at_ms=expires_at,
        remaining_ms=max(0, expires_at - now),
    )


@app.post(
    "/v1/results/commit",
    response_model=ResultCommitResponse,
    tags=["results"],
    summary="提交任务结果（租约凭据原子校验 + 防栅序号幂等）",
    responses={
        status.HTTP_409_CONFLICT: {"model": ResultCommitResponse},
    },
)
async def commit_result(
    req: ResultCommitRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> ResultCommitResponse:
    """当前租约持有者提交任务结果。

    租约校验（token + 防栅序号 + 未过期）与结果写入在**同一个 Redis Lua
    脚本**内基于服务器时间原子完成，不可能出现"校验通过后租约易主、
    旧持有者覆盖新结果"。

    - 首次提交（该防栅序号）：写入结果，返回 **200**、`committed=true`、
      `replayed=false` 与首次提交时刻；同脚本内原子向全局结果变更流
      （`GET /v1/results/changes`）追加一条变更。
    - 相同内容重试：返回 **200**、首次结果与 `replayed=true`
      （响应头 `Result-Replay: true`），不写入、不刷新提交时刻、不追加变更。
      内容按规范化 JSON 比较（忽略对象键顺序与空白差异）。
    - 同一防栅序号提交**不同内容**：返回 **409** `reason=result_conflict`，
      已存结果保持不变。
    - 租约不存在 / 凭据不匹配 / 已过期：返回 **409**，
      `reason` 为 `not_found` / `conflict` / `expired`，不写入结果。
    - 租约到期后被新持有者接管（新的防栅序号）时，新凭据可提交新结果并
      覆盖旧结果；结果本身**不设 TTL**，不随租约到期删除。
    """
    canonical = canonical_result_json(req.result)
    kind, reason, fencing, committed_at = await scripts.result_commit(
        keys=[
            lease_key(settings, req.task_key),
            result_key(settings, req.task_key),
            event_stream_key(settings, req.task_key),
            result_changes_key(settings),
        ],
        args=[
            req.token,
            str(req.fencing_token),
            canonical,
            settings.event_retention,
            req.task_key,
            settings.result_change_retention,
        ],
    )
    return build_commit_response(req, response, kind, reason, fencing, committed_at)


def build_commit_response(
    req: ResultCommitRequest | CheckedResultCommitRequest,
    response: Response,
    kind: int,
    reason: str,
    fencing,
    committed_at,
) -> ResultCommitResponse:
    """把结果提交 Lua（普通 / 受保护共享同一返回契约）的结果转为 HTTP 响应。"""
    if kind == 0:
        response.status_code = status.HTTP_409_CONFLICT
        return ResultCommitResponse(
            committed=False,
            task_key=req.task_key,
            fencing_token=int(fencing),
            result=None,
            committed_at_ms=int(committed_at) if committed_at else None,
            reason=reason,
        )

    # committed（首次写入 / 新租约覆盖）与 replayed（相同内容重放）均为 200；
    # 生效结果即请求内容（重放时与已存内容按规范化 JSON 相等）
    resp = ResultCommitResponse(
        committed=True,
        replayed=kind == 2,
        task_key=req.task_key,
        fencing_token=int(fencing),
        result=req.result,
        committed_at_ms=int(committed_at),
    )
    if kind == 2:
        response.headers["Result-Replay"] = "true"
    return resp


@app.post(
    "/v1/results/commit-checked",
    response_model=ResultCommitResponse,
    tags=["results"],
    summary=(
        "携带前置版本快照提交任务结果（租约 + 当前前置列表及版本在同一 Lua "
        "操作内核对，沿用防栅与幂等语义）"
    ),
    responses={
        status.HTTP_409_CONFLICT: {"model": ResultCommitResponse},
    },
)
async def commit_result_checked(
    req: CheckedResultCommitRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> ResultCommitResponse:
    """当前租约持有者携带**前置结果版本快照**提交任务结果。

    典型流程：先 `GET /v1/dependencies/{task_key}/snapshot` 取得按依赖定义
    顺序的可提交快照（`status=snapshot`，含每个直接前置任务键及其已提交
    结果的 `fencing_token`），执行任务后把快照原样放入本请求的 `snapshot`。

    租约校验、前置核对与结果写入在**同一个 Redis Lua 脚本**内原子完成：

    - 先按与 `POST /v1/results/commit` 完全相同的口径校验有效租约
      （token + 防栅序号 + 未过期）；凭据不合法返回 **409**
      `not_found` / `conflict` / `expired`，不核对前置、不写结果。
    - 再核对**当前直接前置列表与各前置结果版本**：当前
      `{prefix}:deps:{task_key}` 必须与快照逐项一致（数量、顺序、键名），
      且每个前置结果 Hash 的 `fencing` 必须等于快照版本。任一不一致
      （拍快照后前置定义被替换、某前置被新租约提交了新版本结果等）返回
      **409** `reason=stale_dependencies`，**不改结果**；调用方应重新拉取
      快照（必要时重算）后再提交。
    - 全部一致时才进入与普通提交**相同**的防栅与幂等写入：首次写入 200
      `committed=true`（同脚本内原子向全局结果变更流
      `GET /v1/results/changes` 追加一条变更）；相同内容重试 200
      `replayed=true`（带头 `Result-Replay: true`），不追加变更；同一防栅
      序号不同内容 409 `result_conflict`。
    - **无前置任务使用空快照**（`snapshot=[]`，亦为缺省值）：无依赖定义
      与空定义的任务均直接通过前置核对，行为与普通提交一致。
    """
    canonical = canonical_result_json(req.result)
    snapshot_deps = [item.task_key for item in req.snapshot]
    snapshot_fencing = [str(item.fencing_token) for item in req.snapshot]
    kind, reason, fencing, committed_at = await scripts.result_commit_checked(
        keys=[
            lease_key(settings, req.task_key),
            result_key(settings, req.task_key),
            event_stream_key(settings, req.task_key),
            result_changes_key(settings),
            dependency_key(settings, req.task_key),
        ],
        args=[
            req.token,
            str(req.fencing_token),
            canonical,
            settings.event_retention,
            req.task_key,
            settings.result_change_retention,
            f"{settings.key_prefix}:result:",
            str(len(req.snapshot)),
            *snapshot_deps,
            *snapshot_fencing,
        ],
    )
    return build_commit_response(req, response, kind, reason, fencing, committed_at)


@app.get(
    "/v1/results/changes",
    response_model=ResultChangePage,
    tags=["results"],
    summary="按全局写入顺序订阅任务结果变更（单次原子读取的游标分页，跨任务键）",
    responses={
        status.HTTP_410_GONE: {"description": "游标早于当前保留的最早变更"},
    },
)
async def list_result_changes(
    scripts: ScriptsDep,
    settings: SettingsDep,
    cursor: str | None = Query(
        default=None,
        description=(
            "分页游标：上次响应返回的 next_cursor（Redis Stream 条目 ID）；"
            "不传表示从当前保留的最早变更开始。"
        ),
    ),
    limit: int = Query(
        default=50,
        ge=1,
        le=200,
        description="单页最多返回的变更条数（1～200，默认 50）",
    ),
) -> ResultChangePage:
    """读取全局任务结果变更流（所有任务键共用一条 Redis Stream），按写入全局顺序
    （Stream 条目 ID 升序）游标分页，供下游顺序处理已提交结果。

    普通提交（`POST /v1/results/commit`）与携带前置快照的提交
    （`POST /v1/results/commit-checked`）仅在**首次写入新结果版本**时，于结果
    写入的同一个 Lua 脚本内原子向该流追加一条变更，含 `task_key`、防栅序号
    （`fencing_token`）、提交时间（`committed_at_ms`）与当次 JSON 结果
    （`result`）。提交失败与相同内容重放**不追加**，故每条变更恰好对应一次
    结果版本推进。全局流默认保留最新 `LEASE_RESULT_CHANGE_RETENTION` 条，
    超出按最旧变更裁剪。

    分页读取由**单个只读 Lua 脚本**在同一次 Redis 原子执行内完成"最早保留
    条目校验 → 按 (cursor, limit] 读取页面 → 恰满一页时探测后续条目决定
    `next_cursor`"：三步共用同一份流快照，并发提交触发的 MAXLEN 裁剪不可能
    落在校验与读取之间，失效游标不会得到 200 而静默漏读。

    - 不传 `cursor`：从当前保留的最早变更开始读（流为空时返回空页）。
    - 传入的 `cursor` **早于当前保留的最早条目**（已被裁剪）时返回 **410**，
      提示游标失效、避免静默漏读；调用方应从当前最早条目重新追赶
      （不带 `cursor`）或改走全量对账。
    """
    if cursor is not None and not _STREAM_ID_PATTERN.match(cursor):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="cursor must be a Redis Stream entry id (<ms>-<seq>)",
        )

    # 单脚本原子读取：游标校验、页面读取与 next_cursor 探测基于同一份流快照
    result = await scripts.result_changes_page(
        keys=[result_changes_key(settings)],
        args=[cursor or "", limit],
    )
    if result[0] == "410":
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail=(
                "cursor is older than the oldest retained change; "
                "restart without a cursor to resume from the oldest "
                "available change"
            ),
        )

    count = result[1]
    changes = [
        ResultChange(
            id=result[2 + 5 * i],
            task_key=result[3 + 5 * i],
            fencing_token=int(result[4 + 5 * i]),
            committed_at_ms=int(result[5 + 5 * i]),
            result=json.loads(result[6 + 5 * i]),
        )
        for i in range(count)
    ]
    next_cursor = result[2 + 5 * count] or None
    return ResultChangePage(changes=changes, next_cursor=next_cursor)


@app.get(
    "/v1/results/{task_key:path}",
    response_model=TaskResultResponse,
    tags=["results"],
    summary="读取任务最近一次已提交结果（持久保留，不随租约到期删除）",
)
async def get_result(
    task_key: str, redis: RedisDep, settings: SettingsDep
) -> TaskResultResponse:
    """返回任务键最近一次已提交结果及其防栅序号与首次提交时刻。

    结果键不设 TTL：租约到期、被接管或释放后结果仍然可读。
    从未提交过结果时返回 200 且 `committed=false`。
    """
    data = await redis.hgetall(result_key(settings, task_key))
    if not data:
        return TaskResultResponse(task_key=task_key, committed=False)
    return TaskResultResponse(
        task_key=task_key,
        committed=True,
        fencing_token=int(data["fencing"]),
        result=json.loads(data["result"]),
        committed_at_ms=int(data["committed_at_ms"]),
    )


@app.post(
    "/v1/dependencies/{task_key:path}",
    response_model=DependencySetResponse,
    tags=["dependencies"],
    summary="保存任务前置依赖定义（去重、限 16 个，Lua 环检测，拒绝时保留原定义）",
    responses={
        status.HTTP_409_CONFLICT: {"model": DependencySetResponse},
    },
)
async def set_dependencies(
    task_key: str,
    req: DependencySetRequest,
    response: Response,
    scripts: ScriptsDep,
    redis: RedisDep,
    settings: SettingsDep,
) -> DependencySetResponse:
    """整体替换任务的前置任务键列表（空列表表示清除定义）。

    - 前置键由请求模型按首次出现顺序**去重**，去重后最多 **16** 个；
      去重后仍超限返回 **422**。包含任务自身（自身依赖）返回 **422**，
      不触达 Redis。
    - 环检测在**单个 Redis Lua 脚本**内基于**现有已持久化定义**完成：
      从每个前置键出发沿现有依赖边遍历，若能回到 `task_key` 则写入后必成环，
      返回 **409** `reason=cycle`；去重后访问节点数超过 **1024** 立即停止，
      返回 **409** `reason=too_many_nodes`。
    - 环 / 超限都发生在任何写入之前，被拒时**原定义原样保留**，
      响应体 `dependencies` 回带当前仍生效的定义。
    - 检测通过后在同一脚本内 `DEL + RPUSH` 原子替换为新定义。
    """
    # 请求模型已按首次出现顺序去重；这里再兜底一次，并检查自身依赖
    deps = list(dict.fromkeys(req.dependencies))
    if task_key in deps:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="task must not depend on itself",
        )

    dkey = dependency_key(settings, task_key)
    result = await scripts.dependency_set(
        keys=[dkey],
        args=[
            f"{settings.key_prefix}:deps:",
            task_key,
            str(len(deps)),
            *deps,
        ],
    )
    # 成功返回 [1]；拒绝返回 [0, reason]
    if not result[0]:
        # 被拒：回读当前定义（未做任何写入，即原定义），便于客户端核对
        current = await redis.lrange(dkey, 0, -1)
        response.status_code = status.HTTP_409_CONFLICT
        return DependencySetResponse(
            saved=False, task_key=task_key, dependencies=current,
            reason=result[1],
        )
    return DependencySetResponse(
        saved=True, task_key=task_key, dependencies=deps
    )


@app.get(
    "/v1/dependencies/{task_key:path}/ready",
    response_model=DependencyReadyResponse,
    tags=["dependencies"],
    summary="查询任务门禁是否就绪（所有直接前置任务均已提交持久结果）",
)
async def dependencies_ready(
    task_key: str, redis: RedisDep, settings: SettingsDep
) -> DependencyReadyResponse:
    """依据持久任务结果判断所有**直接**前置任务是否均已提交。

    - 读取该任务的依赖定义（`{prefix}:deps:{task_key}` List）；
      **没有定义（键不存在）的任务视为可执行**，返回 `ready=true`。
    - 逐个检查前置任务的持久结果键（`{prefix}:result:{task_key}`，
      与 `GET /v1/results/{task_key}` 同源；结果不设 TTL，提交即完成）。
    - 全部前置均已提交结果时 `ready=true`；否则 `ready=false`，
      `pending` 按依赖定义顺序给出尚未完成（未提交结果）的前置键。
    """
    deps = await redis.lrange(dependency_key(settings, task_key), 0, -1)
    if not deps:
        return DependencyReadyResponse(task_key=task_key, ready=True, pending=[])

    async with redis.pipeline(transaction=False) as pipe:
        for dep in deps:
            pipe.exists(result_key(settings, dep))
        exists_flags = await pipe.execute()
    pending = [dep for dep, flag in zip(deps, exists_flags) if not flag]
    return DependencyReadyResponse(
        task_key=task_key, ready=not pending, pending=pending
    )


@app.get(
    "/v1/dependencies/{task_key:path}/snapshot",
    response_model=DependencySnapshotResponse,
    tags=["dependencies"],
    summary=(
        "读取前置结果版本快照（单个 Lua 只读操作：当前直接前置列表及各结果 "
        "fencing_token，按定义顺序）"
    ),
)
async def dependencies_snapshot(
    task_key: str,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> DependencySnapshotResponse:
    """为受保护提交（`POST /v1/results/commit-checked`）生成一致的前置快照。

    在**单个只读 Lua 脚本**内一次性读取依赖定义 List 与每个直接前置结果
    Hash 的 `fencing`（与 `GET /v1/results/{task_key}` 同源的结果版本），
    "读定义 + 读各结果版本"之间不可能插入并发的定义替换或结果提交。

    - 全部直接前置均已提交持久结果：返回 `status=snapshot`，`dependencies`
      按依赖定义顺序给出每项 `{task_key, fencing_token}`，可**原样**放入
      commit-checked 请求的 `snapshot` 字段。
    - 存在未提交结果的前置：返回 `status=pending`，`pending` 按定义顺序
      给出未完成键，**不生成可提交快照**（`dependencies` 为空）；调用方
      应等待前置完成后重新拉取。
    - 没有依赖定义（或空定义）的任务：返回 `status=snapshot` 与空
      `dependencies`，即**空快照**；commit-checked 时用 `snapshot=[]`。
    """
    result = await scripts.dependency_snapshot(
        keys=[],
        args=[
            f"{settings.key_prefix}:deps:",
            f"{settings.key_prefix}:result:",
            task_key,
        ],
    )
    if not result[0]:
        pending = list(result[2:])
        return DependencySnapshotResponse(
            task_key=task_key, status="pending", dependencies=[], pending=pending
        )

    # {1, n, dep_1, fencing_1, dep_2, fencing_2, ...}（n 位于下标 1）
    items = [
        DependencySnapshotItem(task_key=result[2 + 2 * i],
                               fencing_token=result[3 + 2 * i])
        for i in range(result[1])
    ]
    return DependencySnapshotResponse(
        task_key=task_key, status="snapshot", dependencies=items, pending=[]
    )


@app.post(
    "/v1/limits/check",
    response_model=LimitCheckResponse,
    tags=["rate-limits"],
    summary="滑动窗口成本限流检查（通过即扣减，支持 request_id 幂等）",
    responses={
        status.HTTP_429_TOO_MANY_REQUESTS: {"model": LimitCheckResponse},
    },
)
async def check_limit(
    req: LimitCheckRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> LimitCheckResponse:
    """按 Redis 服务器时间统计滑动窗口内的累计成本并原子扣减。

    - 未超限（`used + cost <= limit`）：原子记入本次成本，返回 **200**、
      `remaining` 与 `reset_at_ms`（窗口内最老一笔成本滑出的时刻）。
    - 超限：不扣减，返回 **429** 与 `retry_after_ms`——按记录到期顺序累计
      将释放的成本，首次足以容纳本次 `cost` 的等待毫秒数（同毫秒到期的记录
      合并判断；可能晚于 `reset_at_ms`，最老一笔滑出未必释放足够额度），
      同时携带标准 `Retry-After` 响应头（秒）。

    **可选幂等（request_id）**：请求体携带 `request_id` 时，同一
    `client_key` + `request_id` 在其首次请求的窗口内只判定 / 扣减一次，
    首次结果（无论 200 还是 429）完整记录在 Redis 中；随后携带相同
    `request_id` 的并发或重试请求直接回放首次结果，响应中
    `idempotent_replay=true` 并携带 `Idempotent-Replay: true` 响应头，
    **不会重复扣减、不刷新任何 TTL**。幂等记录的 TTL 等于首次请求的
    `window_ms`，窗口结束后 `request_id` 自动失效、可再次使用。
    重试请求以首次结果为准，不会重新判定参数与额度；需要发起新的逻辑
    请求时请使用新的 `request_id`。不传 `request_id` 时行为与旧版本一致。

    **不同 window_ms 混用**：同一 `client_key` 可交替使用不同的
    `window_ms`；成本记录按服务端允许的最大窗口（`LIMITS_MAX_WINDOW_MS`）
    保留，每次请求只按本次 `window_ms` 筛选累计并判定——短窗口请求不会
    删除仍属于更长窗口的记录，切回长窗口后旧记录仍被正确计入。过期清理
    与限流键的兜底 `PEXPIRE` 同样按最大允许窗口计算。

    "幂等判定 → 清理旧记录 → 按本次窗口筛选累计 → 判定 → 扣减 → 记录响应"
    由单个 Lua 脚本基于 Redis 服务器时间原子执行，同一 `client_key` 的
    并发请求不会超额，相同 `request_id` 的并发请求只有首个完成扣减；
    滑出最大允许窗口的记录随请求自动清理，无流量时键按兜底 `PEXPIRE`
    自动删除。
    """
    window = resolve_window(req.window_ms, settings)
    keys = [limit_key(settings, req.client_key)]
    if req.request_id is not None:
        keys.append(idempotency_key(settings, req.client_key, req.request_id))
    result = await scripts.limit_check(
        keys=keys,
        args=[
            req.limit,
            window,
            req.cost,
            uuid.uuid4().hex,
            settings.limits_max_window_ms,
        ],
    )

    kind = result[0]
    if kind == 2:
        # 幂等命中：回放首次请求记录的完整结果
        _, allowed_flag, remaining, reset_at_ms, retry_after_ms, stored_limit, stored_window = result
        allowed = bool(allowed_flag)
        replay = LimitCheckResponse(
            allowed=allowed,
            client_key=req.client_key,
            request_id=req.request_id,
            idempotent_replay=True,
            limit=stored_limit,
            window_ms=stored_window,
            remaining=remaining,
            reset_at_ms=reset_at_ms,
            retry_after_ms=retry_after_ms,
        )
        response.headers["Idempotent-Replay"] = "true"
        if not allowed:
            response.status_code = status.HTTP_429_TOO_MANY_REQUESTS
            # 标准 Retry-After 以秒为单位，向上取整避免客户端过早重试
            response.headers["Retry-After"] = str(
                max(1, math.ceil(retry_after_ms / 1000))
            )
        return replay

    if kind == 1:
        return LimitCheckResponse(
            allowed=True,
            client_key=req.client_key,
            request_id=req.request_id,
            limit=req.limit,
            window_ms=window,
            remaining=result[1],
            reset_at_ms=result[2],
        )

    remaining, reset_at_ms, retry_after_ms = result[1], result[2], result[3]
    response.status_code = status.HTTP_429_TOO_MANY_REQUESTS
    # 标准 Retry-After 以秒为单位，向上取整避免客户端过早重试
    response.headers["Retry-After"] = str(max(1, math.ceil(retry_after_ms / 1000)))
    return LimitCheckResponse(
        allowed=False,
        client_key=req.client_key,
        request_id=req.request_id,
        limit=req.limit,
        window_ms=window,
        remaining=remaining,
        reset_at_ms=reset_at_ms,
        retry_after_ms=retry_after_ms,
    )


# 按资源键分配加权并发容量 -------------------------------------------------------------


@app.put(
    "/v1/capacity/{resource_key:path}",
    response_model=CapacitySetResponse,
    tags=["capacity"],
    summary="设置资源键的并发容量（低于当前有效占用量时拒绝，保留原配置）",
    responses={
        status.HTTP_409_CONFLICT: {"model": CapacitySetResponse},
    },
)
async def set_capacity(
    resource_key: str,
    req: CapacitySetRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> CapacitySetResponse:
    """设置（整体调整）某资源键允许的有效占用权重总量上限。

    - 调整在**单个 Redis Lua 脚本**内基于服务器时间先清理过期占用、
      核算当前有效占用量，再比较新容量：新容量**低于当前有效占用量**时返回
      **409** `reason=below_active`，**不写入容量键、不动任何占用**，响应体
      `capacity` 回带当前仍生效的旧配置（从未配置时为 `null`）。
    - 新容量不低于有效占用量时原子写入新配置（容量键无 TTL，整体替换），
      返回 200、生效容量与当前有效占用量 `active_weight`。
    - 资源键必须先设置容量才能申请占用；未配置资源键的申请返回
      **409** `not_configured`。
    """
    result = await scripts.capacity_set(
        keys=[
            capacity_key(settings, resource_key),
            slots_key(settings, resource_key),
            slots_used_key(settings, resource_key),
        ],
        args=[str(req.capacity)],
    )
    active_weight = int(result[-1])
    if not result[0]:
        response.status_code = status.HTTP_409_CONFLICT
        old_capacity = result[2]
        return CapacitySetResponse(
            resource_key=resource_key,
            configured=False,
            capacity=int(old_capacity) if old_capacity is not None else None,
            active_weight=active_weight,
            reason=result[1],
        )
    return CapacitySetResponse(
        resource_key=resource_key,
        configured=True,
        capacity=int(result[1]),
        active_weight=active_weight,
    )


@app.post(
    "/v1/capacity/slots/acquire",
    response_model=SlotAcquireResponse,
    tags=["capacity"],
    summary="申请占用处理槽位（可声明权重，原子核算剩余容量，绝不超配）",
    responses={
        status.HTTP_429_TOO_MANY_REQUESTS: {"model": SlotAcquireResponse},
        status.HTTP_409_CONFLICT: {"model": SlotAcquireResponse},
    },
)
async def acquire_slot(
    req: SlotAcquireRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> SlotAcquireResponse:
    """按资源键申请一个带 TTL 的加权处理槽位，供多个工作进程共享有上限的容量。

    - "按 Redis 服务器时间清理过期占用 → 读取容量 → 累计有效占用权重 →
      判定剩余 → 写入新占用"由**单个 Lua 脚本**原子完成，同一资源键的并发申请
      严格串行，**绝不超配**；容量不足时不写入任何占用。
    - 申请可声明整数 `weight`（默认 1，例如批量条数 / GPU 卡数）；
      `weight` 大于当前容量时永远无法满足，返回 **409**。
    - 成功返回 **200**、本次占用的**独立凭据** `credential`（服务端生成，
      不透明，仅用于续期 / 释放）与 `expires_at_ms`（Redis 服务器时钟）。
    - 资源键未设置容量：返回 **409** `not_configured`；
      剩余容量不足：返回 **429** `capacity_exhausted` 与 `retry_after_ms`
      （至最早一笔占用到期），并携带标准 `Retry-After` 响应头（秒）。
    - 占用到期后由后续操作按服务器时间自动清理，其槽位自动回收。
    """
    ttl = resolve_ttl(req.ttl_ms, settings)
    nonce = uuid.uuid4().hex
    ckey = capacity_key(settings, req.resource_key)
    skey = slots_key(settings, req.resource_key)
    result = await scripts.slot_acquire(
        keys=[ckey, skey, slots_used_key(settings, req.resource_key)],
        args=[str(req.weight), nonce, str(ttl)],
    )

    if result[0]:
        # {1, 凭据, expires_at_ms, 剩余容量, 容量}
        _, credential, expires_at, remaining, capacity = result
        capacity = int(capacity)
        remaining = int(remaining)
        return SlotAcquireResponse(
            acquired=True,
            resource_key=req.resource_key,
            credential=credential,
            weight=req.weight,
            expires_at_ms=int(expires_at),
            capacity=capacity,
            active_weight=capacity - remaining,
            remaining=remaining,
        )

    reason = result[1]
    # 未配置容量 -> 409（调用方须先 PUT 容量）
    if reason == "not_configured":
        response.status_code = status.HTTP_409_CONFLICT
        return SlotAcquireResponse(
            acquired=False,
            resource_key=req.resource_key,
            weight=req.weight,
            reason=reason,
        )

    # capacity_exhausted：
    # {0, reason, remaining, 最早到期, retry_after_ms, capacity}
    remaining = int(result[2])
    retry_after_ms = int(result[4])
    capacity = int(result[5])
    active_weight = capacity - remaining
    # weight 超过容量上限：永远无法满足，按 409（而非可重试的 429）返回
    if req.weight > capacity:
        response.status_code = status.HTTP_409_CONFLICT
        return SlotAcquireResponse(
            acquired=False,
            resource_key=req.resource_key,
            weight=req.weight,
            capacity=capacity,
            active_weight=active_weight,
            remaining=remaining,
            reason="weight_exceeds_capacity",
        )
    # 瞬时剩余容量不足：429，可在最早一笔占用到期后重试
    response.status_code = status.HTTP_429_TOO_MANY_REQUESTS
    # 标准 Retry-After 以秒为单位，向上取整避免客户端过早重试
    response.headers["Retry-After"] = str(max(1, math.ceil(retry_after_ms / 1000)))
    return SlotAcquireResponse(
        acquired=False,
        resource_key=req.resource_key,
        weight=req.weight,
        capacity=capacity,
        active_weight=active_weight,
        remaining=remaining,
        retry_after_ms=retry_after_ms,
        reason=reason,
    )


@app.post(
    "/v1/capacity/slots/renew",
    response_model=SlotRenewResponse,
    tags=["capacity"],
    summary="续期占用（仅接受该占用的凭据，过期 / 旧凭据无效）",
    responses={
        status.HTTP_409_CONFLICT: {"model": SlotRenewResponse},
    },
)
async def renew_slot(
    req: SlotRenewRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> SlotRenewResponse:
    """续期某个占用的到期时间。

    - 续期只认申请时返回的**独立凭据**：脚本先按 Redis 服务器时间清理过期占用，
      凭据对应的 member 仍存在才把到期时刻刷新为 `now + ttl_ms`。
    - 凭据错误、或占用已过期（含被服务器清理）均返回 **409** `not_found`，
      **不做任何修改**；过期 / 旧凭据的续期不可能影响过期后新申请的新占用。
    """
    ttl = resolve_ttl(req.ttl_ms, settings)
    result = await scripts.slot_renew(
        keys=[
            slots_key(settings, req.resource_key),
            slots_used_key(settings, req.resource_key),
        ],
        args=[
            req.credential,
            str(ttl),
            capacity_key(settings, req.resource_key),
        ],
    )
    # 成功 {1, 'ok', expires_at_ms, remaining}；失败 {0, 'not_found', 0, remaining}
    if result[0]:
        return SlotRenewResponse(
            renewed=True,
            credential=req.credential,
            expires_at_ms=int(result[2]),
            remaining=int(result[3]),
        )
    response.status_code = status.HTTP_409_CONFLICT
    return SlotRenewResponse(
        renewed=False,
        credential=req.credential,
        remaining=int(result[3]),
        reason=result[1],
    )


@app.post(
    "/v1/capacity/slots/release",
    response_model=SlotReleaseResponse,
    tags=["capacity"],
    summary="释放占用（仅接受该占用的凭据，幂等）",
)
async def release_slot(
    req: SlotReleaseRequest,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> SlotReleaseResponse:
    """提前释放占用、归还其权重槽位。

    - 只认申请时返回的**独立凭据**：脚本先按服务器时间清理过期占用，
      凭据对应的 member 仍存在才删除并归还权重。
    - 凭据错误或占用已过期时幂等忽略（`released=false`，`weight=0`），
      旧持有者在占用过期后的释放不会误删新占用（新占用是全新凭据）。
    """
    result = await scripts.slot_release(
        keys=[
            slots_key(settings, req.resource_key),
            slots_used_key(settings, req.resource_key),
        ],
        args=[req.credential, capacity_key(settings, req.resource_key)],
    )
    # {是否释放(1/0), 释放权重, 剩余容量}
    return SlotReleaseResponse(
        released=bool(result[0]),
        weight=int(result[1]),
        remaining=int(result[2]),
    )
