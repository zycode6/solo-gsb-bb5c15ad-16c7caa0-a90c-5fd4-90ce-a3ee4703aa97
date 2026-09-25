"""API 请求 / 响应模型。"""
import json
from typing import Annotated, Any

from pydantic import BaseModel, Field, field_validator, model_validator

BATCH_MIN_SIZE = 2
BATCH_MAX_SIZE = 32

# 单个任务允许声明的前置任务键上限（保存前去重）
DEPENDENCY_MAX_COUNT = 16

TaskKey = Annotated[str, Field(min_length=1, max_length=512)]

# 幂等键：仅允许 URL/Header 安全字符，便于客户端通过日志 / 链路追踪传递
REQUEST_ID_PATTERN = r"^[A-Za-z0-9._:-]+$"
RequestId = Annotated[
    str,
    Field(
        min_length=1,
        max_length=128,
        pattern=REQUEST_ID_PATTERN,
        examples=["req-7f3a2c91"],
    ),
]


class AcquireRequest(BaseModel):
    task_key: str = Field(
        min_length=1, max_length=512, examples=["video-transcode:order-123"]
    )
    ttl_ms: int | None = Field(
        default=None, gt=0, description="租约 TTL（毫秒），缺省使用服务端默认值"
    )


class AcquireResponse(BaseModel):
    acquired: bool
    task_key: str
    token: str | None = Field(default=None, description="所有权令牌，仅成功时返回")
    fencing_token: int | None = Field(
        default=None, description="防栅序号，单调递增"
    )
    expires_at_ms: int | None = Field(
        default=None, description="过期时间（Redis 服务器时钟，Unix 毫秒）"
    )


class AcquireBatchRequest(BaseModel):
    task_keys: list[TaskKey] = Field(
        min_length=BATCH_MIN_SIZE,
        max_length=BATCH_MAX_SIZE,
        examples=[["video-transcode:order-123", "video-transcode:order-456"]],
        description=(
            f"一批任务键，{BATCH_MIN_SIZE}～{BATCH_MAX_SIZE} 个且不允许重复"
        ),
    )
    ttl_ms: int | None = Field(
        default=None, gt=0, description="统一租约 TTL（毫秒），缺省使用服务端默认值"
    )

    @field_validator("task_keys")
    @classmethod
    def reject_duplicates(cls, v: list[str]) -> list[str]:
        if len(set(v)) != len(v):
            raise ValueError("task_keys must be unique")
        return v


class AcquiredLease(BaseModel):
    """批量申请结果中的单个任务凭据，顺序与请求的 task_keys 一致。"""

    task_key: str
    token: str = Field(description="所有权令牌")
    fencing_token: int = Field(description="防栅序号，单调递增")
    expires_at_ms: int = Field(
        description="过期时间（Redis 服务器时钟，Unix 毫秒），整批同一基准"
    )


class AcquireBatchResponse(BaseModel):
    acquired: bool
    leases: list[AcquiredLease] = Field(
        default_factory=list,
        description="全部成功时每个任务的独立凭据，按输入顺序返回",
    )
    conflicting_task_keys: list[str] = Field(
        default_factory=list,
        description="冲突时仍被占用的任务键（全部冲突键，按输入顺序）",
    )


class RenewRequest(BaseModel):
    task_key: str = Field(min_length=1, max_length=512)
    token: str = Field(min_length=1)
    fencing_token: int = Field(ge=0)
    ttl_ms: int | None = Field(
        default=None, gt=0, description="新的 TTL（毫秒），缺省使用服务端默认值"
    )


class RenewResponse(BaseModel):
    renewed: bool
    expires_at_ms: int | None = None
    reason: str | None = Field(
        default=None, description="失败原因：not_found / conflict / expired"
    )


class RenewBatchItem(BaseModel):
    """批量续租中的单个任务凭据，顺序即响应顺序。"""

    task_key: str = Field(min_length=1, max_length=512)
    token: str = Field(min_length=1, description="申请时返回的所有权令牌")
    fencing_token: int = Field(ge=0, description="申请时返回的防栅序号")


class RenewBatchRequest(BaseModel):
    items: list[RenewBatchItem] = Field(
        min_length=BATCH_MIN_SIZE,
        max_length=BATCH_MAX_SIZE,
        description=(
            f"一批待续租任务，{BATCH_MIN_SIZE}～{BATCH_MAX_SIZE} 个，"
            "task_key 不允许重复；各自携带 token 与 fencing_token"
        ),
        examples=[
            [
                {"task_key": "video-transcode:order-123",
                 "token": "9f3c...", "fencing_token": 1},
                {"task_key": "video-transcode:order-456",
                 "token": "a1b2...", "fencing_token": 1},
            ]
        ],
    )
    ttl_ms: int | None = Field(
        default=None, gt=0, description="全批统一的新 TTL（毫秒），缺省使用服务端默认值"
    )

    @field_validator("items")
    @classmethod
    def reject_duplicate_task_keys(cls, v: list[RenewBatchItem]) -> list[RenewBatchItem]:
        keys = [item.task_key for item in v]
        if len(set(keys)) != len(keys):
            raise ValueError("task_key in items must be unique")
        return v


class RenewBatchLease(BaseModel):
    """批量续租成功时的单项结果，顺序与请求的 items 一致。"""

    task_key: str
    expires_at_ms: int = Field(
        description="新的过期时间（Redis 服务器时钟，Unix 毫秒），整批同一基准"
    )


class RenewBatchFailure(BaseModel):
    """批量续租失败时的单项失败原因，顺序与请求的 items 一致。"""

    index: int = Field(description="失败项在请求 items 中的下标（从 0 开始）")
    task_key: str
    reason: str = Field(
        description="失败原因：not_found（租约不存在）/ conflict（令牌或防栅序号不匹配）/ expired（租约已过期）"
    )


class RenewBatchResponse(BaseModel):
    renewed: bool
    leases: list[RenewBatchLease] = Field(
        default_factory=list,
        description="全部成功时按输入顺序给出每项新的 expires_at_ms",
    )
    failures: list[RenewBatchFailure] = Field(
        default_factory=list,
        description="整批失败时逐项失败原因（仅失败项，按输入顺序）",
    )


class ReleaseRequest(BaseModel):
    task_key: str = Field(min_length=1, max_length=512)
    token: str = Field(min_length=1)
    fencing_token: int = Field(ge=0)


class TransferRequest(BaseModel):
    task_key: str = Field(
        min_length=1, max_length=512, examples=["video-transcode:order-123"]
    )
    token: str = Field(min_length=1, description="当前持有者的所有权令牌")
    fencing_token: int = Field(ge=0, description="当前持有者的防栅序号")


class TransferResponse(BaseModel):
    transferred: bool
    task_key: str
    token: str | None = Field(
        default=None, description="服务端生成的新所有权令牌，仅成功时返回"
    )
    fencing_token: int | None = Field(
        default=None, description="交接后递增的新防栅序号，单调递增"
    )
    expires_at_ms: int | None = Field(
        default=None,
        description=(
            "过期时间（Redis 服务器时钟，Unix 毫秒）；交接保留原过期时刻，不续期"
        ),
    )
    reason: str | None = Field(
        default=None,
        description="失败原因：not_found（租约不存在）/ conflict（令牌或防栅序号不匹配）/ expired（租约已过期）",
    )


class ReleaseResponse(BaseModel):
    released: bool = Field(description="是否真正删除了租约；不匹配时为 false（幂等）")


class ValidateRequest(BaseModel):
    task_key: str = Field(min_length=1, max_length=512)
    token: str = Field(min_length=1)
    fencing_token: int = Field(ge=0)


class ValidateResponse(BaseModel):
    valid: bool
    remaining_ms: int = Field(default=0, description="租约剩余毫秒数，无效时为 0")


class LeaseStateResponse(BaseModel):
    """租约状态查询（不暴露所有权令牌）。"""

    held: bool
    fencing_token: int | None = None
    expires_at_ms: int | None = None
    remaining_ms: int = 0


class LeaseEvent(BaseModel):
    """单个租约审计事件。"""

    id: str = Field(description="Redis Stream 条目 ID（<毫秒时间>-<序号>），即分页游标")
    op: str = Field(
        description=(
            "操作类型：acquire / acquire_batch / renew / renew_batch / "
            "transfer / release / result_commit"
        )
    )
    result: str = Field(
        description=(
            "操作结果：acquire(_batch) 为 acquired / conflict；"
            "renew(_batch) 为 renewed / not_found / conflict / expired；"
            "transfer 为 transferred / not_found / conflict / expired；"
            "release 为 released / ignored；"
            "result_commit 为 committed / replayed / result_conflict / "
            "stale_dependencies / not_found / conflict / expired"
        )
    )
    fencing_token: int | None = Field(
        default=None, description="事件关联的防栅序号；无关联租约时为 null"
    )
    time_ms: int = Field(description="事件落账时刻（Redis 服务器时钟，Unix 毫秒）")
    batch_size: int | None = Field(
        default=None, description="仅批量申请 / 批量续租事件携带：该批次的任务数量"
    )


class LeaseEventPage(BaseModel):
    """租约审计事件分页结果（按事件 ID 升序，即旧 -> 新）。"""

    task_key: str
    events: list[LeaseEvent]
    next_cursor: str | None = Field(
        default=None,
        description=(
            "下一页游标（本页最后一条事件 ID）；本页事件数不足 limit 或没有"
            "更多事件时为 null。原样作为下次请求的 cursor 参数即可继续翻页。"
        ),
    )


def canonical_result_json(value: Any) -> str:
    """规范化结果 JSON：排序对象键、紧凑分隔符。

    幂等重放与内容冲突判定均以规范化后的字符串比较，因此语义相同的 JSON
    （仅对象键顺序或空白不同）视为相同内容；数字 / 字符串等类型差异仍算不同。
    """
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class ResultCommitRequest(BaseModel):
    task_key: str = Field(
        min_length=1, max_length=512, examples=["video-transcode:order-123"]
    )
    token: str = Field(min_length=1, description="当前有效租约的所有权令牌")
    fencing_token: int = Field(
        ge=0, description="当前有效租约的防栅序号，作为本次结果的提交版本"
    )
    result: Any = Field(
        description=(
            "任意 JSON 值（对象 / 数组 / 字符串 / 数字 / 布尔 / null），"
            "由任务持有者写入。内容按规范化 JSON 比较：仅对象键顺序或空白"
            "差异视为相同内容。"
        ),
        examples=[{"status": "done", "output_url": "https://...", "frames": 128}],
    )


class TaskResult(BaseModel):
    """某次已提交的任务结果。"""

    task_key: str
    fencing_token: int = Field(description="提交该结果的租约防栅序号")
    result: Any = Field(description="提交时写入的 JSON 结果（原样返回）")
    committed_at_ms: int = Field(
        description="首次提交时刻（Redis 服务器时钟，Unix 毫秒）；重放不刷新"
    )


class ResultCommitResponse(BaseModel):
    committed: bool = Field(
        description="true 表示请求凭据为当前有效租约（首次写入或相同内容重放均为 true）"
    )
    replayed: bool = Field(
        default=False,
        description=(
            "true 表示同一防栅序号已提交过且内容一致：本次为重放，"
            "返回首次结果且不改动任何数据，响应头同时带 Result-Replay: true"
        ),
    )
    task_key: str
    fencing_token: int
    result: Any = Field(
        default=None,
        description="生效中的结果；凭据无效等未提交场景为 null",
    )
    committed_at_ms: int | None = Field(
        default=None,
        description="结果首次提交时刻（重放不刷新）；凭据无效且无已存结果时为 null",
    )
    reason: str | None = Field(
        default=None,
        description=(
            "仅失败（409）时返回：not_found（租约不存在）/ conflict"
            "（令牌或防栅序号不匹配）/ expired（租约已过期）/ "
            "result_conflict（同一防栅序号已提交不同内容）/ "
            "stale_dependencies（仅 commit-checked：提交时的前置列表或某前置"
            "结果版本与快照不一致）"
        ),
    )


class TaskResultResponse(BaseModel):
    """GET 任务结果：返回最近一次已提交结果（结果不随租约到期删除）。"""

    task_key: str
    committed: bool = Field(
        description="该任务键是否曾提交过结果；false 时其余字段为空"
    )
    fencing_token: int | None = Field(
        default=None, description="最近一次结果对应的防栅序号"
    )
    result: Any = Field(default=None, description="最近一次提交的 JSON 结果")
    committed_at_ms: int | None = Field(
        default=None, description="最近一次结果首次提交时刻（Unix 毫秒）"
    )


class ResultChange(BaseModel):
    """全局任务结果变更流中的单条变更（一次新结果版本写入）。"""

    id: str = Field(
        description="Redis Stream 条目 ID（<毫秒时间>-<序号>），即分页游标"
    )
    task_key: str = Field(description="提交结果的任务键")
    fencing_token: int = Field(
        description="该结果对应的防栅序号（结果版本）"
    )
    committed_at_ms: int = Field(
        description="结果提交时刻（Redis 服务器时钟，Unix 毫秒）"
    )
    result: Any = Field(description="当次提交的 JSON 结果（规范化后原样返回）")


class ResultChangePage(BaseModel):
    """全局结果变更分页结果（按 Stream 条目 ID 升序，即全局写入顺序）。"""

    changes: list[ResultChange]
    next_cursor: str | None = Field(
        default=None,
        description=(
            "下一页游标（本页最后一条变更 ID）；本页变更数不足 limit 或没有"
            "更多变更时为 null。原样作为下次请求的 cursor 参数即可继续翻页。"
        ),
    )


class LimitCheckRequest(BaseModel):
    client_key: str = Field(
        min_length=1,
        max_length=512,
        examples=["tenant-42:send-sms"],
        description="限流对象标识（租户 / 用户 / API 等任意维度）",
    )
    request_id: RequestId | None = Field(
        default=None,
        description=(
            "可选幂等标识。同一 client_key + 同一 request_id 在其首次请求的"
            "窗口内只扣减一次：后续携带该标识的并发 / 重试请求直接返回首次"
            "判定结果（200 或 429）。窗口结束后标识自动失效，可再次使用。"
            "不传时行为与旧版本完全一致（每次请求正常判定扣减）。"
        ),
    )
    limit: int = Field(
        ge=1,
        le=1_000_000_000,
        examples=[10],
        description="单个窗口内允许累计的成本上限",
    )
    window_ms: int = Field(
        ge=1,
        examples=[1000],
        description=(
            "滑动窗口长度（毫秒）。同一 client_key 可交替使用不同窗口："
            "成本记录按服务端允许的最大窗口保留，每次请求只按本次窗口判定"
        ),
    )
    cost: int = Field(
        default=1,
        ge=1,
        le=1_000_000_000,
        examples=[1],
        description="本次请求消耗的成本；不得大于 limit",
    )

    @model_validator(mode="after")
    def cost_within_limit(self) -> "LimitCheckRequest":
        if self.cost > self.limit:
            raise ValueError("cost must not exceed limit")
        return self


class LimitCheckResponse(BaseModel):
    allowed: bool = Field(description="是否放行（false 时 HTTP 状态码为 429）")
    client_key: str
    request_id: str | None = Field(
        default=None,
        description="回显请求携带的幂等标识；未携带时为 null",
    )
    idempotent_replay: bool = Field(
        default=False,
        description=(
            "true 表示本次响应直接取自同一 request_id 的首次判定结果"
            "（未重复扣减）；响应头同时给出 Idempotent-Replay: true"
        ),
    )
    limit: int = Field(description="窗口内允许的成本上限")
    window_ms: int = Field(description="本次判定使用的滑动窗口长度（毫秒）")
    remaining: int = Field(
        ge=0,
        description=(
            "放行后窗口内剩余可用成本；超限时为不扣减情况下的剩余值"
            "（不同 window_ms 混用致窗口内累计已超限时为 0）"
        ),
    )
    reset_at_ms: int = Field(
        description=(
            "窗口内最老一笔成本滑出、可再获得配额的时刻"
            "（Redis 服务器时钟，Unix 毫秒）"
        )
    )
    retry_after_ms: int = Field(
        default=0,
        ge=0,
        description=(
            "仅超限时返回：建议等待毫秒数。按记录到期顺序累计释放的成本，"
            "首次足以容纳本次 cost 的等待时长（同毫秒到期的记录合并判断）；"
            "可能晚于 reset_at_ms——最老一笔滑出未必释放足够额度"
        ),
    )


class DependencySetRequest(BaseModel):
    """保存任务的前置依赖定义（整体替换；空列表表示清除定义）。"""

    dependencies: list[TaskKey] = Field(
        examples=[["video-transcode:order-123", "video-transcode:order-456"]],
        description=(
            f"前置任务键列表，按首次出现顺序去重后最多 {DEPENDENCY_MAX_COUNT} 个；"
            "是否依赖任务自身由服务端按路径中的 task_key 判定（自身依赖返回 422）。"
            "空列表表示清除该任务的依赖定义。"
        ),
    )

    @field_validator("dependencies")
    @classmethod
    def dedupe_and_bound(cls, v: list[str]) -> list[str]:
        # 服务端按首次出现顺序去重；去重后仍超过上限才拒绝（422）
        deduped = list(dict.fromkeys(v))
        if len(deduped) > DEPENDENCY_MAX_COUNT:
            raise ValueError(
                f"dependencies must contain at most {DEPENDENCY_MAX_COUNT} "
                f"unique task keys"
            )
        return deduped


class DependencySetResponse(BaseModel):
    """保存依赖定义的结果；冲突（成环 / 遍历超限）时回带当前已保存定义。"""

    saved: bool = Field(
        description="true 表示定义已整体替换；false 表示被拒绝，原定义保持不变",
    )
    task_key: str
    dependencies: list[str] = Field(
        default_factory=list,
        description=(
            "保存成功时为本次写入的定义（去重后、按请求顺序）；"
            "被拒绝时为当前仍生效的旧定义（无定义时为空列表）"
        ),
    )
    reason: str | None = Field(
        default=None,
        description=(
            "仅失败（409）时返回：cycle（写入后将形成依赖环，含自身依赖）"
            " / too_many_nodes（环检测遍历节点数超过 1024 上限）"
        ),
    )


class DependencyReadyResponse(BaseModel):
    """任务门禁就绪判定：全部直接前置任务都已提交持久结果时可执行。"""

    task_key: str
    ready: bool = Field(
        description=(
            "全部直接前置任务均已提交结果时为 true；"
            "没有依赖定义的任务视为可执行，同样为 true"
        ),
    )
    pending: list[str] = Field(
        default_factory=list,
        description="尚未提交结果的直接前置任务键（按依赖定义顺序去重）；为空表示就绪",
    )


class DependencySnapshotItem(BaseModel):
    """可提交快照中的单个前置：前置任务键及其已提交结果的版本（防栅序号）。"""

    task_key: TaskKey = Field(description="直接前置任务键")
    fencing_token: int = Field(
        ge=1,
        description=(
            "拍快照时该前置已提交结果的版本（结果 Hash 的 fencing，"
            "与 GET /v1/results/{task_key} 同源）；前置结果被新租约提交覆盖时递增"
        ),
    )


class DependencySnapshotResponse(BaseModel):
    """前置结果版本快照（GET snapshot）。

    status=snapshot 时 dependencies 按依赖定义顺序给出当前直接前置及其结果
    版本，可原样放入 POST /v1/results/commit-checked 的 snapshot 字段；
    status=pending 时存在未提交结果的前置，dependencies 为空（不生成
    可提交快照），pending 按定义顺序给出未完成键。无依赖定义的任务返回
    status=snapshot 与空 dependencies（空快照）。
    """

    task_key: str
    status: str = Field(
        description="snapshot（全部前置已完成，可提交）/ pending（有未完成前置）"
    )
    dependencies: list[DependencySnapshotItem] = Field(
        default_factory=list,
        description=(
            "status=snapshot 时按依赖定义顺序给出各前置任务键及其结果版本；"
            "无前置为空列表（空快照）；status=pending 时为空列表"
        ),
    )
    pending: list[str] = Field(
        default_factory=list,
        description="status=pending 时按定义顺序给出尚未提交结果的前置键；否则为空",
    )


class CheckedResultCommitRequest(BaseModel):
    """携带前置版本快照的结果提交请求。"""

    task_key: str = Field(
        min_length=1, max_length=512, examples=["video-transcode:order-123"]
    )
    token: str = Field(min_length=1, description="当前有效租约的所有权令牌")
    fencing_token: int = Field(
        ge=0, description="当前有效租约的防栅序号，作为本次结果的提交版本"
    )
    result: Any = Field(
        description=(
            "任意 JSON 值（对象 / 数组 / 字符串 / 数字 / 布尔 / null），"
            "由任务持有者写入。内容按规范化 JSON 比较：仅对象键顺序或空白"
            "差异视为相同内容。"
        ),
        examples=[{"status": "done", "output_url": "https://...", "frames": 128}],
    )
    snapshot: list[DependencySnapshotItem] = Field(
        default_factory=list,
        description=(
            "GET /v1/dependencies/{task_key}/snapshot 返回的可提交快照"
            "（按定义顺序的前置键及结果版本），原样带回即可；"
            "无前置任务使用空快照（空列表，同时也是缺省值）"
        ),
    )

    @field_validator("snapshot")
    @classmethod
    def dedupe_and_bound_snapshot(cls, v: list[DependencySnapshotItem]):
        # 快照必须来自 GET snapshot：同一前置不应出现两次；数量与依赖定义上限一致
        keys = [item.task_key for item in v]
        if len(set(keys)) != len(keys):
            raise ValueError("snapshot task_key entries must be unique")
        if len(v) > DEPENDENCY_MAX_COUNT:
            raise ValueError(
                f"snapshot must contain at most {DEPENDENCY_MAX_COUNT} entries"
            )
        return v


# 按资源键分配并发容量 ----------------------------------------------------------------

# 单个资源键允许配置的容量 / 单次申请声明的权重上限（权重单位）
CAPACITY_MAX_VALUE = 1_000_000_000

# 凭据形如 "<uuid32>:<weight>"，预留少量余量
CREDENTIAL_MAX_LENGTH = 64

ResourceKey = Annotated[str, Field(min_length=1, max_length=512)]
Credential = Annotated[str, Field(min_length=1, max_length=CREDENTIAL_MAX_LENGTH)]


class CapacitySetRequest(BaseModel):
    """设置（整体调整）资源键的并发容量（整数权重上限）。"""

    capacity: int = Field(
        ge=1,
        le=CAPACITY_MAX_VALUE,
        examples=[10],
        description=(
            "该资源键允许的有效占用权重总量上限（正整数）。"
            "调整为低于当前有效占用量的值会被拒绝（409），原配置保持不变。"
        ),
    )


class CapacitySetResponse(BaseModel):
    resource_key: str
    configured: bool = Field(
        description="true 表示容量已写入；false 表示被拒绝（低于当前有效占用量），原配置不变"
    )
    capacity: int | None = Field(
        default=None,
        description=(
            "成功时为本次写入的容量；被拒绝时为当前仍生效的旧容量"
            "（资源键从未配置时为 null）"
        ),
    )
    active_weight: int = Field(
        ge=0,
        description="按 Redis 服务器时间清理过期占用后，当前有效占用的权重总量",
    )
    reason: str | None = Field(
        default=None,
        description="仅失败（409）时返回：below_active（新容量低于当前有效占用量）",
    )


class SlotAcquireRequest(BaseModel):
    resource_key: ResourceKey = Field(
        examples=["gpu-pool"],
        description="容量资源键；多个工作进程按同一资源键共享有上限的处理槽位",
    )
    weight: int = Field(
        default=1,
        ge=1,
        le=CAPACITY_MAX_VALUE,
        examples=[1],
        description="本次申请声明的占用权重（正整数）；不得大于当前容量，否则 409",
    )
    ttl_ms: int | None = Field(
        default=None, gt=0, description="占用 TTL（毫秒），缺省使用服务端默认值"
    )


class SlotAcquireResponse(BaseModel):
    acquired: bool = Field(description="是否申请成功（false 时 HTTP 状态码为 409）")
    resource_key: str
    credential: str | None = Field(
        default=None,
        description=(
            "本次占用的独立凭据（服务端生成，仅成功时返回）；"
            "续期 / 释放必须原样携带，过期或旧凭据不可用"
        ),
    )
    weight: int = Field(ge=0, description="本次申请声明的占用权重")
    expires_at_ms: int | None = Field(
        default=None, description="到期时间（Redis 服务器时钟，Unix 毫秒）"
    )
    capacity: int | None = Field(
        default=None, description="该资源键当前配置的容量；未配置时为 null"
    )
    active_weight: int = Field(
        default=0, ge=0, description="清理过期占用后当前有效占用权重总量（不含被拒本次）"
    )
    remaining: int = Field(
        default=0,
        ge=0,
        description="放行后剩余容量；超限时为不占用情况下的剩余值",
    )
    retry_after_ms: int = Field(
        default=0,
        ge=0,
        description="仅容量不足时返回：建议等待毫秒数（至最早一笔占用到期）",
    )
    reason: str | None = Field(
        default=None,
        description=(
            "仅失败（409）时返回：not_configured（资源键尚未设置容量）/ "
            "capacity_exhausted（剩余容量不足）"
        ),
    )


class SlotRenewRequest(BaseModel):
    resource_key: ResourceKey
    credential: Credential = Field(description="申请成功时返回的独立凭据（原样携带）")
    ttl_ms: int | None = Field(
        default=None, gt=0, description="新的 TTL（毫秒），缺省使用服务端默认值"
    )


class SlotRenewResponse(BaseModel):
    renewed: bool
    credential: str | None = Field(
        default=None, description="回显请求携带的凭据；无效时也原样回显"
    )
    expires_at_ms: int | None = Field(
        default=None, description="续期后的新到期时间（Redis 服务器时钟，Unix 毫秒）"
    )
    remaining: int = Field(
        default=0, ge=0, description="续期后该资源键的剩余容量"
    )
    reason: str | None = Field(
        default=None,
        description="仅失败（409）时返回：not_found（凭据不存在或已过期）",
    )


class SlotReleaseRequest(BaseModel):
    resource_key: ResourceKey
    credential: Credential = Field(description="申请成功时返回的独立凭据（原样携带）")


class SlotReleaseResponse(BaseModel):
    released: bool = Field(
        description="是否真正删除了占用；凭据不匹配 / 已过期时为 false（幂等）"
    )
    weight: int = Field(
        default=0, ge=0, description="本次释放归还的占用权重（未释放为 0）"
    )
    remaining: int = Field(
        default=0, ge=0, description="释放后该资源键的剩余容量"
    )
