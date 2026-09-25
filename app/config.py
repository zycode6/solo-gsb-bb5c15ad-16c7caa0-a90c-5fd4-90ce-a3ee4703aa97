"""应用配置：全部可通过环境变量覆盖。"""
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Redis 连接串
    redis_url: str = Field(
        default="redis://localhost:6379/0", validation_alias="REDIS_URL"
    )
    # Redis 键前缀，便于多环境共用一个实例
    key_prefix: str = Field(default="lease", validation_alias="LEASE_KEY_PREFIX")
    # TTL 合法区间（毫秒），超出范围的请求会被拒绝
    min_ttl_ms: int = Field(default=100, validation_alias="LEASE_MIN_TTL_MS")
    max_ttl_ms: int = Field(default=300_000, validation_alias="LEASE_MAX_TTL_MS")
    # 请求未携带 ttl_ms 时使用的默认值
    default_ttl_ms: int = Field(
        default=10_000, validation_alias="LEASE_DEFAULT_TTL_MS"
    )
    # 租约审计 Stream 每个任务键保留的最新事件条数；超出按最旧事件裁剪。
    # 0 表示不限制条数（不裁剪）。
    event_retention: int = Field(
        default=1_000, ge=0, validation_alias="LEASE_EVENT_RETENTION"
    )
    # 全局任务结果变更 Stream（所有任务键共用一条）保留的最新变更条数，
    # 供下游按全局写入顺序订阅已提交结果；超出按最旧变更裁剪。
    # 0 表示不限制条数（不裁剪）。
    result_change_retention: int = Field(
        default=10_000, ge=0, validation_alias="LEASE_RESULT_CHANGE_RETENTION"
    )
    # 限流键前缀，与租约键前缀隔离，便于多环境共用一个实例
    limit_key_prefix: str = Field(default="ratelimit", validation_alias="LIMIT_KEY_PREFIX")
    # 限流幂等记录键前缀，与限流 ZSet 键隔离
    # （幂等记录为 Hash，键名采用长度定界，不同 client_key 间不可能碰撞）
    limit_idem_key_prefix: str = Field(
        default="ratelimit-idem", validation_alias="LIMIT_IDEM_KEY_PREFIX"
    )
    # window_ms 合法区间（毫秒），超出范围的限流请求会被拒绝
    limits_min_window_ms: int = Field(
        default=1, validation_alias="LIMITS_MIN_WINDOW_MS"
    )
    limits_max_window_ms: int = Field(
        default=86_400_000, validation_alias="LIMITS_MAX_WINDOW_MS"
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
