"""运行配置，全部可由环境变量覆盖（Docker Compose 注入）。"""
from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", case_sensitive=False, extra="ignore")

    # ---- 数据库 ----
    database_url: str = Field(
        default="postgresql+psycopg2://weigh:weigh@localhost:5432/weighbridge",
        description="SQLAlchemy 数据库连接串",
    )

    # ---- Redpanda / Kafka ----
    kafka_bootstrap: str = Field(default="localhost:9092")
    topic: str = Field(default="weighbridge-events")
    topic_partitions: int = Field(default=2)
    consumer_group: str = Field(default="weighbridge-consumer-group")
    # 演示用生产者的默认交付超时
    produce_timeout_seconds: float = Field(default=10.0)
    # 轮询间隔
    poll_timeout_seconds: float = Field(default=1.0)

    # ---- 业务规则 ----
    # 箱号长度上下限
    container_no_min_len: int = 4
    container_no_max_len: int = 17
    # 重量绝对值上限（整数千克），防止离谱值
    max_weight_kg: int = 1_000_000_000

    # ---- 运行控制 ----
    consumer_enabled: bool = Field(default=True)
    instance_id: str = Field(default="instance-1")
    # 消费线程在进程内单实例
    log_level: str = Field(default="INFO")

    # ---- 同箱复称统计补建 ----
    # 每个实例一个后台补建线程；多实例并行安全，逐个实例退出可被其他实例接续
    stats_backfill_enabled: bool = Field(default=True)
    stats_backfill_batch_size: int = Field(default=200, ge=1, le=5000)
    # 一批处理完立即继续下一批；仅在没有待计入行时短暂休眠
    stats_backfill_idle_seconds: float = Field(default=0.2, ge=0.0)
    # READY 之后的低频巡检间隔：接续任何遗留的未计入行
    stats_sweep_seconds: float = Field(default=5.0, ge=0.5)
    # 单个补建事务的锁等待上限（仅 PostgreSQL，SET LOCAL），超时回滚重试
    stats_lock_timeout_ms: int = Field(default=5000, ge=100)


@lru_cache
def get_settings() -> Settings:
    return Settings()
