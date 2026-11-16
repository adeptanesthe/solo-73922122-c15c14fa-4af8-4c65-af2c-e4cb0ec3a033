"""ORM 模型：净重明细、隔离记录。

幂等键 (topic, source_partition, source_offset) 在两张表上各自唯一，
与消费位点的手工提交配合，实现“每条源消息仅入仓一次”。
"""
from __future__ import annotations

import enum
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    Index,
    Integer,
    String,
    UniqueConstraint,
    false,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base

# SQLite 仅允许 INTEGER PRIMARY KEY 自增，生产 PostgreSQL 仍使用 BIGINT
PKBigInt = BigInteger().with_variant(Integer(), "sqlite")


class QuarantineStatus(str, enum.Enum):
    OPEN = "OPEN"          # 仍在隔离，等待合法补正
    CORRECTED = "CORRECTED"  # 已接受唯一一次合法补正并生成净重明细


class NetWeightRecord(Base):
    """净重明细（入仓事实表，只追加）。

    ``stat_applied`` 是统计增量维护的“恰好一次”标记：明细落库与
    聚合更新在同一事务内完成，先以条件 UPDATE 抢占该标记，只有抢占
    成功的执行者才允许更新聚合；失败回滚时标记一并回滚。补建扫描只
    挑选 ``stat_applied = false`` 的存量明细。
    """

    __tablename__ = "net_weight_records"
    __table_args__ = (
        UniqueConstraint(
            "topic",
            "source_partition",
            "source_offset",
            name="uq_net_weight_source_position",
        ),
        CheckConstraint("gross_weight >= 0", name="ck_net_gross_non_negative"),
        CheckConstraint("tare_weight >= 0", name="ck_net_tare_non_negative"),
        CheckConstraint("gross_weight >= tare_weight", name="ck_net_gross_ge_tare"),
        CheckConstraint("net_weight >= 0", name="ck_net_weight_non_negative"),
        Index("ix_net_weight_container_no", "container_no"),
        Index("ix_net_weight_created_at", "created_at"),
        # 只索引尚未计入统计的明细，补建按 id 顺序批量取数
        Index(
            "ix_net_weight_pending_stats",
            "id",
            postgresql_where=text("stat_applied = false"),
            sqlite_where=text("stat_applied = 0"),
        ),
    )

    id: Mapped[int] = mapped_column(PKBigInt, primary_key=True)
    container_no: Mapped[str] = mapped_column(String(32), nullable=False)
    gross_weight: Mapped[int] = mapped_column(BigInteger, nullable=False)
    tare_weight: Mapped[int] = mapped_column(BigInteger, nullable=False)
    net_weight: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # 来源定位
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    source_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    source_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # 来源标记：消费者正常处理 / 隔离补正
    from_correction: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # 补正成功时关联的隔离记录 id；正常路径为空
    quarantine_id: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # 该明细是否已计入同箱统计（同一事务内抢标记，恰好一次）
    stat_applied: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )


class QuarantineRecord(Base):
    """隔离记录：无效消息的原文、错误原因与补正状态。"""

    __tablename__ = "quarantine_records"
    __table_args__ = (
        UniqueConstraint(
            "topic",
            "source_partition",
            "source_offset",
            name="uq_quarantine_source_position",
        ),
        Index("ix_quarantine_status", "status"),
        Index("ix_quarantine_partition_offset", "source_partition", "source_offset"),
        Index("ix_quarantine_container_no", "container_no"),
    )

    id: Mapped[int] = mapped_column(PKBigInt, primary_key=True)

    # 原文原样保留（UTF-8 解码失败时保留可打印转义），同文不同位置分别保留
    raw_payload: Mapped[str] = mapped_column(String, nullable=False)
    # 解析出的箱号（无法解析时为空串），便于检索
    container_no: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    reason_code: Mapped[str] = mapped_column(String(64), nullable=False)
    reason_detail: Mapped[str] = mapped_column(String(512), nullable=False)

    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    source_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    source_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)

    status: Mapped[QuarantineStatus] = mapped_column(
        Enum(QuarantineStatus, name="quarantine_status"),
        nullable=False,
        default=QuarantineStatus.OPEN,
    )
    # 最近一次失败补正的原因（成功后清空思路：保留最后错误，便于审计）
    last_correction_error: Mapped[str | None] = mapped_column(
        String(512), nullable=True
    )
    correction_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    corrected_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class ContainerNetWeightStat(Base):
    """按箱号持久增量维护的净重统计（查询只读本表，不扫明细）。

    只统计“已入仓”的净重明细：正常消费入仓与补正成功入仓都计入，
    隔离失败（仍 OPEN）不产生明细因而不计入。同文不同源位置各自
    一次称量，分别计数。

    ``min_source_partition/offset``、``max_source_partition/offset``
    记录当前最小/最大净重各一处来源位置（按 (partition, offset) 取
    最早出现的一处），供巡检查回地磅来源。
    """

    __tablename__ = "container_net_weight_stats"
    __table_args__ = (
        CheckConstraint("sample_count > 0", name="ck_container_stat_count_positive"),
        CheckConstraint("min_net_weight >= 0", name="ck_container_stat_min_non_negative"),
        CheckConstraint(
            "max_net_weight >= min_net_weight", name="ck_container_stat_max_ge_min"
        ),
        CheckConstraint(
            "weight_range = max_net_weight - min_net_weight",
            name="ck_container_stat_range_consistent",
        ),
        Index("ix_container_stat_range", "weight_range"),
        Index("ix_container_stat_sample_count", "sample_count"),
    )

    container_no: Mapped[str] = mapped_column(String(32), primary_key=True)

    sample_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    min_net_weight: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_net_weight: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # 极差 = max - min，物化存储以便直接按“最小极差”索引筛选
    weight_range: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # 最小净重的一处来源位置（同值取 (topic, partition, offset) 最早的一处）
    min_source_topic: Mapped[str] = mapped_column(String(255), nullable=False)
    min_source_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    min_source_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # 最大净重的一处来源位置
    max_source_topic: Mapped[str] = mapped_column(String(255), nullable=False)
    max_source_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    max_source_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class StatBuildState(Base):
    """统计补建进度（单行，主键固定为 1）。

    ``ready`` 为真表示存量补建已完成、统计可用于对外查询；
    完成前巡检接口只返回“准备中”，不暴露半成品数据。
    该行持久化保存，重启不清空已建统计。
    """

    __tablename__ = "stat_build_state"
    __table_args__ = (CheckConstraint("id = 1", name="ck_stat_build_single_row"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)

    ready: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    ready_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

