"""数据库引擎、会话工厂与建表逻辑。

业务表只追加、不更新业务数据（隔离记录仅在补正成功时翻状态），
并以 (topic, source_partition, source_offset) 作为全流程幂等键，
保证每条源消息最多入仓一次、最多补正一次。
"""
from __future__ import annotations

import logging
from collections.abc import Iterator

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import get_settings

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


# 延迟导入，避免模型模块循环依赖
from app import models  # noqa: E402,F401  (side effect: 注册 ORM 模型)


class Database:
    """进程级数据库资源封装。"""

    def __init__(self, url: str | None = None) -> None:
        settings = get_settings()
        self.url = url or settings.database_url
        # pool_pre_ping：连接断后取连接时自动重验，减少“写库失败/断连”场景的脏错误
        self.engine: Engine = create_engine(
            self.url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=5,
            future=True,
        )
        self._session_factory = sessionmaker(
            bind=self.engine, expire_on_commit=False, autoflush=False, future=True
        )

    def init_schema(self) -> None:
        """幂等建表 + 存量库轻量升级（演示项目用，生产可换 Alembic）。

        ``create_all`` 不会给已存在的表补列，因此这里显式补齐
        ``net_weight_records.stat_applied``：已有明细默认 false，
        由在线补建线程按 id 批次计入统计。该 DDL 只加可空默认列，
        不重写表、不阻塞正常写入。
        """
        Base.metadata.create_all(self.engine)
        self._ensure_stat_applied_column()
        self._ensure_pending_stats_index()
        logger.info("database schema ready")

    def _ensure_stat_applied_column(self) -> None:
        inspector = inspect(self.engine)
        if not inspector.has_table(models.NetWeightRecord.__tablename__):
            return
        columns = {
            c["name"] for c in inspector.get_columns(models.NetWeightRecord.__tablename__)
        }
        if "stat_applied" in columns:
            return

        dialect = self.engine.dialect.name
        if dialect == "postgresql":
            ddl = (
                "ALTER TABLE net_weight_records "
                "ADD COLUMN IF NOT EXISTS stat_applied BOOLEAN NOT NULL DEFAULT FALSE"
            )
        elif dialect == "sqlite":
            ddl = (
                "ALTER TABLE net_weight_records "
                "ADD COLUMN stat_applied BOOLEAN NOT NULL DEFAULT 0"
            )
        else:  # pragma: no cover - 仅支持这两种方言
            raise RuntimeError(f"不支持自动补列: {dialect}")

        with self.engine.begin() as conn:
            conn.execute(text(ddl))
        logger.warning("已为存量库补列 net_weight_records.stat_applied")

    def _ensure_pending_stats_index(self) -> None:
        """部分索引用于“只扫未计入行”；独立于补列执行，便于中断后自愈。"""
        inspector = inspect(self.engine)
        table = models.NetWeightRecord.__tablename__
        if not inspector.has_table(table):
            return
        columns = {c["name"] for c in inspector.get_columns(table)}
        if "stat_applied" not in columns:
            return

        index_name = "ix_net_weight_pending_stats"
        existing = {i["name"] for i in inspector.get_indexes(table)}
        if index_name in existing:
            return

        dialect = self.engine.dialect.name
        if dialect == "postgresql":
            predicate = "stat_applied = false"
        elif dialect == "sqlite":
            predicate = "stat_applied = 0"
        else:  # pragma: no cover
            return
        with self.engine.begin() as conn:
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS {index_name} "
                f"ON net_weight_records (id) WHERE {predicate}"
            ))

    def session(self) -> Session:
        return self._session_factory()

    def dispose(self) -> None:
        self.engine.dispose()


_db: Database | None = None


def get_database() -> Database:
    """FastAPI 依赖与后台线程共用的单例。"""
    global _db
    if _db is None:
        _db = Database()
        _db.init_schema()
    return _db


def get_session() -> Iterator[Session]:
    """FastAPI 请求级会话。"""
    db = get_database()
    session = db.session()
    try:
        yield session
    finally:
        session.close()


def reset_database_singleton() -> None:
    """测试辅助：释放单例，便于切换内存库。"""
    global _db
    if _db is not None:
        _db.dispose()
    _db = None
