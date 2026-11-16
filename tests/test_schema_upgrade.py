"""存量库升级：已有 net_weight_records 缺 stat_applied 时在线补列补索引。

覆盖“已有数据在线补建”的升级前置条件：不重建表、已有行默认为未计入，
随后补建线程即可计入。DDL 幂等（重复启动不报错）。
"""
from __future__ import annotations

from sqlalchemy import create_engine, inspect, text

from app.database import Database


def _make_legacy_db(url: str) -> None:
    """构造上线前结构：净重表存在但没有 stat_applied 列。"""
    engine = create_engine(url, future=True)
    with engine.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE net_weight_records (
                id INTEGER NOT NULL PRIMARY KEY,
                container_no VARCHAR(32) NOT NULL,
                gross_weight BIGINT NOT NULL,
                tare_weight BIGINT NOT NULL,
                net_weight BIGINT NOT NULL,
                topic VARCHAR(255) NOT NULL,
                source_partition INTEGER NOT NULL,
                source_offset BIGINT NOT NULL,
                from_correction BOOLEAN NOT NULL,
                quarantine_id BIGINT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        ))
        conn.execute(text(
            "INSERT INTO net_weight_records "
            "(id, container_no, gross_weight, tare_weight, net_weight, "
            " topic, source_partition, source_offset, from_correction) "
            "VALUES (1, 'OLD0000001', 10000, 4000, 6000, 't', 0, 1, 0),"
            "       (2, 'OLD0000001', 11000, 4000, 7000, 't', 1, 1, 0)"
        ))
    engine.dispose()


def test_legacy_db_is_upgraded_and_rows_marked_pending(tmp_path):
    url = f"sqlite:///{tmp_path / 'legacy.db'}"
    _make_legacy_db(url)

    db = Database(url)
    try:
        db.init_schema()
        cols = {c["name"] for c in inspect(db.engine).get_columns("net_weight_records")}
        assert "stat_applied" in cols

        with db.engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT id, stat_applied FROM net_weight_records ORDER BY id"
            )).all()
        # 已有行全部默认为“未计入”，交由补建线程处理，绝不重复计入旧数据
        assert rows == [(1, 0), (2, 0)]

        indexes = {i["name"] for i in inspect(db.engine).get_indexes("net_weight_records")}
        assert "ix_net_weight_pending_stats" in indexes

        # 重复启动幂等
        db.init_schema()
    finally:
        db.dispose()


def test_legacy_rows_are_backfilled_without_rebuilding_table(tmp_path):
    """升级后补建：两条旧明细计入统计，表未被重建（id 与原文保留）。"""
    from sqlalchemy.orm import sessionmaker

    from app.stats import backfill_batch, mark_ready_if_backfilled

    url = f"sqlite:///{tmp_path / 'legacy2.db'}"
    _make_legacy_db(url)
    db = Database(url)
    try:
        db.init_schema()
        factory = sessionmaker(bind=db.engine, expire_on_commit=False, future=True)
        with factory() as s:
            scanned, applied = backfill_batch(s, batch_size=10)
        assert (scanned, applied) == (2, 2)
        with factory() as s:
            assert mark_ready_if_backfilled(s) is True
            # 原表未被重建：旧 id 仍在
            ids = [r[0] for r in s.execute(text(
                "SELECT id FROM net_weight_records ORDER BY id"
            )).all()]
            assert ids == [1, 2]
            stat = s.execute(text(
                "SELECT sample_count, min_net_weight, max_net_weight "
                "FROM container_net_weight_stats WHERE container_no='OLD0000001'"
            )).one()
            assert tuple(stat) == (2, 6000, 7000)
    finally:
        db.dispose()
