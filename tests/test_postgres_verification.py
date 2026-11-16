"""真实 PostgreSQL 验证：方言专用 SQL（ON CONFLICT / SKIP LOCKED / RETURNING）、
并发双实例补建与在线写入的恰好一次、不锁整表。

仅在设置 WB_TEST_PG_URL 时运行（默认跳过，不影响 SQLite 测试套件）。
"""
from __future__ import annotations

import os
import threading
import time

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.models import (
    ContainerNetWeightStat,
    NetWeightRecord,
    StatBuildState,
)
from app.stats import (
    StatsBackfillWorker,
    apply_net_record_to_stats,
    backfill_batch,
    count_pending_records,
    mark_ready_if_backfilled,
)

PG_URL = os.getenv("WB_TEST_PG_URL")
pytestmark = pytest.mark.skipif(
    not PG_URL, reason="设置 WB_TEST_PG_URL 才运行真实 PostgreSQL 验证"
)

TOPIC = "weighbridge-events"


@pytest.fixture()
def pg(tmp_path):
    engine = create_engine(PG_URL, future=True, pool_size=10, max_overflow=10)
    # 每次从干净 schema 开始
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    yield engine, factory
    Base.metadata.drop_all(engine)
    engine.dispose()


def _seed(factory, rows):
    with factory() as s:
        for i, (cn, net, partition) in enumerate(rows):
            s.add(NetWeightRecord(
                container_no=cn, gross_weight=net + 4000, tare_weight=4000,
                net_weight=net, topic=TOPIC, source_partition=partition,
                source_offset=200000 + i, from_correction=False, stat_applied=False,
            ))
        s.commit()


def _truth(factory):
    with factory() as s:
        rows = s.execute(
            select(NetWeightRecord.container_no, func.count(),
                   func.min(NetWeightRecord.net_weight),
                   func.max(NetWeightRecord.net_weight))
            .group_by(NetWeightRecord.container_no)
        ).all()
        stats = {st.container_no: st for st in s.scalars(select(ContainerNetWeightStat))}
    return {r[0]: (r[1], r[2], r[3]) for r in rows}, stats


def _assert_match(factory):
    truth, stats = _truth(factory)
    assert set(truth) == set(stats)
    for cn, (c, mn, mx) in truth.items():
        st = stats[cn]
        assert (st.sample_count, st.min_net_weight, st.max_net_weight) == (c, mn, mx), cn


def test_postgres_schema_and_upsert_semantics(pg):
    engine, factory = pg
    # 部分索引存在
    with engine.connect() as conn:
        idx = conn.execute(text(
            "select indexname from pg_indexes where tablename='net_weight_records'"
        )).scalars().all()
    assert "ix_net_weight_pending_stats" in idx

    rec_id = None
    with factory() as s:
        rec = NetWeightRecord(container_no="PGUP0000001", gross_weight=10000,
                              tare_weight=4000, net_weight=6000, topic=TOPIC,
                              source_partition=0, source_offset=1,
                              from_correction=False, stat_applied=False)
        s.add(rec); s.flush()
        assert apply_net_record_to_stats(s, rec) is True
        s.commit()
        rec_id = rec.id

    # 再次计入：条件更新抢不到标记
    with factory() as s:
        rec = s.get(NetWeightRecord, rec_id)
        assert apply_net_record_to_stats(s, rec) is False
        s.commit()
    with factory() as s:
        stat = s.get(ContainerNetWeightStat, "PGUP0000001")
        assert stat.sample_count == 1


def test_postgres_skip_locked_two_instances(pg):
    engine, factory = pg
    _seed(factory, [(f"PG{i % 20:04d}", 6000 + (i % 11), i % 2) for i in range(800)])

    errors: list[str] = []
    stop = threading.Event()

    def worker():
        try:
            while not stop.is_set():
                with factory() as s:
                    scanned, _ = backfill_batch(s, batch_size=17)
                if scanned == 0:
                    time.sleep(0.002)
        except Exception as exc:  # noqa: BLE001
            errors.append(repr(exc))

    ts = [threading.Thread(target=worker) for _ in range(3)]
    for t in ts:
        t.start()
    deadline = time.time() + 40
    while time.time() < deadline:
        with factory() as s:
            if count_pending_records(s) == 0:
                break
        time.sleep(0.01)
    stop.set()
    for t in ts:
        t.join(timeout=15)
    assert not errors, errors[:3]

    with factory() as s:
        assert mark_ready_if_backfilled(s) is True
    _assert_match(factory)


def test_postgres_backfill_does_not_take_table_lock(pg):
    """补建持锁期间，其他连接仍可写入并提交（行级锁而非整表锁）。"""
    engine, factory = pg
    _seed(factory, [(f"LK{i % 10:04d}", 6000 + i, 0) for i in range(500)])

    holder_ready = threading.Event()
    release = threading.Event()
    writer_done = threading.Event()
    errors: list[str] = []

    def hold_backfill_transaction():
        try:
            with factory() as s:
                # 开始一批（持有行锁与聚合锁），但不提交
                ids = list(s.scalars(
                    select(NetWeightRecord.id)
                    .where(NetWeightRecord.stat_applied.is_(False))
                    .order_by(NetWeightRecord.id).limit(50)
                    .with_for_update(skip_locked=True)
                ).all())
                holder_ready.set()
                release.wait(timeout=20)
                s.rollback()
        except Exception as exc:  # noqa: BLE001
            errors.append(("holder", repr(exc)))

    def live_write():
        holder_ready.wait(timeout=20)
        try:
            with factory() as s:
                rec = NetWeightRecord(
                    container_no="LKLK0000001", gross_weight=12000, tare_weight=4000,
                    net_weight=8000, topic=TOPIC, source_partition=1,
                    source_offset=999999, from_correction=False, stat_applied=False,
                )
                s.add(rec); s.flush()
                apply_net_record_to_stats(s, rec)
                s.commit()  # 若被整表锁阻塞，这里会超时失败
            writer_done.set()
        except Exception as exc:  # noqa: BLE001
            errors.append(("writer", repr(exc)))

    h = threading.Thread(target=hold_backfill_transaction)
    w = threading.Thread(target=live_write)
    h.start(); w.start()
    ok = writer_done.wait(timeout=10)
    release.set()
    h.join(timeout=15); w.join(timeout=15)

    assert not errors, errors[:3]
    assert ok, "在线写入在补建持锁期间被阻塞，说明锁粒度过大"
    with factory() as s:
        st = s.get(ContainerNetWeightStat, "LKLK0000001")
        assert st is not None and st.sample_count == 1


def test_postgres_worker_takeover(pg):
    engine, factory = pg
    _seed(factory, [(f"TO{i % 4:04d}", 6000 + i, 0) for i in range(40)])
    with factory() as s:
        backfill_batch(s, batch_size=5)  # 实例 A 部分完成即退出

    worker = StatsBackfillWorker(factory, batch_size=5, idle_seconds=0.0,
                                 sweep_seconds=0.5)
    deadline = time.time() + 20
    while time.time() < deadline and not worker.run_once():
        pass
    with factory() as s:
        assert s.get(StatBuildState, 1).ready is True
        assert count_pending_records(s) == 0
    _assert_match(factory)
