"""关键交错自测：双实例补建、补建与在线写并发、失败/重放/重启不重复累计。

每个用例使用独立的文件 SQLite（WAL）+ 多个独立连接/线程，尽量贴近
PostgreSQL 的多连接语义。SQLite 无行级 ``FOR UPDATE``，因此这里验证的是
“条件更新抢标记 + 同事务聚合”这层与方言无关的恰好一次不变量；
PostgreSQL 行锁/SKIP LOCKED 的作用在代码中由方言分支实现。
"""
from __future__ import annotations

import threading
import time

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError

from app.models import (
    ContainerNetWeightStat,
    NetWeightRecord,
    StatBuildState,
)
from app.services import apply_correction, get_quarantine_by_position, process_consumed_message
from app.stats import (
    StatsBackfillWorker,
    apply_net_record_to_stats,
    backfill_batch,
    count_pending_records,
    mark_ready_if_backfilled,
)

TOPIC = "weighbridge-events"


def _seed_legacy(factory, rows):
    with factory() as s:
        for i, (cn, net, partition) in enumerate(rows):
            s.add(NetWeightRecord(
                container_no=cn, gross_weight=net + 4000, tare_weight=4000,
                net_weight=net, topic=TOPIC, source_partition=partition,
                source_offset=100000 + i, from_correction=False, stat_applied=False,
            ))
        s.commit()


def _ground_truth(factory):
    with factory() as s:
        rows = s.execute(
            select(
                NetWeightRecord.container_no,
                func.count(),
                func.min(NetWeightRecord.net_weight),
                func.max(NetWeightRecord.net_weight),
            ).group_by(NetWeightRecord.container_no)
        ).all()
        stats = {
            st.container_no: st for st in s.scalars(select(ContainerNetWeightStat))
        }
        pending = count_pending_records(s)
        ready = s.get(StatBuildState, 1).ready if s.get(StatBuildState, 1) else None
    return {r[0]: (r[1], r[2], r[3]) for r in rows}, stats, pending, ready


def _assert_stats_match(factory):
    truth, stats, pending, _ = _ground_truth(factory)
    assert pending == 0, "存在未计入统计的明细"
    assert set(stats) == set(truth), (set(stats) ^ set(truth))
    bad = []
    for cn, (count, mn, mx) in truth.items():
        st = stats[cn]
        if (st.sample_count, st.min_net_weight, st.max_net_weight) != (count, mn, mx):
            bad.append((cn, (count, mn, mx),
                        (st.sample_count, st.min_net_weight, st.max_net_weight)))
    assert not bad, bad[:5]


def _insert_live(factory, cn, net, partition, offset):
    """模拟在线消费/补正：插入明细并在同事务内计入统计。"""
    with factory() as s:
        rec = NetWeightRecord(
            container_no=cn, gross_weight=net + 4000, tare_weight=4000,
            net_weight=net, topic=TOPIC, source_partition=partition,
            source_offset=offset, from_correction=False, stat_applied=False,
        )
        s.add(rec)
        s.flush()
        apply_net_record_to_stats(s, rec)
        s.commit()
        return rec.id


# --------------------------------------------------------------------------

def test_two_backfill_instances_parallel_exactly_once(db_setup):
    """双实例同时补建：无重复累计、无漏算，READY 后完全一致。"""
    rows = [(f"PAR{i % 30:04d}", 6000 + (i % 13) * 10, i % 2) for i in range(600)]
    _seed_legacy(db_setup, rows)

    errors: list[str] = []
    stop = threading.Event()

    def worker():
        try:
            while not stop.is_set():
                with db_setup() as s:
                    scanned, _ = backfill_batch(s, batch_size=13)
                if scanned == 0 and not stop.is_set():
                    time.sleep(0.002)
        except Exception as exc:  # noqa: BLE001
            errors.append(repr(exc))

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    deadline = time.time() + 30
    while time.time() < deadline:
        with db_setup() as s:
            if count_pending_records(s) == 0:
                break
        time.sleep(0.01)
    stop.set()
    for t in threads:
        t.join(timeout=10)

    assert not errors, errors[:3]
    with db_setup() as s:
        assert mark_ready_if_backfilled(s) is True
    _assert_stats_match(db_setup)


def test_backfill_does_not_block_live_consumption_or_correction(db_setup):
    """补建大存量进行中，在线消费与补正仍能提交（无整表锁）。"""
    # 大量存量，使补建持续若干批次
    _seed_legacy(
        db_setup,
        [(f"BULK{i % 20:04d}", 5000 + (i % 7), 0) for i in range(1200)],
    )

    progressed = threading.Event()
    live_results: dict[str, object] = {}
    errors: list[str] = []

    def backfill():
        try:
            while True:
                with db_setup() as s:
                    scanned, _ = backfill_batch(s, batch_size=25)
                if scanned == 0:
                    break
        except Exception as exc:  # noqa: BLE001
            errors.append(("backfill", repr(exc)))

    def live():
        try:
            # 1) 在线消费入仓
            with db_setup() as s:
                out = process_consumed_message(
                    s, topic=TOPIC, partition=1, offset=900001,
                    raw_text='{"container_no": "LIVE0000001", "gross_weight": 12000, "tare_weight": 4000}',
                )
                live_results["consume"] = out.action
            progressed.set()

            # 2) 隔离 + 补正
            with db_setup() as s:
                process_consumed_message(
                    s, topic=TOPIC, partition=1, offset=900002,
                    raw_text='{"container_no": "LIVE0000001", "gross_weight": 1, "tare_weight": 4000}',
                )
            with db_setup() as s:
                q = get_quarantine_by_position(s, topic=TOPIC, partition=1, offset=900002)
                _, net, reason = apply_correction(
                    s, q.id,
                    {"container_no": "LIVE0000001", "gross_weight": 9000, "tare_weight": 4000},
                )
            live_results["correction"] = (reason, net.net_weight if net else None)
        except Exception as exc:  # noqa: BLE001
            errors.append(("live", repr(exc)))

    bf = threading.Thread(target=backfill)
    lv = threading.Thread(target=live)
    bf.start()
    lv.start()
    lv.join(timeout=30)
    bf.join(timeout=30)

    assert not errors, errors[:3]
    assert live_results["consume"] == "NET_RECORD"
    assert live_results["correction"] == (None, 5000)
    assert progressed.is_set()

    # 存量全部补建 + 在线明细都在统计中（存在重复写入时数量会偏大）
    with db_setup() as s:
        while backfill_batch(s, batch_size=100)[0]:
            pass
        mark_ready_if_backfilled(s)
    _assert_stats_match(db_setup)


def test_write_failure_then_retry_no_double_count(db_setup):
    """写库失败回滚：标记随事务回滚，重试后只累计一次。"""
    _seed_legacy(db_setup, [("FAIL0000001", 6000, 0)])

    # 第一次：聚合 upsert 失败，整批回滚
    import app.stats as stats_module

    real_upsert = stats_module._upsert_group
    state = {"fail": True}

    def flaky_upsert(*args, **kwargs):
        if state["fail"]:
            state["fail"] = False
            raise OperationalError("UPDATE", {}, Exception("simulated failure"))
        return real_upsert(*args, **kwargs)

    stats_module._upsert_group = flaky_upsert
    try:
        with pytest.raises(OperationalError):
            with db_setup() as s:
                backfill_batch(s, batch_size=10)
    finally:
        stats_module._upsert_group = real_upsert

    # 回滚后：明细仍在、标记为 false（未计入）
    with db_setup() as s:
        assert count_pending_records(s) == 1
        assert s.scalar(select(func.count()).select_from(ContainerNetWeightStat)) == 0

    # 重试成功
    scanned, applied = backfill_batch(db_setup(), batch_size=10)
    assert (scanned, applied) == (1, 1)
    _assert_stats_match(db_setup)


def test_replay_after_commit_failure_does_not_double_count(db_setup):
    """模拟消费者“统计已提交、位点提交失败后重放”：明细层幂等跳过，不重复计数。"""
    with db_setup() as s:
        first = process_consumed_message(
            s, topic=TOPIC, partition=0, offset=1,
            raw_text='{"container_no": "RPLY0000001", "gross_weight": 20000, "tare_weight": 4000}',
        )
    assert first.action == "NET_RECORD"

    for _ in range(5):
        with db_setup() as s:
            again = process_consumed_message(
                s, topic=TOPIC, partition=0, offset=1,
                raw_text='{"container_no": "RPLY0000001", "gross_weight": 20000, "tare_weight": 4000}',
            )
        assert again.action == "DUPLICATE_SKIP"

    with db_setup() as s:
        stat = s.get(ContainerNetWeightStat, "RPLY0000001")
        assert stat.sample_count == 1
        assert s.scalar(select(func.count()).select_from(NetWeightRecord)) == 1


def test_restart_keeps_stats_and_does_not_recount(db_setup):
    """重启不清空已建统计：再次补建扫描到 0 待计入行。"""
    _seed_legacy(db_setup, [("REST0000001", 6000, 0), ("REST0000001", 7000, 1)])
    with db_setup() as s:
        while backfill_batch(s, batch_size=1)[0]:
            pass
        mark_ready_if_backfilled(s)
    _, stats_before, pending, ready = _ground_truth(db_setup)
    assert ready is True and pending == 0
    before = stats_before["REST0000001"].sample_count

    worker = StatsBackfillWorker(db_setup, batch_size=10, idle_seconds=0.01,
                                 sweep_seconds=0.5)
    assert worker.run_once() is True  # 已 ready 且无待计入行

    _, stats_after, pending, ready = _ground_truth(db_setup)
    assert pending == 0 and ready is True
    assert stats_after["REST0000001"].sample_count == before == 2


def test_worker_takeover_after_process_exit(db_setup):
    """补建进程退出（模拟崩溃）后，存活实例继续把剩余存量计入。"""
    _seed_legacy(
        db_setup, [(f"TAKE{i % 5:04d}", 6000 + i, 0) for i in range(50)]
    )

    # 实例 A 只完成一部分就“退出”
    with db_setup() as s:
        backfill_batch(s, batch_size=7)
    _, _, pending_after_a, ready_after_a = _ground_truth(db_setup)
    # 状态行可能尚未建立（None），但绝不能已 READY
    assert pending_after_a > 0 and ready_after_a is not True

    # 实例 B 接续直到 READY
    worker = StatsBackfillWorker(db_setup, batch_size=7, idle_seconds=0.0,
                                 sweep_seconds=0.5)
    deadline = time.time() + 20
    while time.time() < deadline and not worker.run_once():
        pass
    _, _, pending, ready = _ground_truth(db_setup)
    assert pending == 0 and ready is True
    _assert_stats_match(db_setup)


def test_ready_waits_for_other_instance_inflight_batch(db_setup):
    """另一实例尚在提交最后一批时，本实例不得提前置 READY（避免少报）。"""
    _seed_legacy(db_setup, [("GATE0000001", 6000, 0)])

    with db_setup() as s:
        pending_before = count_pending_records(s)
    assert pending_before == 1

    # 尚未计入任何行时请求置位：必须拒绝
    with db_setup() as s:
        assert mark_ready_if_backfilled(s) is False
        assert s.get(StatBuildState, 1).ready is False

    # 计入后才能置位
    with db_setup() as s:
        backfill_batch(s, batch_size=10)
    with db_setup() as s:
        assert mark_ready_if_backfilled(s) is True
    _assert_stats_match(db_setup)
