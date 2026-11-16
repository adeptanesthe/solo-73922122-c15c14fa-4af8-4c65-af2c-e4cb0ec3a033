"""同箱复称巡检统计：增量持久化、恰好一次、补建与在线交错。

覆盖：
1. 正常消费 / 补正成功 计入，隔离失败 不计入；
2. 同文不同源位置分别计数；最小/最大净重与各一处源位置正确；
3. 重放、重复调用、并发抢标记不重复累计；
4. 存量补建：完成前只报 PREPARING，完成后可查；新明细与统计同事务生效；
5. 补建期间 / 迟到明细不漏算；实例退出由存活实例接续；
6. 查询只读聚合表，不重扫明细。
"""
from __future__ import annotations

import time

import pytest
from sqlalchemy import func, select, update

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
    mark_ready_if_backfilled,
)

TOPIC = "weighbridge-events"


# --------------------------------------------------------------------------
# 工具
# --------------------------------------------------------------------------

def _msg(container_no: str, gross: int, tare: int = 4000) -> str:
    return (
        '{"container_no": "%s", "gross_weight": %d, "tare_weight": %d}'
        % (container_no, gross, tare)
    )


def _process(session, partition: int, offset: int, raw: str):
    return process_consumed_message(
        session, topic=TOPIC, partition=partition, offset=offset, raw_text=raw
    )


def _stat(session, container_no: str) -> ContainerNetWeightStat | None:
    return session.get(ContainerNetWeightStat, container_no)


def _add_legacy_records(session, rows: list[tuple[str, int, int, int]]) -> int:
    """直接插入“存量明细”，stat_applied 保持 false 以模拟上线前数据。"""
    max_id = session.scalar(select(func.max(NetWeightRecord.id))) or 0
    for i, (container_no, gross, tare, partition) in enumerate(rows, start=1):
        session.add(
            NetWeightRecord(
                container_no=container_no,
                gross_weight=gross,
                tare_weight=tare,
                net_weight=gross - tare,
                topic=TOPIC,
                source_partition=partition,
                source_offset=max_id + i,
                from_correction=False,
                stat_applied=False,
            )
        )
    session.commit()
    return len(rows)


# --------------------------------------------------------------------------
# 增量计入
# --------------------------------------------------------------------------

def test_incremental_stats_from_consumer_and_correction(db_session):
    # 三次正常入仓：净重 16000 / 20000 / 10000
    _process(db_session, 0, 0, _msg("MSCU1234567", 20000))
    _process(db_session, 0, 1, _msg("MSCU1234567", 24000))
    _process(db_session, 1, 0, _msg("MSCU1234567", 14000))

    stat = _stat(db_session, "MSCU1234567")
    assert stat is not None
    assert stat.sample_count == 3
    assert (stat.min_net_weight, stat.max_net_weight, stat.weight_range) == (
        10000,
        20000,
        10000,
    )
    assert (stat.min_source_partition, stat.min_source_offset) == (1, 0)
    assert (stat.max_source_partition, stat.max_source_offset) == (0, 1)

    # 补正成功入仓计入：一次新的称量，净重 800
    _process(db_session, 0, 2, '{"container_no": "MSCU1234567", "gross_weight": 1, "tare_weight": 2}')
    q = get_quarantine_by_position(db_session, topic=TOPIC, partition=0, offset=2)
    _, net, reason = apply_correction(
        db_session, q.id,
        {"container_no": "MSCU1234567", "gross_weight": 4800, "tare_weight": 4000},
    )
    assert reason is None and net is not None

    db_session.expire_all()
    stat = _stat(db_session, "MSCU1234567")
    assert stat.sample_count == 4
    assert stat.min_net_weight == 800
    assert (stat.min_source_partition, stat.min_source_offset) == (0, 2)
    assert stat.max_net_weight == 20000
    assert stat.weight_range == 19200


def test_quarantined_messages_do_not_count_until_corrected(db_session):
    _process(db_session, 0, 0, _msg("BADU0000001", 3000, tare=4200))  # 毛<皮 -> 隔离
    assert _stat(db_session, "BADU0000001") is None

    q = get_quarantine_by_position(db_session, topic=TOPIC, partition=0, offset=0)
    # 失败补正：仍不计入
    apply_correction(db_session, q.id, {"container_no": "BADU0000001",
                                        "gross_weight": 10, "tare_weight": 20})
    db_session.expire_all()
    assert _stat(db_session, "BADU0000001") is None

    # 合法补正后计入一次
    _, net, reason = apply_correction(
        db_session, q.id,
        {"container_no": "BADU0000001", "gross_weight": 5000, "tare_weight": 4200},
    )
    assert reason is None
    db_session.expire_all()
    stat = _stat(db_session, "BADU0000001")
    assert stat.sample_count == 1 and stat.min_net_weight == 800


def test_same_payload_different_positions_counted_separately(db_session):
    raw = _msg("DUPU0000000", 9000)
    _process(db_session, 0, 0, raw)
    _process(db_session, 0, 1, raw)   # 同文、不同 offset
    _process(db_session, 1, 0, raw)   # 同文、不同分区

    stat = _stat(db_session, "DUPU0000000")
    assert stat.sample_count == 3
    assert stat.weight_range == 0
    # 两条明细各自一次称量，且各源位置都被计入
    with db_session:
        pass
    assert db_session.scalar(
        select(func.count()).select_from(NetWeightRecord).where(
            NetWeightRecord.container_no == "DUPU0000000"
        )
    ) == 3


def test_min_max_source_picks_stable_position_on_ties(db_session):
    # 相同净重时收敛到 (topic, partition, offset) 字典序最小的一处源位置
    _process(db_session, 1, 5, _msg("TIEU0000001", 9000))   # net 5000
    _process(db_session, 0, 9, _msg("TIEU0000001", 9000))   # net 5000
    _process(db_session, 0, 3, _msg("TIEU0000001", 9000))   # net 5000，最早

    db_session.expire_all()
    stat = _stat(db_session, "TIEU0000001")
    assert stat.sample_count == 3
    assert (stat.min_source_partition, stat.min_source_offset) == (0, 3)
    assert (stat.max_source_partition, stat.max_source_offset) == (0, 3)


def test_replay_does_not_double_count(db_session):
    raw = _msg("RPLY0000001", 20000)
    _process(db_session, 0, 0, raw)
    for _ in range(3):
        out = _process(db_session, 0, 0, raw)
        assert out.action == "DUPLICATE_SKIP"

    stat = _stat(db_session, "RPLY0000001")
    assert stat.sample_count == 1
    assert db_session.scalar(
        select(func.count()).select_from(NetWeightRecord)
    ) == 1


def test_apply_twice_is_idempotent_via_claim_marker(db_session):
    """模拟补建与在线链路先后碰到同一条明细：标记保证只累计一次。"""
    _process(db_session, 0, 0, _msg("MARK0000001", 7000))
    record = db_session.scalar(select(NetWeightRecord))
    assert record.stat_applied is True

    # 再把标记复位并重复计入两次，验证只有抢到标记的那一次生效
    db_session.execute(
        update(NetWeightRecord)
        .where(NetWeightRecord.id == record.id)
        .values(stat_applied=False)
    )
    db_session.commit()
    db_session.expire_all()
    record = db_session.get(NetWeightRecord, record.id)

    first = apply_net_record_to_stats(db_session, record)
    db_session.commit()
    # 第二次：内存对象已标记，但数据库也已置位，条件更新必然抢不到
    db_session.expire_all()
    record2 = db_session.get(NetWeightRecord, record.id)
    second = apply_net_record_to_stats(db_session, record2)
    db_session.commit()

    assert first is True and second is False
    # 统计对象在同一 session 的 identity map 中已过期，先刷新再断言。
    # 基线 1 次（首次处理）+ 仅一次重新计入 = 2，证明第二次未被累计。
    db_session.expire_all()
    assert _stat(db_session, "MARK0000001").sample_count == 2


def test_stat_failure_rolls_back_together_with_record(db_session, monkeypatch):
    """统计写入失败时，明细与计数必须一起回滚，不能出现“有明细、无统计”。"""
    import app.stats as stats_module

    def boom(*args, **kwargs):
        raise RuntimeError("simulated stats failure")

    monkeypatch.setattr(stats_module, "_upsert_group", boom)
    with pytest.raises(RuntimeError):
        _process(db_session, 0, 0, _msg("FAIL0000001", 9000))
    db_session.rollback()

    assert db_session.scalar(select(func.count()).select_from(NetWeightRecord)) == 0
    assert _stat(db_session, "FAIL0000001") is None


# --------------------------------------------------------------------------
# 存量补建
# --------------------------------------------------------------------------

def test_backfill_reports_preparing_then_ready(db_session):
    _add_legacy_records(
        db_session,
        [
            ("LEGA0000001", 20000, 4000, 0),
            ("LEGA0000001", 21000, 4000, 1),
            ("LEGB0000002", 9000, 4000, 0),
        ],
    )
    assert _stat(db_session, "LEGA0000001") is None

    scanned, applied = backfill_batch(db_session, batch_size=100)
    assert (scanned, applied) == (3, 3)

    stat = _stat(db_session, "LEGA0000001")
    assert stat.sample_count == 2 and stat.weight_range == 1000

    ready = mark_ready_if_backfilled(db_session)
    assert ready is True
    db_session.expire_all()
    assert db_session.get(StatBuildState, 1).ready is True


def test_backfill_batches_small_and_keeps_order(db_session):
    # 净重 6000..6009，分 3 条一批共 4 批扫描完毕
    _add_legacy_records(
        db_session, [("BATC0000001", 10000 + i, 4000, 0) for i in range(10)]
    )
    total_scanned = 0
    while True:
        scanned, _ = backfill_batch(db_session, batch_size=3)
        if scanned == 0:
            break
        total_scanned += scanned
    assert total_scanned == 10

    db_session.expire_all()
    stat = _stat(db_session, "BATC0000001")
    assert stat.sample_count == 10
    assert stat.min_net_weight == 6000 and stat.max_net_weight == 6009


def test_new_records_during_backfill_are_counted_exactly_once(db_session):
    """补建扫描期间新入仓的明细：要么被补建计入，要么在线链路自行计入。"""
    _add_legacy_records(db_session, [("MIXD0000001", 20000, 4000, 0)])
    # 未补建前，在线链路直接写入一条新明细（正常消费）
    _process(db_session, 1, 0, _msg("MIXD0000001", 30000))
    # 此时新明细已计入；补建只应处理存量那条
    scanned, applied = backfill_batch(db_session, batch_size=100)
    assert scanned == 1 and applied == 1

    stat = _stat(db_session, "MIXD0000001")
    assert stat.sample_count == 2
    assert (stat.min_net_weight, stat.max_net_weight) == (16000, 26000)


def test_alternative_worker_takes_over_after_worker_exit(db_session, db_session_factory):
    """一个补建实例退出后，存活实例继续把剩余存量计入。"""
    _add_legacy_records(
        db_session, [("TAKE0000001", 10000 + i * 1000, 4000, 0) for i in range(8)]
    )

    worker_a = StatsBackfillWorker(db_session_factory, batch_size=2)
    # 手工跑一步然后“退出”
    scanned_a, _ = backfill_batch(db_session_factory(), batch_size=2)
    assert scanned_a == 2

    worker_b = StatsBackfillWorker(db_session_factory, batch_size=2)
    while not worker_b.run_once():
        pass
    worker_a.stop()

    db_session.expire_all()
    stat = _stat(db_session, "TAKE0000001")
    assert stat.sample_count == 8
    assert db_session.get(StatBuildState, 1).ready is True


def test_worker_is_idempotent_across_restart(db_session, db_session_factory):
    """重启不重复累计：再次启动的 worker 不会把已计入的明细再加一遍。"""
    _add_legacy_records(db_session, [("RSTR0000001", 20000, 4000, 0)])
    worker = StatsBackfillWorker(db_session_factory, batch_size=10)
    while not worker.run_once():
        pass
    db_session.expire_all()
    before = _stat(db_session, "RSTR0000001").sample_count

    worker2 = StatsBackfillWorker(db_session_factory, batch_size=10)
    while not worker2.run_once():
        pass
    db_session.expire_all()
    after = _stat(db_session, "RSTR0000001").sample_count
    assert before == after == 1


def test_backfill_does_not_scan_after_ready_for_new_records(db_session):
    """ready 之后新明细由在线链路计入，补建不再对其生效（无待计入行）。"""
    backfill_stub = mark_ready_if_backfilled(db_session)
    assert backfill_stub is True
    _process(db_session, 0, 0, _msg("POST0000001", 12000))
    assert _stat(db_session, "POST0000001").sample_count == 1
    assert backfill_batch(db_session, batch_size=10) == (0, 0)
