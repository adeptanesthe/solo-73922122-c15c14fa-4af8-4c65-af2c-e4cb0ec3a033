"""巡检接口行为：PREPARING 门禁、按最小极差筛选、只读聚合表。

使用 TestClient + 内存 Broker + 临时 SQLite，验证 HTTP 层语义；
并发与补建算法由 test_stats_incremental / test_stats_interleavings 覆盖。
"""
from __future__ import annotations

import time

from sqlalchemy import event

from app.models import NetWeightRecord, StatBuildState


def _wait_until(predicate, timeout=10.0, interval=0.05):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"条件在 {timeout}s 内未满足，最后值={last!r}")


def _send(client, container_no, gross, tare=4000, partition=None):
    body = {"message": {"container_no": container_no,
                        "gross_weight": gross, "tare_weight": tare}}
    if partition is not None:
        body["partition"] = partition
    r = client.post("/messages", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def test_ready_after_backfill_and_inspection_fields(client, db_setup):
    _wait_until(lambda: client.get("/inspection/status").json()["ready"])
    _send(client, "MSCU1234567", 20000)
    _send(client, "MSCU1234567", 20500)
    _send(client, "MSCU1234567", 22000)

    def stat_ready():
        body = client.get("/inspection/containers/MSCU1234567").json()
        return body if body.get("weighing_count") == 3 else None

    body = _wait_until(stat_ready)
    assert body["inspection_status"] == "READY"
    assert body["min_net_weight"] == 16000
    assert body["max_net_weight"] == 18000
    assert body["weight_range"] == 2000
    assert body["min_source"]["topic"] == "weighbridge-events"
    assert body["max_source"]["topic"] == "weighbridge-events"
    # 两处源位置互不相同（净重不同）
    assert (body["min_source"]["partition"], body["min_source"]["offset"]) != (
        body["max_source"]["partition"], body["max_source"]["offset"])


def test_preparing_never_exposes_partial_stats(client, db_setup):
    """存量未补建完成时，巡检只报准备中，不暴露半成品聚合。"""
    import app.main as main_module
    from app.database import get_database

    # 先停掉后台补建，制造“补建未完成”的确定状态
    worker = main_module._stats_worker
    assert worker is not None
    worker.stop()

    session = get_database().session()
    try:
        session.merge(StatBuildState(id=1, ready=False, ready_at=None))
        session.add(NetWeightRecord(
            container_no="PART0000001", gross_weight=10000, tare_weight=4000,
            net_weight=6000, topic="t", source_partition=0, source_offset=999,
            from_correction=False, stat_applied=False,
        ))
        session.commit()
    finally:
        session.close()

    status = client.get("/inspection/status").json()
    assert status["inspection_status"] == "PREPARING" and status["ready"] is False

    one = client.get("/inspection/containers/PART0000001").json()
    assert one["inspection_status"] == "PREPARING"
    assert one["weighing_count"] is None and one["min_net_weight"] is None

    listing = client.get("/inspection/containers").json()
    assert listing["inspection_status"] == "PREPARING"
    assert listing["items"] == [] and listing["total"] == 0

    # 恢复补建：存量被计入、状态转 READY，之前隐藏的数据才可见
    from app.stats import StatsBackfillWorker

    survivor = StatsBackfillWorker(db_setup, batch_size=10, sweep_seconds=0.5)
    try:
        deadline = time.time() + 10
        while time.time() < deadline and not survivor.run_once():
            pass
        _wait_until(lambda: client.get("/inspection/status").json()["ready"])
        body = client.get("/inspection/containers/PART0000001").json()
        assert body["inspection_status"] == "READY"
        assert body["weighing_count"] == 1
    finally:
        survivor.stop()


def test_filter_by_min_range_returns_reweighed_boxes(client, db_setup):
    _wait_until(lambda: client.get("/inspection/status").json()["ready"])

    # A: 净重 8000/8010 极差 10；B: 8000/9000 极差 1000；C: 单次不算复称箱
    _send(client, "RANG0000001", 12000)
    _send(client, "RANG0000001", 12010)
    _send(client, "RANG0000002", 12000)
    _send(client, "RANG0000002", 13000)
    _send(client, "RANG0000003", 12000)

    def ready():
        a = client.get("/inspection/containers/RANG0000001").json()
        b = client.get("/inspection/containers/RANG0000002").json()
        if a.get("weighing_count") == 2 and b.get("weighing_count") == 2:
            return a, b
        return None

    _wait_until(ready)

    # 默认 min_samples=2：C 不出现；按极差升序，A 在 B 前
    page = client.get("/inspection/containers", params={"limit": 500}).json()
    names = [i["container_no"] for i in page["items"]]
    assert "RANG0000003" not in names
    assert names.index("RANG0000001") < names.index("RANG0000002")
    ranges = [i["weight_range"] for i in page["items"]]
    assert ranges == sorted(ranges)

    # 最小极差筛选：max_range=10 只留 A
    filtered = client.get(
        "/inspection/containers", params={"max_range": 10, "min_samples": 2}
    ).json()
    only = {i["container_no"] for i in filtered["items"]}
    assert "RANG0000001" in only and "RANG0000002" not in only

    # min_samples=1 能看到单次箱
    all_boxes = client.get("/inspection/containers", params={"min_samples": 1}).json()
    assert "RANG0000003" in {i["container_no"] for i in all_boxes["items"]}


def test_correction_contributes_one_more_weighing(client, db_setup):
    _wait_until(lambda: client.get("/inspection/status").json()["ready"])
    _send(client, "CORR0000001", 20000)
    _send(client, "CORR0000001", 20100)
    # 隔离后补正：只有补正成功才计入一次
    _send(client, "CORR0000001", 100, tare=200)

    def quarantined():
        rows = client.get("/quarantine").json()
        return rows[0] if rows else None

    q = _wait_until(quarantined)
    r = client.post(f"/quarantine/{q['id']}/correction", json={
        "container_no": "CORR0000001", "gross_weight": 24000, "tare_weight": 4000,
    })
    assert r.json()["accepted"] is True

    def counted():
        body = client.get("/inspection/containers/CORR0000001").json()
        return body if body.get("weighing_count") == 3 else None

    body = _wait_until(counted)
    assert body["min_net_weight"] == 16000
    assert body["max_net_weight"] == 20000
    # 最大净重来自补正入仓的明细
    assert body["max_source"]["partition"] == q["source_partition"]
    assert body["max_source"]["offset"] == q["source_offset"]


def test_query_does_not_scan_net_weight_records(client, db_setup):
    """巡检查询只读聚合表：把明细表访问记录打点，断言 SQL 不触碰它。"""
    from app.database import get_database

    _wait_until(lambda: client.get("/inspection/status").json()["ready"])
    _send(client, "SCAN0000001", 20000)
    _send(client, "SCAN0000001", 21000)
    _wait_until(lambda: client.get(
        "/inspection/containers/SCAN0000001").json().get("weighing_count") == 2)

    db = get_database()
    statements: list[str] = []

    @event.listens_for(db.engine, "before_cursor_execute")
    def _capture(conn, cursor, statement, params, context, executemany):
        statements.append(statement)

    try:
        client.get("/inspection/containers/SCAN0000001")
        client.get("/inspection/containers", params={"max_range": 10})
        client.get("/inspection/status")
    finally:
        event.remove(db.engine, "before_cursor_execute", _capture)

    joined = " ".join(statements).lower()
    assert "net_weight_records" not in joined
    assert "container_net_weight_stats" in joined


def test_unknown_container_ready_returns_404(client, db_setup):
    _wait_until(lambda: client.get("/inspection/status").json()["ready"])
    r = client.get("/inspection/containers/NOPE0000001")
    assert r.status_code == 404
