"""FastAPI 入口：演示消息生产、隔离查询/补正、净重明细查询。

启动时：
1. 初始化数据库表；
2. 确保 Topic 存在（Kafka 后端）；
3. 启动后台消费工作线程（可通过 CONSUMER_ENABLED=false 关闭）。
"""
from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query, status
from sqlalchemy.orm import Session

from app import __version__
from app.config import get_settings
from app.consumer import ConsumerWorker
from app.database import get_database, get_session
from app.messaging import get_broker
from app.models import QuarantineRecord, QuarantineStatus
from app.schemas import (
    ContainerInspectionOut,
    ContainerInspectionPage,
    CorrectionRequest,
    CorrectionResult,
    HealthOut,
    InspectionStatusOut,
    NetWeightOut,
    ProducedMessage,
    ProduceMessageRequest,
    QuarantineOut,
    SourcePositionOut,
)
from app.services import (
    CorrectionError,
    apply_correction,
    get_quarantine_by_position,
    list_net_records,
    list_quarantine,
)
from app.stats import (
    StatsBackfillWorker,
    count_containers,
    get_build_state,
    get_container_stat,
    list_container_stats,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("weighbridge")

settings = get_settings()
_worker: ConsumerWorker | None = None
_stats_worker: StatsBackfillWorker | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _worker, _stats_worker
    db = get_database()
    db.init_schema()

    broker = get_broker()
    if settings.consumer_enabled:
        # Kafka 后端确保 Topic 就绪；内存后端在工厂里已建
        from app.messaging import KafkaBroker

        if isinstance(broker, KafkaBroker):
            try:
                broker.create_topic(settings.topic, settings.topic_partitions)
            except Exception:
                logger.exception("topic 初始化失败（若已存在可忽略）")
        _worker = ConsumerWorker(broker=broker)
        _worker.start()

    # 存量补建：每实例一个后台线程，多实例并行安全，退出可被存活实例接续。
    # 补建与在线写入均使用行级条件更新 + 聚合 upsert，不锁整表。
    if settings.stats_backfill_enabled:
        _stats_worker = StatsBackfillWorker(
            db.session,
            batch_size=settings.stats_backfill_batch_size,
            idle_seconds=settings.stats_backfill_idle_seconds,
            sweep_seconds=settings.stats_sweep_seconds,
        )
        _stats_worker.start()
    else:
        # 关闭补建时仍要保证状态行存在并完成一次就绪判定：
        # 空库/已补建完成的库可直接 READY，否则保持 PREPARING（直到有实例补建）。
        from app.stats import backfill_batch, ensure_state_row, mark_ready_if_backfilled

        session = db.session()
        try:
            ensure_state_row(session)
            if backfill_batch(session, settings.stats_backfill_batch_size)[0] == 0:
                mark_ready_if_backfilled(session)
        except Exception:
            logger.exception("补建关闭时初始化统计状态失败，巡检将保持 PREPARING")
        finally:
            session.close()

    logger.info("weighbridge API ready (instance=%s, version=%s)",
                settings.instance_id, __version__)
    try:
        yield
    finally:
        if _worker is not None:
            _worker.stop()
        if _stats_worker is not None:
            _stats_worker.stop()
        db.dispose()


app = FastAPI(
    title="港口地磅称量流 - 净重明细服务",
    description=(
        "## 功能\n"
        "- 接收含箱号、毛重、皮重（整数千克）的称量消息；\n"
        "- 字段无效或毛重 < 皮重的消息进入隔离区，可按源分区/offset 查看原文并补正；\n"
        "- 合法消息生成净重明细，每条源消息保证仅入仓一次；\n"
        "- 双实例同组消费两分区，持久化成功后才提交消费位点，失败可接续、不丢不重；\n"
        "- **同箱复称巡检**：按箱号返回称量次数、最小/最大净重及各一处源位置，\n"
        "  可按最小极差筛选复称箱；统计持久增量维护，查询不重扫明细；\n"
        "- 存量数据在线补建，完成前巡检只报准备中（PREPARING）。\n\n"
        "在本页 **Try it out** 即可发送演示消息、提交补正并查询/巡检结果。"
    ),
    version=__version__,
    lifespan=lifespan,
)


# --------------------------------------------------------------------------
# 健康检查与演示生产
# --------------------------------------------------------------------------

@app.get("/health", response_model=HealthOut, tags=["运维"])
def health(session: Session = Depends(get_session)) -> HealthOut:
    partitions = _worker.assigned_partitions if _worker else []
    try:
        ready = _inspection_ready(session)
    except Exception:  # 健康检查绝不能因统计查询失败而 500
        logger.exception("健康检查读取补建状态失败")
        ready = False
    return HealthOut(
        status="ok",
        instance_id=settings.instance_id,
        consumer_enabled=settings.consumer_enabled,
        assigned_partitions=partitions,
        inspection_status="READY" if ready else "PREPARING",
    )


@app.post(
    "/messages",
    response_model=ProducedMessage,
    status_code=status.HTTP_201_CREATED,
    tags=["演示消息"],
    summary="发送一条称量消息（可构造非法消息验证隔离）",
)
def produce_message(req: ProduceMessageRequest) -> ProducedMessage:
    if req.raw_text is not None:
        text = req.raw_text
    elif req.message is not None:
        text = json.dumps(req.message, ensure_ascii=False)
    else:
        raise HTTPException(status_code=422, detail="message 与 raw_text 至少提供一个")

    key = req.key
    if key is None and isinstance(req.message, dict):
        cn = req.message.get("container_no")
        if isinstance(cn, str):
            key = cn

    try:
        delivered = get_broker().producer().produce(
            settings.topic,
            value=text.encode("utf-8"),
            key=key.encode("utf-8") if key else None,
            partition=req.partition,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"消息投递失败: {exc}") from exc

    return ProducedMessage(
        topic=delivered.topic,
        partition=delivered.partition,
        offset=delivered.offset,
        key=key,
        delivered_text=text,
    )


@app.post(
    "/demo/seed",
    response_model=list[ProducedMessage],
    tags=["演示消息"],
    summary="一键投递一组演示消息（合法/非法/毛重小于皮重）",
)
def seed_demo() -> list[ProducedMessage]:
    demo_messages = [
        {"container_no": "MSCU1234567", "gross_weight": 20000, "tare_weight": 4000},
        {"container_no": "MSCU7654321", "gross_weight": 18500, "tare_weight": 3800},
        {"container_no": "BADU0000001", "gross_weight": 3000, "tare_weight": 4200},  # 毛<皮
        {"container_no": "BAD", "gross_weight": 1000, "tare_weight": 200},          # 箱号非法
        {"container_no": "BADU0000002", "gross_weight": "1000", "tare_weight": 200},  # 类型非法
        {"container_no": "MSCU1111111"},                                              # 缺字段
    ]
    producer = get_broker().producer()
    out: list[ProducedMessage] = []
    for msg in demo_messages:
        text = json.dumps(msg, ensure_ascii=False)
        delivered = producer.produce(
            settings.topic, value=text.encode("utf-8"),
            key=msg.get("container_no", "").encode("utf-8"), partition=None,
        )
        out.append(ProducedMessage(
            topic=delivered.topic, partition=delivered.partition,
            offset=delivered.offset, key=msg.get("container_no"), delivered_text=text,
        ))
    producer.flush()
    return out


# --------------------------------------------------------------------------
# 净重明细查询
# --------------------------------------------------------------------------

@app.get(
    "/net-weights",
    response_model=list[NetWeightOut],
    tags=["净重明细"],
    summary="查询净重明细（可按箱号过滤）",
)
def get_net_weights(
    container_no: str | None = Query(default=None, description="精确箱号过滤"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
) -> list[NetWeightOut]:
    rows = list_net_records(session, container_no=container_no, limit=limit, offset=offset)
    return [NetWeightOut.model_validate(r) for r in rows]


# --------------------------------------------------------------------------
# 隔离区：查询原文、补正
# --------------------------------------------------------------------------

@app.get(
    "/quarantine",
    response_model=list[QuarantineOut],
    tags=["隔离区"],
    summary="查询隔离记录（可按源分区、状态过滤）",
)
def get_quarantine_list(
    partition: int | None = Query(default=None, ge=0, description="源分区过滤"),
    status_filter: QuarantineStatus | None = Query(
        default=None, alias="status", description="OPEN / CORRECTED"
    ),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
) -> list[QuarantineOut]:
    rows = list_quarantine(
        session, partition=partition, status=status_filter, limit=limit, offset=offset
    )
    return [QuarantineOut.model_validate(r) for r in rows]


@app.get(
    "/quarantine/by-position/{partition}/{offset}",
    response_model=QuarantineOut,
    tags=["隔离区"],
    summary="按源分区与 offset 查看隔离原文",
)
def get_quarantine_position(
    partition: int,
    offset: int,
    session: Session = Depends(get_session),
) -> QuarantineOut:
    row = get_quarantine_by_position(
        session, topic=settings.topic, partition=partition, offset=offset
    )
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"分区 {partition} offset {offset} 不存在隔离记录",
        )
    return QuarantineOut.model_validate(row)


@app.post(
    "/quarantine/{quarantine_id}/correction",
    response_model=CorrectionResult,
    tags=["隔离区"],
    summary="提交补正（每条隔离记录仅接受一次合法补正）",
)
def correct_quarantine(
    quarantine_id: int,
    req: CorrectionRequest,
    session: Session = Depends(get_session),
) -> CorrectionResult:
    try:
        q, net, reason = apply_correction(
            session,
            quarantine_id,
            {
                "container_no": req.container_no,
                "gross_weight": req.gross_weight,
                "tare_weight": req.tare_weight,
            },
        )
    except CorrectionError as exc:
        raise HTTPException(status_code=exc.status_code, detail={
            "code": exc.code, "message": exc.detail,
        }) from exc

    return CorrectionResult(
        accepted=reason is None,
        quarantine_id=q.id,
        status=q.status,
        reason=reason,
        net_weight=NetWeightOut.model_validate(net) if net else None,
    )


@app.get(
    "/quarantine/{quarantine_id}",
    response_model=QuarantineOut,
    tags=["隔离区"],
    summary="按主键查询单条隔离记录",
)
def get_quarantine_one(
    quarantine_id: int,
    session: Session = Depends(get_session),
) -> QuarantineOut:
    row = session.get(QuarantineRecord, quarantine_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"隔离记录 {quarantine_id} 不存在")
    return QuarantineOut.model_validate(row)


# --------------------------------------------------------------------------
# 同箱复称巡检（只读持久统计，不扫明细）
# --------------------------------------------------------------------------

def _to_inspection(stat) -> ContainerInspectionOut:
    return ContainerInspectionOut(
        inspection_status="READY",
        container_no=stat.container_no,
        weighing_count=stat.sample_count,
        min_net_weight=stat.min_net_weight,
        max_net_weight=stat.max_net_weight,
        weight_range=stat.weight_range,
        min_source=SourcePositionOut(
            topic=stat.min_source_topic,
            partition=stat.min_source_partition,
            offset=stat.min_source_offset,
        ),
        max_source=SourcePositionOut(
            topic=stat.max_source_topic,
            partition=stat.max_source_partition,
            offset=stat.max_source_offset,
        ),
        updated_at=stat.updated_at,
    )


def _preparing_inspection(container_no: str) -> ContainerInspectionOut:
    return ContainerInspectionOut(
        inspection_status="PREPARING", container_no=container_no
    )


def _inspection_ready(session: Session) -> bool:
    state = get_build_state(session)
    return bool(state is not None and state.ready)


@app.get(
    "/inspection/status",
    response_model=InspectionStatusOut,
    tags=["复称巡检"],
    summary="查看统计补建进度（存量补建完成前为 PREPARING）",
)
def inspection_status(session: Session = Depends(get_session)) -> InspectionStatusOut:
    state = get_build_state(session)
    ready = bool(state is not None and state.ready)
    return InspectionStatusOut(
        inspection_status="READY" if ready else "PREPARING",
        ready=ready,
        containers=count_containers(session) if ready else 0,
        started_at=state.started_at if state else None,
        ready_at=state.ready_at if state else None,
    )


@app.get(
    "/inspection/containers",
    response_model=ContainerInspectionPage,
    tags=["复称巡检"],
    summary="按最小极差巡检复称箱（默认只看称量≥2 次的箱）",
)
def inspect_containers(
    max_range: int | None = Query(
        default=None, ge=0, description="最大极差过滤（净重 max-min ≤ 该值）"
    ),
    min_range: int | None = Query(
        default=None, ge=0, description="最小极差过滤（净重 max-min ≥ 该值）"
    ),
    min_samples: int = Query(
        default=2, ge=1, description="最少称量次数，默认 2 即只看复称箱；传 1 看全部"
    ),
    container_no: str | None = Query(default=None, description="精确箱号过滤"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
) -> ContainerInspectionPage:
    if not _inspection_ready(session):
        return ContainerInspectionPage(
            inspection_status="PREPARING", total=0, limit=limit, offset=offset
        )
    if (
        min_range is not None
        and max_range is not None
        and min_range > max_range
    ):
        raise HTTPException(status_code=422, detail="min_range 不能大于 max_range")

    rows, total = list_container_stats(
        session,
        min_range=min_range,
        max_range=max_range,
        min_samples=min_samples,
        container_no=container_no,
        limit=limit,
        offset=offset,
    )
    return ContainerInspectionPage(
        inspection_status="READY",
        total=total,
        limit=limit,
        offset=offset,
        items=[_to_inspection(r) for r in rows],
    )


@app.get(
    "/inspection/containers/{container_no}",
    response_model=ContainerInspectionOut,
    tags=["复称巡检"],
    summary="按箱号巡检：称量次数、最小/最大净重及各一处源位置",
)
def inspect_container(
    container_no: str,
    session: Session = Depends(get_session),
) -> ContainerInspectionOut:
    if not _inspection_ready(session):
        return _preparing_inspection(container_no)

    stat = get_container_stat(session, container_no)
    if stat is None:
        raise HTTPException(
            status_code=404,
            detail=f"箱号 {container_no} 尚无已入仓的净重明细",
        )
    return _to_inspection(stat)
