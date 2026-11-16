"""同箱复称净重统计：持久增量维护 + 在线补建。

核心不变量
==========

1. **恰好一次计入**：每条 ``net_weight_records`` 行带 ``stat_applied`` 标记，
   明细落库与聚合更新处于同一数据库事务：
   - 先以条件 UPDATE ``WHERE id=:id AND stat_applied=false`` 抢占标记，
     只有 rowcount=1 的执行者才允许更新聚合；
   - 抢占与聚合更新随后一起提交；任一步失败整体回滚，标记恢复 false；
   - 重放/迟到消息在明细层已被幂等键拦截，补建与在线链路即使同时
     碰到同一行，也只有一方能抢到标记。因此写库失败、重放、双实例
     同时启动都不会重复累计。

2. **不扫明细的查询**：巡检接口只读 ``container_net_weight_stats`` 聚合表
   （按箱号主键 / 极差索引），不重扫 ``net_weight_records``。

3. **在线补建**：存量明细 ``stat_applied=false``，由每个实例的后台线程
   按 id 顺序分批领取。只做行级条件更新与聚合 upsert，绝不 ``LOCK TABLE``，
   因此双实例消费与补正可继续提交。某个实例退出后，其余实例的补建线程
   会继续扫描未计入行，自动接续。

4. **完成前只报准备中**：``stat_build_state.ready`` 为 false 时巡检接口
   只返回 PREPARING，不暴露半成品聚合；扫描不到未计入行时以
   ``UPDATE ... WHERE ready=false`` 原子置位。置为 ready 之后，任何新明细
   都在自身事务内与统计一起提交，查询不会看到“有明细、无统计”的中间态。
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone

from sqlalchemy import and_, case, func, or_, select, text, tuple_, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import (
    ContainerNetWeightStat,
    NetWeightRecord,
    StatBuildState,
)

logger = logging.getLogger(__name__)

STAT_STATE_ID = 1


# --------------------------------------------------------------------------
# 聚合写入（恰好一次）
# --------------------------------------------------------------------------

def _dialect_insert(session: Session):
    """返回当前方言的 insert(...)，用于 ON CONFLICT 原子 upsert。"""
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        return pg_insert
    if dialect == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        return sqlite_insert
    return None


def _claim_record(session: Session, record_id: int) -> bool:
    """原子抢占“该明细已计入统计”标记；返回是否由本次调用抢到。

    ``stat_applied=false`` 的条件更新在 PostgreSQL 下会拿到行锁并在
    等待后重新校验谓词，因此并发调用只有一个能得到 rowcount=1。
    """
    result = session.execute(
        update(NetWeightRecord)
        .where(
            NetWeightRecord.id == record_id,
            NetWeightRecord.stat_applied.is_(False),
        )
        .values(stat_applied=True)
        .execution_options(synchronize_session=False)
    )
    return result.rowcount == 1


def _source_key(record: NetWeightRecord) -> tuple[str, int, int]:
    return (record.topic, record.source_partition, record.source_offset)


def _stat_values(group: dict) -> dict:
    """一组（同箱）明细首次计入时的插入值。"""
    return {
        "container_no": group["container_no"],
        "sample_count": group["count"],
        "min_net_weight": group["min_weight"],
        "max_net_weight": group["max_weight"],
        "weight_range": group["max_weight"] - group["min_weight"],
        "min_source_topic": group["min_source"][0],
        "min_source_partition": group["min_source"][1],
        "min_source_offset": group["min_source"][2],
        "max_source_topic": group["max_source"][0],
        "max_source_partition": group["max_source"][1],
        "max_source_offset": group["max_source"][2],
        "updated_at": func.now(),
    }


def _upsert_group(session: Session, group: dict, insert_fn) -> None:
    """单条 SQL 原子 upsert：样本数累加，min/max 及对应源位置按需替换。

    **与处理顺序无关的确定性结果**：
    - min/max 取真正的极值；
    - 同值时取 ``(topic, partition, offset)`` 字典序最小的一处源位置。

    因此同一批明细无论被谁、以什么顺序计入，最终源位置一致——
    这让补建与在线链路的任意交错都收敛到相同结果。
    """
    stmt = insert_fn(ContainerNetWeightStat).values(**_stat_values(group))
    excluded = stmt.excluded
    stat = ContainerNetWeightStat

    ex_min_source = tuple_(
        excluded.min_source_topic,
        excluded.min_source_partition,
        excluded.min_source_offset,
    )
    cur_min_source = tuple_(
        stat.min_source_topic, stat.min_source_partition, stat.min_source_offset
    )
    ex_max_source = tuple_(
        excluded.max_source_topic,
        excluded.max_source_partition,
        excluded.max_source_offset,
    )
    cur_max_source = tuple_(
        stat.max_source_topic, stat.max_source_partition, stat.max_source_offset
    )

    new_min_wins = excluded.min_net_weight < stat.min_net_weight
    new_max_wins = excluded.max_net_weight > stat.max_net_weight
    # 同值且候选源位置更靠前时替换（保证多实例/多批次收敛到同一处）
    min_tie_wins = and_(
        excluded.min_net_weight == stat.min_net_weight, ex_min_source < cur_min_source
    )
    max_tie_wins = and_(
        excluded.max_net_weight == stat.max_net_weight, ex_max_source < cur_max_source
    )

    take_new_min = or_(new_min_wins, min_tie_wins)
    take_new_max = or_(new_max_wins, max_tie_wins)

    new_min = case((take_new_min, excluded.min_net_weight), else_=stat.min_net_weight)
    new_max = case((take_new_max, excluded.max_net_weight), else_=stat.max_net_weight)

    stmt = stmt.on_conflict_do_update(
        index_elements=[stat.container_no],
        set_={
            "sample_count": stat.sample_count + excluded.sample_count,
            "min_net_weight": new_min,
            "max_net_weight": new_max,
            # 新值在本语句内计算，避免依赖 SET 求值顺序
            "weight_range": new_max - new_min,
            "min_source_topic": case(
                (take_new_min, excluded.min_source_topic), else_=stat.min_source_topic
            ),
            "min_source_partition": case(
                (take_new_min, excluded.min_source_partition),
                else_=stat.min_source_partition,
            ),
            "min_source_offset": case(
                (take_new_min, excluded.min_source_offset),
                else_=stat.min_source_offset,
            ),
            "max_source_topic": case(
                (take_new_max, excluded.max_source_topic), else_=stat.max_source_topic
            ),
            "max_source_partition": case(
                (take_new_max, excluded.max_source_partition),
                else_=stat.max_source_partition,
            ),
            "max_source_offset": case(
                (take_new_max, excluded.max_source_offset),
                else_=stat.max_source_offset,
            ),
            "updated_at": func.now(),
        },
    )
    session.execute(stmt)


def _new_group(record: NetWeightRecord) -> dict:
    return {
        "container_no": record.container_no,
        "count": 1,
        "min_weight": record.net_weight,
        "max_weight": record.net_weight,
        "min_source": _source_key(record),
        "max_source": _source_key(record),
    }


def _fold_into_group(group: dict, record: NetWeightRecord) -> None:
    """把一条明细并入同箱分组，源位置取同值中最早的一处。"""
    group["count"] += 1
    key = _source_key(record)
    if record.net_weight < group["min_weight"]:
        group["min_weight"] = record.net_weight
        group["min_source"] = key
    elif record.net_weight == group["min_weight"] and key < group["min_source"]:
        group["min_source"] = key
    if record.net_weight > group["max_weight"]:
        group["max_weight"] = record.net_weight
        group["max_source"] = key
    elif record.net_weight == group["max_weight"] and key < group["max_source"]:
        group["max_source"] = key


def _apply_with_read_modify_write(session: Session, group: dict) -> None:
    """非 PostgreSQL/SQLite 方言的保守回退：行锁读改写（本测试环境不用）。"""
    container_no = group["container_no"]
    stat = session.get(ContainerNetWeightStat, container_no, with_for_update=True)
    if stat is None:
        session.add(ContainerNetWeightStat(**_stat_values(group)))
        session.flush()
        return

    stat.sample_count += group["count"]
    if group["min_weight"] < stat.min_net_weight:
        stat.min_net_weight = group["min_weight"]
        (
            stat.min_source_topic,
            stat.min_source_partition,
            stat.min_source_offset,
        ) = group["min_source"]
    if group["max_weight"] > stat.max_net_weight:
        stat.max_net_weight = group["max_weight"]
        (
            stat.max_source_topic,
            stat.max_source_partition,
            stat.max_source_offset,
        ) = group["max_source"]
    stat.weight_range = stat.max_net_weight - stat.min_net_weight
    stat.updated_at = datetime.now(timezone.utc)
    session.flush()


def apply_net_record_to_stats(
    session: Session, record: NetWeightRecord, insert_fn=None
) -> bool:
    """把一条净重明细计入同箱统计，返回本次是否真正计入。

    必须与明细的插入/补正状态变更处于同一事务，由调用方 commit。
    未抢到标记（已被其他执行者计入）时返回 False，不产生任何聚合变更。
    """
    if record.id is None:
        session.flush()
    if not _claim_record(session, record.id):
        return False

    # 条件 UPDATE 已把数据库中的标记置为 true（事务内可见），
    # 同步内存对象避免同事务后续读到过期值
    record.stat_applied = True
    apply_groups_to_stats(session, [_new_group(record)], insert_fn=insert_fn)
    return True


def apply_groups_to_stats(
    session: Session, groups: list[dict], *, insert_fn=None
) -> None:
    """按箱号排序后逐箱 upsert 聚合。

    全局统一的加锁顺序（箱号升序）避免多实例补建时交叉等待聚合行
    造成死锁；同一箱的多条明细已在内存合并为一次 upsert。
    """
    if not groups:
        return
    insert_fn = insert_fn or _dialect_insert(session)
    ordered = sorted(groups, key=lambda g: g["container_no"])
    for group in ordered:
        if insert_fn is not None:
            _upsert_group(session, group, insert_fn)
        else:
            _apply_with_read_modify_write(session, group)


# --------------------------------------------------------------------------
# 补建状态与 worker
# --------------------------------------------------------------------------

def ensure_state_row(session: Session) -> StatBuildState:
    """确保补建状态行存在（幂等，可并发调用）。"""
    state = session.get(StatBuildState, STAT_STATE_ID)
    if state is not None:
        return state
    try:
        session.add(StatBuildState(id=STAT_STATE_ID, ready=False))
        session.commit()
    except IntegrityError:
        session.rollback()
    state = session.get(StatBuildState, STAT_STATE_ID)
    assert state is not None
    return state


def count_pending_records(session: Session) -> int:
    """尚未计入统计的明细数（走部分索引，不扫全表）。"""
    return int(
        session.scalar(
            select(func.count())
            .select_from(NetWeightRecord)
            .where(NetWeightRecord.stat_applied.is_(False))
        )
        or 0
    )


def _apply_batch_lock_timeout(session: Session) -> None:
    """给补建事务设置锁等待上限，避免异常环境下整批无限期挂起。

    只影响本事务（``SET LOCAL``），不改变池中连接的全局行为；超时后
    事务报错回滚，``stat_applied`` 标记随之回滚，下一轮重试即可。
    """
    if session.get_bind().dialect.name != "postgresql":
        return
    session.execute(
        text("SET LOCAL lock_timeout = :ms"),
        {"ms": f"{get_settings().stats_lock_timeout_ms}ms"},
    )


def backfill_batch(session: Session, batch_size: int) -> tuple[int, int]:
    """扫描并计入一批存量明细，返回 (扫描行数, 实际计入行数)。

    单批一个事务。PostgreSQL 下对候选行 ``FOR UPDATE SKIP LOCKED``：
    多实例并行补建时各自领取不同的行，不会互相等待，也不会重复领取；
    锁粒度是行级，且每批很快提交，绝不会锁整表。SQLite 忽略该子句，
    由 ``stat_applied`` 条件更新保证恰好一次。
    """
    pending_stmt = (
        select(NetWeightRecord)
        .where(NetWeightRecord.stat_applied.is_(False))
        .order_by(NetWeightRecord.id)
        .limit(batch_size)
    )
    if session.get_bind().dialect.name == "postgresql":
        _apply_batch_lock_timeout(session)
        pending_stmt = pending_stmt.with_for_update(skip_locked=True)

    records = list(session.scalars(pending_stmt).all())
    if not records:
        return 0, 0

    # 批量抢占标记：只有本次真正抢到的行才并入聚合，重复领取/并发补建安全。
    # PostgreSQL 用一条 RETURNING 语句原子抢占并拿回 id（SKIP LOCKED 已避免
    # 与其他实例争抢同一批行）；SQLite 无 RETURNING 语义差异，逐行条件更新。
    is_postgres = session.get_bind().dialect.name == "postgresql"
    if is_postgres:
        claimed_ids = {
            row[0]
            for row in session.execute(
                update(NetWeightRecord)
                .where(
                    NetWeightRecord.id.in_([r.id for r in records]),
                    NetWeightRecord.stat_applied.is_(False),
                )
                .values(stat_applied=True)
                .returning(NetWeightRecord.id)
                .execution_options(synchronize_session=False)
            ).all()
        }
    else:
        claimed_ids = {r.id for r in records if _claim_record(session, r.id)}
    if not claimed_ids:
        session.commit()
        return len(records), 0

    groups: dict[str, dict] = {}
    for record in records:
        if record.id not in claimed_ids:
            continue
        record.stat_applied = True
        group = groups.get(record.container_no)
        if group is None:
            groups[record.container_no] = _new_group(record)
        else:
            _fold_into_group(group, record)

    apply_groups_to_stats(session, list(groups.values()))
    session.commit()
    return len(records), len(claimed_ids)


def mark_ready_if_backfilled(session: Session) -> bool:
    """确认无待计入明细后原子置 ready（并发下只生效一次）。

    这里用**不加锁**的计数，而不是上一批 ``SKIP LOCKED`` 的结果：
    其他实例正在处理的批次，其 ``stat_applied=true`` 尚未提交、对本事务
    仍显示为 false，因此计数 > 0，绝不会在别人提交统计之前抢先置 ready。
    只有计数确认归零（所有存量明细都已提交计入）才置位，保证 READY 之后
    对外查询看到的是完整统计。
    """
    ensure_state_row(session)
    if count_pending_records(session) > 0:
        session.rollback()
        return False

    session.execute(
        update(StatBuildState)
        .where(
            StatBuildState.id == STAT_STATE_ID,
            StatBuildState.ready.is_(False),
        )
        .values(ready=True, ready_at=func.now())
        .execution_options(synchronize_session=False)
    )
    session.commit()
    state = session.get(StatBuildState, STAT_STATE_ID)
    return bool(state and state.ready)


class StatsBackfillWorker:
    """每个实例一个后台补建线程；多实例并行安全，退出可被其他实例接续。

    READY 之后线程**不退出**，转为低频巡检：仍有 ``stat_applied=false``
    的历史/异常遗留行时继续计入；正常情况下只做一次空计数的廉价检查。
    这样即使本实例的补建进程退出、或存在旧版本实例写入的未计入行，
    存活实例也会自动接续，不会永久漏算。
    """

    def __init__(
        self,
        session_factory,
        *,
        batch_size: int = 200,
        idle_seconds: float = 0.2,
        sweep_seconds: float = 5.0,
        max_backoff_seconds: float = 10.0,
    ) -> None:
        self._session_factory = session_factory
        self._batch_size = batch_size
        self._idle_seconds = idle_seconds
        self._sweep_seconds = sweep_seconds
        self._max_backoff = max_backoff_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="stats-backfill", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 15.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def run_once(self) -> bool:
        """执行一步；返回是否已确认补建完成（ready 且无待计入行）。

        只有真正把状态置为 READY 才返回 True；若其他实例正在处理最后的
        一批（本事务仍看得到未计入行），返回 False 继续下一轮，绝不提前退出。
        """
        session = self._session_factory()
        try:
            ensure_state_row(session)
            scanned, applied = backfill_batch(session, self._batch_size)
            if scanned:
                logger.info(
                    "stats backfill: scanned=%s applied=%s", scanned, applied
                )
                # 整批都已被别的实例计入时也继续下一批
                return False
            return mark_ready_if_backfilled(session)
        finally:
            session.close()

    def _run(self) -> None:
        backoff = self._idle_seconds
        while not self._stop.is_set():
            try:
                if self.run_once():
                    backoff = self._idle_seconds
                    if not self._stop.wait(self._sweep_seconds):
                        continue
                    logger.debug("stats sweep tick")
                    continue
                backoff = self._idle_seconds
            except SQLAlchemyError:
                logger.exception("stats backfill batch failed; will retry")
                self._stop.wait(backoff)
                backoff = min(self._max_backoff, max(backoff * 2, 0.5))
            except Exception:
                # 线程不能因未预期异常永久退出：存活实例需能继续接续补建
                logger.exception("unexpected stats backfill error; will retry")
                self._stop.wait(backoff)
                backoff = min(self._max_backoff, max(backoff * 2, 0.5))
        logger.info("stats backfill worker stopped")


# --------------------------------------------------------------------------
# 巡检查询（只读聚合表）
# --------------------------------------------------------------------------

def get_build_state(session: Session) -> StatBuildState | None:
    return session.get(StatBuildState, STAT_STATE_ID)


def get_container_stat(
    session: Session, container_no: str
) -> ContainerNetWeightStat | None:
    return session.get(ContainerNetWeightStat, container_no)


def list_container_stats(
    session: Session,
    *,
    min_range: int | None = None,
    max_range: int | None = None,
    min_samples: int = 2,
    container_no: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[ContainerNetWeightStat], int]:
    """按最小极差升序返回复称箱及其总数。

    ``min_samples`` 默认 2：只列出“复称箱”；传 1 可查看全部已称量箱。
    """
    conds = []
    if container_no:
        conds.append(ContainerNetWeightStat.container_no == container_no)
    if min_samples is not None:
        conds.append(ContainerNetWeightStat.sample_count >= min_samples)
    if min_range is not None:
        conds.append(ContainerNetWeightStat.weight_range >= min_range)
    if max_range is not None:
        conds.append(ContainerNetWeightStat.weight_range <= max_range)

    base = select(ContainerNetWeightStat)
    count_stmt = select(func.count()).select_from(ContainerNetWeightStat)
    if conds:
        base = base.where(*conds)
        count_stmt = count_stmt.where(*conds)

    total = int(session.scalar(count_stmt) or 0)
    rows = list(
        session.scalars(
            base.order_by(
                ContainerNetWeightStat.weight_range.asc(),
                ContainerNetWeightStat.sample_count.desc(),
                ContainerNetWeightStat.container_no.asc(),
            )
            .limit(limit)
            .offset(offset)
        ).all()
    )
    return rows, total


def count_containers(session: Session) -> int:
    return int(
        session.scalar(select(func.count()).select_from(ContainerNetWeightStat)) or 0
    )
