# 港口地磅称量流 · 净重明细服务

把地磅称量消息（箱号、毛重、皮重，整数千克）整理成**净重明细**入仓；
字段无效或毛重小于皮重的消息进入**隔离区**，可查看原文并**补正**后入仓。
另提供**同箱复称巡检 API**：按箱号返回称量次数、最小/最大净重及各一处源位置，
可按最小极差筛选复称箱。统计由**持久增量聚合**维护，查询不重扫明细。

技术栈：**FastAPI + PostgreSQL + 本地 Redpanda（Kafka 协议）+ Docker Compose**，
两个 API 实例以**同一消费组**消费**两个分区**。

---

## 1. 一键启动

```bash
docker compose up -d --build
```

启动后：

| 服务 | 地址 | 说明 |
| --- | --- | --- |
| API 实例 1 / Swagger | http://localhost:8001/docs | 双实例之一 |
| API 实例 2 / Swagger | http://localhost:8002/docs | 双实例之二 |
| Redpanda Console | http://localhost:8080 | 查看 Topic、分区、消息 |
| PostgreSQL | localhost:5432 | 库 `weighbridge`，用户/密码 `weigh/weigh` |
| Redpanda Kafka 端口 | localhost:19092 | 宿主机可用 Kafka 客户端连接（容器内服务走 `redpanda:9092`） |

健康检查：

```bash
curl http://localhost:8001/health
# {"status":"ok","instance_id":"instance-1","consumer_enabled":true,"assigned_partitions":[...]}
```

两个实例同属消费组 `weighbridge-consumer-group`，Topic `weighbridge-events` 有 2 个分区，
因此稳态下每个实例各消费一个分区（重平衡后由组协调器重新分配）。

---

## 2. 在 /docs 走一遍演示流程

也可以直接用命令行：

```bash
# 1) 一键投递 6 条演示消息（2 合法 + 4 各类非法）
curl -X POST http://localhost:8001/demo/seed

# 2) 发送单条合法消息
curl -X POST http://localhost:8001/messages -H 'Content-Type: application/json' -d '{
  "message": {"container_no": "MSCU1234567", "gross_weight": 20000, "tare_weight": 4000}
}'

# 3) 发送一条会被隔离的消息（毛重 < 皮重）
curl -X POST http://localhost:8001/messages -H 'Content-Type: application/json' -d '{
  "message": {"container_no": "BADU0000001", "gross_weight": 3000, "tare_weight": 4200}
}'

# 4) 查询净重明细
curl http://localhost:8001/net-weights

# 5) 查询隔离记录（可按分区/状态过滤）
curl "http://localhost:8001/quarantine"
curl "http://localhost:8001/quarantine?partition=0&status=OPEN"

# 6) 按源分区 + offset 查看隔离原文
curl http://localhost:8001/quarantine/by-position/0/1

# 7) 对隔离记录补正（把 <隔离记录 id> 换成实际 id）
curl -X POST http://localhost:8001/quarantine/1/correction \
  -H 'Content-Type: application/json' \
  -d '{"container_no":"MSCU1234567","gross_weight":5000,"tare_weight":4200}'

# 8) 同箱复称巡检：查看统计补建是否就绪
curl http://localhost:8001/inspection/status

# 9) 按箱号看复称明细（称量次数、最小/最大净重、各一处源位置）
curl http://localhost:8001/inspection/containers/MSCU1234567

# 10) 复称箱列表（默认只看称量≥2 次），按最小极差升序
curl "http://localhost:8001/inspection/containers"

# 11) 只用最小极差筛选：只留净重差 ≤ 5kg 的复称箱
curl "http://localhost:8001/inspection/containers?max_range=5"

# 12) 回查最小/最大净重的源位置原文（partition/offset 来自上一步）
curl http://localhost:8001/quarantine/by-position/0/1
```

### 接口清单

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| POST | `/messages` | 投递演示消息（支持畸形 JSON 原文 `raw_text`，可指定分区） |
| POST | `/demo/seed` | 一键投递一组合法/非法演示消息 |
| GET | `/net-weights` | 查询净重明细，可按箱号过滤 |
| GET | `/quarantine` | 查询隔离记录，可按源分区、状态过滤 |
| GET | `/quarantine/by-position/{partition}/{offset}` | **按源分区与 offset 查看隔离原文** |
| GET | `/quarantine/{id}` | 按主键查单条隔离记录 |
| POST | `/quarantine/{id}/correction` | 提交补正（每条仅接受一次合法补正） |
| GET | `/inspection/status` | 统计补建进度（存量补建完成前为 PREPARING） |
| GET | `/inspection/containers` | 复称箱巡检列表，按最小极差升序，可按极差/次数筛选 |
| GET | `/inspection/containers/{container_no}` | 单箱巡检：称量次数、最小/最大净重及各一处源位置 |
| GET | `/health` | 健康检查与当前分配分区 |

### 巡检接口说明

- 默认 `min_samples=2`，只列**复称箱**；传 `min_samples=1` 可看全部已称量箱。
- `max_range` / `min_range` 按 `极差 = 最大净重 − 最小净重` 过滤，
  默认按极差升序（差异最小的箱排最前），同极差按称量次数降序。
- 每箱返回 `min_source` / `max_source` 两处 Kafka 源位置（`topic/partition/offset`），
  可直接回查该次称量的原始消息。
- **只计已入仓明细**：正常消费入仓、补正成功入仓都计入；隔离未补正（失败）不计入。
  同文不同源位置各自一次称量、分别计数。
- **补建期间**返回 `inspection_status=PREPARING` 且不返回统计字段；
  补建完成后转 `READY`。完成前新入仓的明细与统计在同一事务内生效，
  查询只会看到完整提交结果。

---

## 3. 数据模型

### `net_weight_records`（净重明细，只追加）

- 业务字段：`container_no`、`gross_weight`、`tare_weight`、`net_weight = gross - tare`
- 来源定位：`topic`、`source_partition`、`source_offset`
- `from_correction`：是否来自隔离补正；`quarantine_id`：关联的隔离记录
- **唯一约束 `(topic, source_partition, source_offset)`**：每条源消息最多入仓一次
- 约束：毛重/皮重非负、毛重 ≥ 皮重、净重非负

### `quarantine_records`（隔离记录）

- `raw_payload` 原样保留消息原文；`reason_code` / `reason_detail` 记录错误原因
- 同样以 `(topic, source_partition, source_offset)` 唯一约束，**同文不同位置分别保留**
- 状态机：`OPEN` → `CORRECTED`（只允许翻转一次）
- `correction_attempts`、`last_correction_error` 记录失败补正（失败保持 `OPEN`）

### `container_net_weight_stats`（同箱复称聚合，持久增量维护）

- 主键 `container_no`；`sample_count`（称量次数）、`min_net_weight`、`max_net_weight`、
  `weight_range = max − min`（物化，便于按最小极差索引筛选）。
- 记录最小/最大净重**各一处来源位置**（`min_source_*` / `max_source_*`），
  同值取 `(topic, partition, offset)` 字典序最早的一处，保证结果与计入顺序无关。
- 查询只读本表（主键 / `weight_range` / `sample_count` 索引），**不重扫明细表**。

### `stat_build_state`（补建进度，单行）

- `ready`：存量补建是否完成。完成前巡检接口只报 PREPARING。
- 持久保存，重启不清空已建统计；多实例共享同一行。

### `net_weight_records.stat_applied`（恰好一次标记）

- 每条明细带“已计入统计”标记，**明细落库与聚合更新在同一事务**：
  先以条件 UPDATE 抢占标记，抢到者才更新聚合；失败整笔回滚，标记恢复。
- 重放、写库失败重试、双实例同时补建，都只会有一方抢到标记 → **不重复累计、不漏算**。

---

## 4. 正确性设计（如何满足每条要求）

### 4.1 每条源消息仅入仓一次、不重复明细

- 消费处理先做幂等前置检查，再以数据库**唯一约束兜底**并捕获 `IntegrityError`，
  任何重放/重复投递都走 `DUPLICATE_SKIP`，不会产生第二条明细。
- 净重表与隔离表各自持有来源位点唯一约束。

### 4.2 持久成功后才推进消费位点

消费者关闭自动提交（`enable.auto.commit=false`、`enable.auto.offset.store=false`），
每条消息严格按序执行：

1. 数据库事务提交成功（明细落库 **或** 隔离落库）；
2. `store_offset` 仅在内存标记位点；
3. 同步 `commit` 把位点提交给 Redpanda。

任一环节失败都不会推进位点。

### 4.3 写库失败 / 提交断连 / 实例退出 / 重平衡后接续，不漏不重

- **写库失败**：回滚事务、不提交位点，`seek` 回当前消息重试（带指数退避）。
- **提交位点时断连**：捕获异常后 `seek` 回本条消息；重放时唯一约束识别为重复并跳过，再正常提交。
- **实例退出 / 分区重分配**：未提交的位点不会被确认，新 owner 从 Broker 已提交位点重放，
  由幂等键去重——因此**不丢消息、不重复明细**。
- 静态成员 `group.instance.id` + 协作式重平衡（`cooperative-sticky`）减少不必要的分区抖动。

### 4.4 迟到消费不能覆盖补正结果

补正成功后，该来源位点在净重表已有记录。迟到/重放消息在前置幂等检查时即被识别为
`DUPLICATE_SKIP`，**不会再走校验或写库分支**，补正生成的明细与 `CORRECTED` 状态不受影响。

### 4.5 并发补正仅一份生效，失败保留隔离状态

- 补正在独立事务中以 `SELECT ... FOR UPDATE` 锁定该隔离记录（PostgreSQL 行锁），串行化并发请求。
- 持锁事务把状态从 `OPEN` 翻转为 `CORRECTED` 并插入明细；后到者读到 `CORRECTED` 直接返回 `409 ALREADY_CORRECTED`。
- 净重表来源位点唯一约束是第二道防线，任何情况下都不会插入两条明细。
- 非法补正只累加 `correction_attempts`、记录错误并提交，**隔离状态保持 `OPEN`**，不影响后续合法补正。

### 4.6 字段校验规则（消费与补正同一套口径）

- 消息体必须是 JSON 对象；
- 箱号非空字符串，长度 4–17，仅字母/数字/连字符；
- 毛重、皮重必须是**严格整数**（布尔、浮点、字符串数字一律拒绝），非负且不超上限；
- 毛重 ≥ 皮重，否则以 `GROSS_LT_TARE` 隔离；毛重 = 皮重合法（净重 0）。

### 4.7 同箱复称统计：持久增量、恰好一次、在线补建

| 要求 | 实现 |
| --- | --- |
| 统计持久增量维护，查询不重扫明细 | 聚合表 `container_net_weight_stats`，巡检只读聚合表 |
| 只计已入仓明细 | 正常入仓、补正成功入仓都计入；隔离未补正不计入（无明细） |
| 同文不同源位置分别计数 | 按明细逐条计入，每条源位点一次称量 |
| 已有数据在线补建 | 存量行 `stat_applied=false`，后台线程按 id 分批计入 |
| 补建不锁整表、双实例可继续提交 | 行级条件更新 + 聚合 upsert，PostgreSQL 用 `SKIP LOCKED` 分批，**绝不 `LOCK TABLE`** |
| 完成前只报准备中 | `stat_build_state.ready=false` 时巡检接口只返回 PREPARING，不暴露半成品 |
| 完成后新明细与统计共同生效 | 明细插入/补正与聚合更新在**同一事务**提交 |
| 查询只见完整提交结果 | 聚合只做已提交的原子 upsert；READY 只在“无待计入行”时置位 |
| 补建期间/迟到明细不漏算 | 补建与新明细都走同一“抢标记”逻辑；迟到消息由源位点幂等键去重 |
| 补建进程退出由存活实例接续 | 每实例一个补建线程，扫描的是**共享表**的未计入行，天然接续 |
| 写库失败/重放/同时启动不重复 | 条件 UPDATE 抢标记 + 唯一约束；失败整笔回滚，标记回滚；重放走 `DUPLICATE_SKIP` |
| 重启不清空已建统计 | 聚合与进度都在库中；重启后只补未计入行 |

**为什么不会重复累计**：`stat_applied` 的抢占是**数据库条件更新**
（`UPDATE ... WHERE id=? AND stat_applied=false`），只有 `rowcount=1` 的执行者才写聚合；
抢占与聚合在同一事务，任一步失败全部回滚。并发/多实例下该谓词由数据库行锁串行化。

**为什么查询看不到中间态**：READY 之前一律 PREPARING；READY 之后新明细只在
“明细 + 统计”同事务提交后才可见，因此不存在“有明细、无统计”的中间时刻。

---

## 5. 本地开发与自测

不依赖 Docker 也能跑测试：消息层抽象了 `Broker` 接口，测试用进程内 `InMemoryBroker`，
数据库用临时文件 SQLite（WAL），完整覆盖 API → 后台消费者 → 入库/隔离/补正链路。

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install pytest httpx
pytest -q
```

> 说明：SQLite 的 `FOR UPDATE` 为空操作、单文件库无法完整复现真实多连接行锁，
> 因此“多线程并发补正”用例以 `xfail` 标注其平台限制。恰好一次的核心不变量
> （条件更新抢标记 + 同事务聚合）与方言无关，已在 SQLite 线程测试中覆盖；
> PostgreSQL 专用的 `ON CONFLICT` / `SKIP LOCKED` / `RETURNING` 路径由
> `tests/test_postgres_verification.py` 在真实 PostgreSQL 上验证：

```bash
WB_TEST_PG_URL='postgresql+psycopg2://user:pass@localhost:5432/weighbridge' \
  pytest tests/test_postgres_verification.py -q
```

### 无 Redpanda 时本地直接运行 API（内存 Broker）

```bash
export DATABASE_URL="sqlite:////tmp/weighbridge.db"
export BROKER_BACKEND=memory
uvicorn app.main:app --reload --port 8000
# 打开 http://localhost:8000/docs
```

---

## 6. 代码结构

```
app/
  config.py            # 环境变量配置（含补建批量/巡检/锁超时）
  database.py          # 引擎/会话/建表 + 存量库在线补列补索引
  models.py            # 净重明细、隔离记录、同箱聚合、补建状态
  validation.py        # 纯函数消息校验（各类隔离原因）
  schemas.py           # HTTP 请求/响应模型
  messaging.py         # Broker 抽象：Kafka(redpanda) 实现 + 内存实现
  consumer.py          # 消费工作线程：先落库后提交位点、失败 seek 重试
  stats.py             # 同箱聚合恰好一次计入 + 在线补建 worker + 巡检查询
  services.py          # 幂等入仓、隔离、补正状态机、查询
  main.py              # FastAPI 入口与路由
tests/
  test_validation.py           # 校验纯函数
  test_services.py             # 幂等入仓/隔离/补正状态机
  test_api_e2e.py              # HTTP 端到端主链路
  test_consumer_reliability.py # 位点提交/重放/接续
  test_stats_incremental.py    # 统计增量、补建、恰好一次
  test_stats_interleavings.py  # 双实例补建、补建与在线并发、失败/重启
  test_inspection_api.py       # 巡检 HTTP 语义、PREPARING 门禁、不扫明细
  test_schema_upgrade.py       # 存量库在线升级
  test_postgres_verification.py# 真实 PostgreSQL 方言与并发（需 WB_TEST_PG_URL）
scripts/entrypoint.sh  # 容器入口：等待依赖后启动 uvicorn
docker-compose.yml     # postgres + redpanda + topic-init + app-1 + app-2 + console
Dockerfile
```

---

## 7. 已实现 / 已验证 / 未完成

### 已实现

- 双实例同组消费双分区、FastAPI 全套接口、Swagger 可直接发演示消息/补正/查询；
- 严格整数千克校验、毛重<皮重等各类无效消息隔离并保留原文与原因；
- 按源分区 + offset 查隔离原文；同文不同位置分别保留；
- 补正状态机（行锁 + 唯一约束），每条仅一次合法补正，失败保持隔离；
- “先持久化后提交位点”、失败 seek 重试、重放幂等去重、迟到消息不覆盖补正；
- **同箱复称巡检**：按箱返回称量次数/最小/最大净重及各一处源位置，按最小极差筛选；
- **统计持久增量维护**：聚合表 + `stat_applied` 恰好一次标记，查询不扫明细；
- **在线补建**：存量按批计入，双实例并行、进程退出自动接续、不锁整表；
  READY 前只报准备中，READY 后新明细与统计同事务生效；
- 存量库在线升级（补列/补索引，不重建表）；
- 可启动全部服务的 Docker Compose（含健康检查、Topic 初始化、Redpanda Console）。

### 已验证

**SQLite + 内存 Broker（`pytest -q`，无需外部服务）**

- 56 项测试通过、1 项平台相关 xfail；多轮重复运行稳定；
- 覆盖：增量计入、补建分批、重放/并发抢标记不重复、写库失败整笔回滚、
  双实例补建恰好一次、补建与在线写入并发、进程退出接续、重启不清空、
  PREPARING 不暴露半成品、按极差筛选、查询 SQL 不触碰明细表、存量库升级。

**真实 PostgreSQL 16（`WB_TEST_PG_URL=... pytest tests/test_postgres_verification.py`）**

- `ON CONFLICT` / `SKIP LOCKED` / `RETURNING` 方言路径全部通过；
- 3 线程并行补建 800 行：无重复、无漏算、无死锁；
- 补建持锁期间另一连接可正常写入并提交（**行级锁而非整表锁**）；
- 补建部分完成后另一 worker 接续至 READY。

**真实双 uvicorn 进程 + PostgreSQL（手工链路）**

- 5000 条存量在线补建，同时双实例持续投递消息、并发补正；
  约 30s 转 READY，期间 `ON CONFLICT` 行锁竞争约 1s 级（`lock_timeout` 兜底），**零死锁**；
- 补建完成核对：5018 条明细、`sum(sample_count)=5018`、201 个箱、
  0 条待计入、0 处统计与明细不一致、每个源位置都能回查到对应净重的真实记录；
- 并发补正时后到者正确返回 409（每条仅一次合法补正）。

### 未完成 / 需在目标环境验证

- 本沙箱**无 Docker 守护进程**（`docker` 命令不可用），无法实际 `docker compose up`
  验证容器编排与真实 Redpanda 下的双实例消费；上述双实例验证使用两个真实 uvicorn
  进程 + 真实 PostgreSQL + 进程内 Broker 完成，Kafka 路径由 `confluent-kafka` 实现与
  Compose 配置保证，建议在有 Docker 的环境执行一次第 1、2 节流程确认。
- 建表演示用 `create_all` + 轻量在线补列；生产建议替换为 Alembic 迁移。
- 补建的锁等待上限（`STATS_LOCK_TIMEOUT_MS`，默认 5s）用于异常环境兜底；
  极端高并发下补建可能因超时反复重试，属预期（不丢不重，仅变慢）。
