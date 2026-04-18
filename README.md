# Sublinear Real-time Search Analyzer

基于 Spark Structured Streaming 的实时热搜分析项目，核心算法为 Misra-Gries + Reservoir Sampling。

推荐使用本地脚本一键启动（不依赖 Docker）。

## 0. 这个项目的实用价值

它不是为了做“精确统计”，而是为了在高吞吐、低成本场景中做实时趋势发现与告警：

1. 搜索/推荐预热：在分钟级发现突发关键词，提前触发召回和缓存预热。
2. 舆情/运营监控：通过热词集中度判断突发事件，辅助运营快速响应。
3. 反作弊线索：样本多样性突然降低时，可作为刷词或异常流量的信号。
4. 资源成本控制：用固定内存上限替代全量词频统计，适合边缘节点和低配机器。

一句话定位：

这是一个“实时趋势雷达”，不是“离线精确报表”。

## 1. 项目结构

- `run_local.sh`: 本地一键启动（Zookeeper/Kafka/Redis + 处理链路 + Dashboard）
- `stop_local.sh`: 本地一键停止
- `gen.py`: 数据生成器（支持 `trigger.txt` 触发爆发模式）
- `process_batch.py`: Spark Structured Streaming 核心处理
- `dashboard.py`: Streamlit 实时可视化
- `requirements.txt`: Python 依赖
- `docker-compose.yml`: Docker 方案（可选）

## 2. 本地运行前提（非 Docker）

请确保以下条件满足：

1. Python 3.10+（建议使用你当前的 conda/base 环境）
2. Java 已安装（Spark/Kafka 需要）
3. Kafka 可用（满足以下任一条件即可）：
	- 已设置 `KAFKA_HOME`
	- `kafka-server-start.sh` 在 `PATH` 中
	- Kafka 位于常见目录（脚本会自动探测）
4. Redis 可用（本机安装了 `redis-server`，或 6379 端口已有 Redis 在跑）

如果你希望显式指定 Kafka 位置，启动前可设置：

```bash
export KAFKA_HOME=/path/to/kafka
```

## 3. 安装依赖

```bash
pip install -r requirements.txt
```

## 4. 一键启动（推荐）

首次建议先给脚本执行权限：

```bash
chmod +x run_local.sh stop_local.sh
```

启动：

```bash
./run_local.sh
```

启动成功后访问：

```text
http://localhost:8501
```

说明：

- 脚本会自动检查并拉起 Zookeeper(2181)、Kafka(9092)、Redis(6379)
- 自动创建 Kafka Topic（默认 `search_topic`）
- 自动启动：`process_batch.py`、`gen.py`、`dashboard.py`
- 日志目录：`.runtime/logs`
- 如果你设置了 `KAFKA_BOOTSTRAP_SERVERS` 或 `KAFKA_TOPIC`，脚本会在生成器和处理器中保持一致传参

## 5. 一键停止

```bash
./stop_local.sh
```

脚本会按顺序停止 dashboard、generator、process_batch、kafka、redis、zookeeper（仅停止由启动脚本托管的进程）。

## 6. 演示操作

1. 常态运行：观察 Top-K 热词和水库样本持续变化
2. 触发爆发模式（默认爆发词 `VibeCoding`）：

```bash
touch trigger.txt
```

也可以自定义爆发词（写入文件内容）：

```bash
echo "MyHotWord" > trigger.txt
```

3. 删除触发文件回到常态：

```bash
rm -f trigger.txt
```

4. 在看板侧边栏调节 `K` / `N`，或点击重置系统

### 6.1 演示讲解模板（30 秒）

1. 先看三类运营指标：热词集中度、样本多样性、数据新鲜度。
2. 常态下集中度不会极端，多样性保持稳定，说明流量健康。
3. 触发 `trigger.txt` 后，集中度迅速上升，看板告警出现，说明系统可在秒级发现异常热点。
4. 关闭触发后指标逐步回落，证明系统可用于实时观测事件生命周期。

## 7. 常见问题排查

### 7.1 页面能打开但数据不动

按顺序检查：

1. 确认不是爆发模式残留：

```bash
rm -f trigger.txt
```

2. 查看关键日志：

```bash
tail -n 100 .runtime/logs/process_batch.log
tail -n 100 .runtime/logs/generator.log
```

3. 重启全链路：

```bash
./stop_local.sh
./run_local.sh
```

### 7.2 8501 端口被占用

先停止旧实例再启动：

```bash
./stop_local.sh
./run_local.sh
```

## 8. 关键环境变量

- `KAFKA_HOME`：Kafka 安装目录（可选，优先级最高）
- `KAFKA_BOOTSTRAP_SERVERS`：Kafka 地址（默认 `localhost:9092`）
- `KAFKA_TOPIC`：Topic 名称（默认 `search_topic`）
- `STREAMLIT_PORT`：Dashboard 端口（默认 `8501`）
- `GEN_REGULAR_RATE`：常态生成速率（默认 `1500`）
- `GEN_BURST_RATE`：爆发生成速率（默认 `5000`）
- `STREAM_TRIGGER_INTERVAL`：Spark 触发间隔（默认 `1 seconds`）

示例（跨机器推荐写法）：

```bash
export KAFKA_HOME=/path/to/kafka
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=search_topic
./run_local.sh
```

## 9. Redis Key 说明

- `hot_topics` (Hash): Misra-Gries 当前候选词及计数
- `samples` (List): 水库样本
- `total_processed` (String): 累计处理条数
- `last_batch_count` (String): 最近批次处理条数
- `last_update_epoch` (String): 最近更新时间戳
- `control_config` (Hash): 看板下发的运行参数（k/n）
- `control_reset` (String): 重置控制信号

## 10. Docker 方案（可选）

如果你确实想用 Docker，只用于拉起基础设施：

```bash
docker compose up -d
```

其余 Python 模块仍可按本地方式启动，或继续使用 `./run_local.sh`（优先）。
