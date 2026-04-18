import os
import random
import time
from typing import Dict, List

import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import LongType, StringType, StructField, StructType


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "search_topic")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "./.checkpoint/sublinear-demo")
TRIGGER_INTERVAL_SECONDS = os.getenv("TRIGGER_INTERVAL_SECONDS", "2 seconds")

DEFAULT_MG_K = int(os.getenv("MG_K", "50"))
DEFAULT_RESERVOIR_N = int(os.getenv("RESERVOIR_N", "20"))


r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


mg_k = DEFAULT_MG_K
reservoir_n = DEFAULT_RESERVOIR_N
mg_state: Dict[str, int] = {}
reservoir: List[str] = []
total_processed = 0
stream_index = 0


def update_runtime_config() -> None:
    global mg_k, reservoir_n, mg_state, reservoir

    config = r.hgetall("control_config")
    if not config:
        return

    try:
        new_k = int(config.get("k", mg_k))
        if new_k > 0 and new_k != mg_k:
            mg_k = new_k
            if len(mg_state) > mg_k:
                mg_state = dict(sorted(mg_state.items(), key=lambda item: item[1], reverse=True)[:mg_k])
    except ValueError:
        pass

    try:
        new_n = int(config.get("n", reservoir_n))
        if new_n > 0 and new_n != reservoir_n:
            reservoir_n = new_n
            if len(reservoir) > reservoir_n:
                reservoir = reservoir[:reservoir_n]
    except ValueError:
        pass


def apply_reset_if_requested() -> None:
    global mg_state, reservoir, total_processed, stream_index

    if r.get("control_reset") != "1":
        return

    mg_state = {}
    reservoir = []
    total_processed = 0
    stream_index = 0
    r.delete("hot_topics", "samples")
    r.delete("control_reset")


def misra_gries_update(word: str) -> None:
    global mg_state

    if word in mg_state:
        mg_state[word] += 1
        return

    if len(mg_state) < mg_k:
        mg_state[word] = 1
        return

    keys_to_drop = []
    for key in list(mg_state.keys()):
        mg_state[key] -= 1
        if mg_state[key] <= 0:
            keys_to_drop.append(key)
    for key in keys_to_drop:
        del mg_state[key]


def reservoir_update(word: str) -> None:
    global reservoir, stream_index

    stream_index += 1

    if len(reservoir) < reservoir_n:
        reservoir.append(word)
        return

    replace_at = random.randint(1, stream_index)
    if replace_at <= reservoir_n:
        reservoir[replace_at - 1] = word


def write_state_to_redis(batch_count: int) -> None:
    topk = sorted(mg_state.items(), key=lambda item: item[1], reverse=True)

    pipe = r.pipeline()
    pipe.delete("hot_topics")
    if topk:
        pipe.hset("hot_topics", mapping={word: str(count) for word, count in topk})
    pipe.delete("samples")
    if reservoir:
        pipe.rpush("samples", *reservoir)
    pipe.set("total_processed", total_processed)
    pipe.set("last_batch_count", batch_count)
    pipe.set("last_update_epoch", int(time.time()))
    pipe.execute()


def process_batch(df, epoch_id: int) -> None:
    global total_processed

    update_runtime_config()
    apply_reset_if_requested()

    batch_df = df.select("keyword").where(col("keyword").isNotNull())

    batch_count = 0
    for row in batch_df.toLocalIterator():
        word = row["keyword"]
        if not word:
            continue
        batch_count += 1
        misra_gries_update(word)
        reservoir_update(word)

    if batch_count == 0:
        return

    total_processed += batch_count
    write_state_to_redis(batch_count)
    print(
        "epoch="
        f"{epoch_id}"
        ", "
        f"batch={batch_count}"
        ", "
        f"total={total_processed}"
        ", "
        f"mg_k={mg_k}"
        ", "
        f"reservoir_n={reservoir_n}"
    )


def create_spark_session() -> SparkSession:
    package = os.getenv("SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    spark = (
        SparkSession.builder.appName("SublinearRealtimeSearch")
        .config("spark.jars.packages", package)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def start_spark_stream() -> None:
    spark = create_spark_session()

    schema = StructType(
        [
            StructField("keyword", StringType(), True),
            StructField("timestamp", LongType(), True),
        ]
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
    )

    query = (
        parsed_df.writeStream.outputMode("append")
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER_INTERVAL_SECONDS)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    start_spark_stream()