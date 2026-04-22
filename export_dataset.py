import argparse
import json
import os
from datetime import datetime
from pathlib import Path

import redis


def parse_hot_topics(client: redis.Redis):
    raw = client.hgetall("hot_topics")
    result = []
    for word, count in raw.items():
        try:
            result.append((word, int(count)))
        except ValueError:
            continue
    result.sort(key=lambda item: item[1], reverse=True)
    return result


def build_records(hot_topics, samples, total_processed, last_batch_count, last_update_epoch, exported_at):
    records = []

    for rank, (keyword, count) in enumerate(hot_topics, start=1):
        records.append(
            {
                "record_type": "hot_topic",
                "keyword": keyword,
                "approx_count": count,
                "rank": rank,
                "sample_index": None,
                "snapshot_total_processed": total_processed,
                "snapshot_last_batch_count": last_batch_count,
                "snapshot_last_update_epoch": last_update_epoch,
                "snapshot_exported_at": exported_at,
            }
        )

    for idx, keyword in enumerate(samples):
        records.append(
            {
                "record_type": "sample",
                "keyword": keyword,
                "approx_count": None,
                "rank": None,
                "sample_index": idx,
                "snapshot_total_processed": total_processed,
                "snapshot_last_batch_count": last_batch_count,
                "snapshot_last_update_epoch": last_update_epoch,
                "snapshot_exported_at": exported_at,
            }
        )

    return records


def export_snapshot(args: argparse.Namespace) -> tuple[Path, Path, int]:
    client = redis.Redis(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
        decode_responses=True,
    )

    hot_topics = parse_hot_topics(client)
    samples = client.lrange("samples", 0, -1)
    total_processed = int(client.get("total_processed") or 0)
    last_batch_count = int(client.get("last_batch_count") or 0)
    last_update_epoch = int(client.get("last_update_epoch") or 0)

    exported_at = int(datetime.now().timestamp())
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_prefix = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in args.prefix).strip("_") or "snapshot"

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    jsonl_path = output_dir / f"{safe_prefix}_{timestamp}.jsonl"
    meta_path = output_dir / f"{safe_prefix}_{timestamp}.meta.json"

    records = build_records(
        hot_topics,
        samples,
        total_processed,
        last_batch_count,
        last_update_epoch,
        exported_at,
    )

    with open(jsonl_path, "w", encoding="utf-8") as f:
        for row in records:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "exported_at": exported_at,
                "total_processed": total_processed,
                "last_batch_count": last_batch_count,
                "last_update_epoch": last_update_epoch,
                "hot_topic_count": len(hot_topics),
                "sample_count": len(samples),
                "record_count": len(records),
                "jsonl_file": str(jsonl_path),
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    return jsonl_path, meta_path, len(records)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export current Redis results to dataset files")
    parser.add_argument("--redis-host", default=os.getenv("REDIS_HOST", "localhost"))
    parser.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", "6379")))
    parser.add_argument("--redis-db", type=int, default=int(os.getenv("REDIS_DB", "0")))
    parser.add_argument("--output-dir", default="./dataset_exports")
    parser.add_argument("--prefix", default="search_snapshot")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    jsonl_path, meta_path, count = export_snapshot(args)
    print(f"Exported {count} records")
    print(f"JSONL: {jsonl_path}")
    print(f"Meta:  {meta_path}")


if __name__ == "__main__":
    main()
