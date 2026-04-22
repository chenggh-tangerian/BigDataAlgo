from pathlib import Path

import pandas as pd


def load_snapshot_jsonl(jsonl_path: str) -> pd.DataFrame:
    """Load exported JSONL dataset into a DataFrame."""
    return pd.read_json(jsonl_path, lines=True)


def split_views(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split unified dataset into hot_topic and sample views."""
    hot_topics = df[df["record_type"] == "hot_topic"].copy()
    samples = df[df["record_type"] == "sample"].copy()
    return hot_topics, samples


def build_training_inputs(df: pd.DataFrame) -> list[dict]:
    """Build simple dataset input list for downstream model pipeline."""
    samples = df[df["record_type"] == "sample"]
    return [{"text": row["keyword"]} for _, row in samples.iterrows()]


def demo() -> None:
    latest = sorted(Path("./dataset_exports").glob("*.jsonl"))[-1]
    df = load_snapshot_jsonl(str(latest))
    hot_topics, samples = split_views(df)

    print("Loaded:", latest)
    print("Total rows:", len(df))
    print("Top hot topics:")
    print(hot_topics[["keyword", "approx_count", "rank"]].head(10))
    print("Sample rows:")
    print(samples[["keyword", "sample_index"]].head(10))

    train_inputs = build_training_inputs(df)
    print("Training input size:", len(train_inputs))
    print("First item:", train_inputs[0] if train_inputs else None)


if __name__ == "__main__":
    demo()
