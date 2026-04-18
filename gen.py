import argparse
import json
import os
import random
import subprocess
import time
import uuid
from pathlib import Path
from shutil import which

# ================= 数据分布设计 (核心改造区) =================
# 1. 头部词汇 (Heavy Hitters)：赋予极高的权重，模拟日常热搜
HEAD_WORDS = ["AI", "Spark", "DataLake", "Kafka", "Redis"]
HEAD_WEIGHTS = [2000, 1500, 1200, 1000, 800] 

# 2. 腰部词汇 (Mid-tail)：普通频率的词汇
MID_WORDS = [
    "Python", "Cloud", "GPU", "Docker", "VectorDB", "Analytics",
    "Streaming", "Realtime", "BigData", "Search", "Index",
    "Ranking", "NLP", "Agent", "Copilot"
]
MID_WEIGHTS = [100] * len(MID_WORDS)

# 3. 长尾词汇 (Long-tail / Noise)：生成 10,000 个毫无意义的 UUID 字符串！
# 目的：向观众证明，如果不用 MG 算法，这 1 万个只出现一两次的词会把精确 Hash Map 的内存撑爆！
print("正在初始化长尾干扰词库 (10,000个)...")
TAIL_WORDS = [f"uuid_{str(uuid.uuid4())[:8]}" for _ in range(10000)]
TAIL_WEIGHTS = [1] * len(TAIL_WORDS) # 每个词权重极低

# 合并词库与权重
ALL_WORDS = HEAD_WORDS + MID_WORDS + TAIL_WORDS
ALL_WEIGHTS = HEAD_WEIGHTS + MID_WEIGHTS + TAIL_WEIGHTS
# =========================================================

class SearchLogGenerator:
    def __init__(self, args: argparse.Namespace) -> None:
        self.topic = args.topic
        self.bootstrap_servers = args.bootstrap_servers
        self.trigger_file = Path(args.trigger_file)
        self.regular_rate = args.regular_rate
        self.burst_rate = args.burst_rate
        self.default_burst_word = args.burst_word
        self.kafka_bin = resolve_kafka_producer_script(args.kafka_home)
        if self.kafka_bin is None:
            raise FileNotFoundError(
                "Kafka producer script not found. Set --kafka-home, set KAFKA_HOME, "
                "or add kafka-console-producer.sh to PATH."
            )
            
        self.producer_proc = subprocess.Popen(
            [
                str(self.kafka_bin),
                "--bootstrap-server",
                self.bootstrap_servers,
                "--topic",
                self.topic,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )

    def current_mode_and_target(self) -> tuple[str, str]:
        """
        检查 trigger.txt 是否存在。
        如果存在，读取文件内容作为引爆词（支持前端动态下发）。
        """
        if self.trigger_file.exists():
            try:
                with open(self.trigger_file, "r") as f:
                    word = f.read().strip()
                return "burst", word if word else self.default_burst_word
            except Exception:
                return "burst", self.default_burst_word
        return "normal", ""

    def make_payload(self, mode: str, burst_word: str) -> dict:
        if mode == "burst":
            # 爆发模式下，90% 的流量是引爆词，剩下 10% 依然保持常规噪音（更真实）
            if random.random() < 0.90:
                keyword = burst_word
            else:
                keyword = random.choices(ALL_WORDS, weights=ALL_WEIGHTS, k=1)[0]
        else:
            # 正常模式下，按长尾权重分布抽取
            keyword = random.choices(ALL_WORDS, weights=ALL_WEIGHTS, k=1)[0]
            
        return {
            "keyword": keyword,
            "timestamp": int(time.time() * 1000),
        }

    def run(self) -> None:
        print(
            f"🚀 Generator Started! Topic={self.topic}, "
            f"Regular Rate={self.regular_rate}/s, Burst Rate={self.burst_rate}/s"
        )

        try:
            while True:
                loop_start = time.time()
                mode, burst_word = self.current_mode_and_target()
                rate = self.burst_rate if mode == "burst" else self.regular_rate

                if self.producer_proc.stdin is None:
                    raise RuntimeError("Kafka producer stdin is unavailable")

                # 生成该秒内的所有数据
                for _ in range(rate):
                    payload = self.make_payload(mode, burst_word)
                    self.producer_proc.stdin.write(json.dumps(payload, ensure_ascii=True) + "\n")

                self.producer_proc.stdin.flush()
                
                # 终端状态打印
                if mode == "burst":
                    print(f"🚨 [BURST MODE] Target: {burst_word} | Rate: {rate} msgs/s")
                else:
                    print(f"🟢 [NORMAL] Rate: {rate} msgs/s")

                # 精确控制 1 秒发送 1 批
                elapsed = time.time() - loop_start
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
                    
        finally:
            if self.producer_proc.stdin:
                self.producer_proc.stdin.close()
            self.producer_proc.terminate()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka search log generator")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "search_topic"))
    parser.add_argument("--kafka-home", default=os.getenv("KAFKA_HOME"))
    parser.add_argument("--trigger-file", default="trigger.txt")
    parser.add_argument("--regular-rate", type=int, default=300) # 调高了一点日常频率
    parser.add_argument("--burst-rate", type=int, default=2000)  # 爆发时频率飙升
    parser.add_argument("--burst-word", default="VibeCoding")
    return parser.parse_args()


def resolve_kafka_producer_script(kafka_home: str | None) -> Path | None:
    candidates = []

    if kafka_home:
        candidates.append(Path(kafka_home).expanduser() / "bin" / "kafka-console-producer.sh")

    env_home = os.getenv("KAFKA_HOME")
    if env_home:
        candidates.append(Path(env_home).expanduser() / "bin" / "kafka-console-producer.sh")

    in_path = which("kafka-console-producer.sh")
    if in_path:
        candidates.append(Path(in_path))

    common_homes = [
        Path.cwd() / ".tools" / "kafka",
        Path.cwd() / "tools" / "kafka",
        Path.home() / "kafka",
        Path("/opt/kafka"),
    ]
    for home in common_homes:
        candidates.append(home / "bin" / "kafka-console-producer.sh")

    for path in candidates:
        if path.exists() and path.is_file():
            return path

    for home in sorted(Path.home().glob("tools/kafka*")):
        path = home / "bin" / "kafka-console-producer.sh"
        if path.exists() and path.is_file():
            return path

    return None


if __name__ == "__main__":
    args = parse_args()
    SearchLogGenerator(args).run()