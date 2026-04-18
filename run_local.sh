#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$ROOT_DIR/.runtime"
PID_DIR="$RUNTIME_DIR/pids"
LOG_DIR="$RUNTIME_DIR/logs"

mkdir -p "$PID_DIR" "$LOG_DIR"

KAFKA_HOME="${KAFKA_HOME:-}"
KAFKA_BIN=""
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-search_topic}"
STREAMLIT_PORT="${STREAMLIT_PORT:-8501}"
GEN_REGULAR_RATE="${GEN_REGULAR_RATE:-1500}"
GEN_BURST_RATE="${GEN_BURST_RATE:-5000}"
STREAM_TRIGGER_INTERVAL="${STREAM_TRIGGER_INTERVAL:-1 seconds}"
CLEAN_START="${CLEAN_START:-1}"

log() {
  echo "[$(date +"%H:%M:%S")] $*"
}

fail() {
  echo "[ERROR] $*" >&2
  exit 1
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

resolve_kafka_home() {
  if [[ -n "${KAFKA_HOME:-}" && -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]]; then
    KAFKA_HOME="$(cd "$KAFKA_HOME" && pwd)"
    KAFKA_BIN="$KAFKA_HOME/bin"
    return 0
  fi

  if command_exists kafka-server-start.sh; then
    KAFKA_BIN="$(dirname "$(command -v kafka-server-start.sh)")"
    KAFKA_HOME="$(cd "$KAFKA_BIN/.." && pwd)"
    return 0
  fi

  local candidates=(
    "$ROOT_DIR/.tools/kafka"
    "$ROOT_DIR/tools/kafka"
    "$HOME/kafka"
    "/opt/kafka"
  )

  local dir
  for dir in "${candidates[@]}"; do
    if [[ -x "$dir/bin/kafka-server-start.sh" ]]; then
      KAFKA_HOME="$dir"
      KAFKA_BIN="$KAFKA_HOME/bin"
      return 0
    fi
  done

  for dir in "$HOME"/tools/kafka*; do
    if [[ -x "$dir/bin/kafka-server-start.sh" ]]; then
      KAFKA_HOME="$dir"
      KAFKA_BIN="$KAFKA_HOME/bin"
      return 0
    fi
  done

  return 1
}

is_port_open() {
  local port="$1"
  if command_exists ss; then
    ss -ltn "sport = :$port" | awk 'NR>1 { found=1 } END { exit(found?0:1) }'
  elif command_exists lsof; then
    lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
  else
    return 1
  fi
}

wait_for_port() {
  local port="$1"
  local timeout_seconds="$2"
  local waited=0

  while ! is_port_open "$port"; do
    sleep 1
    waited=$((waited + 1))
    if [[ "$waited" -ge "$timeout_seconds" ]]; then
      return 1
    fi
  done

  return 0
}

start_process() {
  local name="$1"
  local command="$2"
  local pid_file="$PID_DIR/$name.pid"
  local log_file="$LOG_DIR/$name.log"

  if [[ -f "$pid_file" ]]; then
    local existing_pid
    existing_pid="$(cat "$pid_file")"
    if kill -0 "$existing_pid" >/dev/null 2>&1; then
      log "$name already running (pid=$existing_pid)"
      return 0
    fi
    rm -f "$pid_file"
  fi

  log "starting $name"
  nohup bash -lc "cd '$ROOT_DIR'; exec $command" >"$log_file" 2>&1 &
  local pid=$!
  echo "$pid" >"$pid_file"

  sleep 1
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    fail "$name failed to start, check log: $log_file"
  fi
}

check_requirements() {
  command_exists python3 || fail "python3 not found"
  command_exists java || fail "java not found"

  [[ -n "${STREAM_TRIGGER_INTERVAL// }" ]] || fail "STREAM_TRIGGER_INTERVAL must be a non-empty string, e.g. '1 seconds'"

  resolve_kafka_home || fail "Kafka not found. Set KAFKA_HOME or add kafka-server-start.sh to PATH."

  if ! command_exists redis-server && ! is_port_open 6379; then
    fail "redis-server not found and 6379 not open. Install Redis or run one manually."
  fi

  if ! python3 -c "import pyspark, redis, pandas, streamlit" >/dev/null 2>&1; then
    fail "Python dependencies missing. Run: pip install -r requirements.txt"
  fi
}

start_infra() {
  if ! is_port_open 2181; then
    start_process "zookeeper" "'$KAFKA_BIN/zookeeper-server-start.sh' '$KAFKA_HOME/config/zookeeper.properties'"
    wait_for_port 2181 25 || fail "zookeeper did not open port 2181"
  else
    log "zookeeper already running (port 2181)"
  fi

  if ! is_port_open 9092; then
    start_process "kafka" "'$KAFKA_BIN/kafka-server-start.sh' '$KAFKA_HOME/config/server.properties'"
    wait_for_port 9092 35 || fail "kafka did not open port 9092"
  else
    log "kafka already running (port 9092)"
  fi

  if ! is_port_open 6379; then
    start_process "redis" "redis-server --port 6379"
    wait_for_port 6379 10 || fail "redis did not open port 6379"
  else
    log "redis already running (port 6379)"
  fi
}

ensure_topic() {
  log "ensuring kafka topic: $KAFKA_TOPIC"
  "$KAFKA_BIN/kafka-topics.sh" \
    --create \
    --if-not-exists \
    --topic "$KAFKA_TOPIC" \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" >/dev/null
}

start_apps() {
  start_process "process_batch" "env TRIGGER_INTERVAL_SECONDS='$STREAM_TRIGGER_INTERVAL' KAFKA_BOOTSTRAP_SERVERS='$KAFKA_BOOTSTRAP_SERVERS' KAFKA_TOPIC='$KAFKA_TOPIC' python3 process_batch.py"
  start_process "generator" "python3 gen.py --bootstrap-servers '$KAFKA_BOOTSTRAP_SERVERS' --topic '$KAFKA_TOPIC' --kafka-home '$KAFKA_HOME' --regular-rate '$GEN_REGULAR_RATE' --burst-rate '$GEN_BURST_RATE'"
  start_process "dashboard" "python3 -m streamlit run dashboard.py --server.headless true --server.port $STREAMLIT_PORT"
}

main() {
  if [[ "$CLEAN_START" == "1" ]] && [[ -x "$ROOT_DIR/stop_local.sh" ]]; then
    log "clean start enabled, stopping residual services first"
    "$ROOT_DIR/stop_local.sh" >/dev/null 2>&1 || true
  fi

  check_requirements
  start_infra
  ensure_topic
  start_apps

  log "all services started"
  log "dashboard: http://localhost:$STREAMLIT_PORT"
  log "kafka bootstrap: $KAFKA_BOOTSTRAP_SERVERS"
  log "kafka home: $KAFKA_HOME"
  log "generator rates: normal=${GEN_REGULAR_RATE}/s, burst=${GEN_BURST_RATE}/s"
  log "stream trigger interval: $STREAM_TRIGGER_INTERVAL"
  log "logs dir: $LOG_DIR"
  log "stop command: ./stop_local.sh"
}

main "$@"
