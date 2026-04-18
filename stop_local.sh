#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$ROOT_DIR/.runtime"
PID_DIR="$RUNTIME_DIR/pids"

KAFKA_HOME="${KAFKA_HOME:-}"
KAFKA_BIN=""
STREAMLIT_PORT="${STREAMLIT_PORT:-8501}"

log() {
  echo "[$(date +"%H:%M:%S")] $*"
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

resolve_kafka_bin() {
  if [[ -n "${KAFKA_HOME:-}" && -x "$KAFKA_HOME/bin/kafka-server-stop.sh" ]]; then
    KAFKA_HOME="$(cd "$KAFKA_HOME" && pwd)"
    KAFKA_BIN="$KAFKA_HOME/bin"
    return 0
  fi

  if command_exists kafka-server-stop.sh; then
    KAFKA_BIN="$(dirname "$(command -v kafka-server-stop.sh)")"
    return 0
  fi

  local candidates=(
    "$ROOT_DIR/.tools/kafka/bin"
    "$ROOT_DIR/tools/kafka/bin"
    "$HOME/kafka/bin"
    "/opt/kafka/bin"
  )

  local dir
  for dir in "${candidates[@]}"; do
    if [[ -x "$dir/kafka-server-stop.sh" ]]; then
      KAFKA_BIN="$dir"
      return 0
    fi
  done

  for dir in "$HOME"/tools/kafka*/bin; do
    if [[ -x "$dir/kafka-server-stop.sh" ]]; then
      KAFKA_BIN="$dir"
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

kill_port_listeners() {
  local port="$1"

  if command_exists fuser; then
    fuser -k -n tcp "$port" >/dev/null 2>&1 || true
  fi

  if command_exists lsof; then
    local pids
    pids="$(lsof -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | xargs echo -n || true)"
    if [[ -n "$pids" ]]; then
      log "killing listeners on port $port: $pids"
      kill $pids >/dev/null 2>&1 || true
      sleep 1
      kill -9 $pids >/dev/null 2>&1 || true
    fi
  fi
}

stop_from_pid_file() {
  local name="$1"
  local pid_file="$PID_DIR/$name.pid"

  if [[ ! -f "$pid_file" ]]; then
    log "$name not started by run script (pid file missing)"
    return 0
  fi

  local pid
  pid="$(cat "$pid_file")"

  if ! kill -0 "$pid" >/dev/null 2>&1; then
    log "$name already stopped"
    rm -f "$pid_file"
    return 0
  fi

  log "stopping $name (pid=$pid)"
  kill "$pid" >/dev/null 2>&1 || true

  for _ in $(seq 1 10); do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      rm -f "$pid_file"
      log "$name stopped"
      return 0
    fi
    sleep 1
  done

  log "$name did not exit in time, force killing"
  kill -9 "$pid" >/dev/null 2>&1 || true
  rm -f "$pid_file"
}

stop_by_pattern() {
  local name="$1"
  local pattern="$2"
  local pids

  pids="$(pgrep -f "$pattern" || true)"
  if [[ -z "$pids" ]]; then
    log "$name pattern clean"
    return 0
  fi

  log "stopping $name by pattern: $pattern"
  kill $pids >/dev/null 2>&1 || true
  sleep 2

  pids="$(pgrep -f "$pattern" || true)"
  if [[ -n "$pids" ]]; then
    log "force killing $name: $pids"
    kill -9 $pids >/dev/null 2>&1 || true
  fi
}

verify_clean() {
  local dirty=0
  local ports=("$STREAMLIT_PORT" 9092 2181 6379)

  for p in "${ports[@]}"; do
    if is_port_open "$p"; then
      log "port still open: $p"
      dirty=1
    fi
  done

  if pgrep -f "streamlit run dashboard.py|python3 process_batch.py|python3 gen.py|kafka.Kafka|zookeeper|SparkSubmit|spark-submit" >/dev/null 2>&1; then
    log "some target processes are still alive"
    dirty=1
  fi

  if [[ "$dirty" -eq 0 ]]; then
    log "all target services are fully stopped"
  else
    log "warning: some services may still be running"
  fi
}

main() {
  resolve_kafka_bin || true

  stop_from_pid_file "dashboard"
  stop_from_pid_file "generator"
  stop_from_pid_file "process_batch"
  stop_from_pid_file "kafka"
  stop_from_pid_file "redis"
  stop_from_pid_file "zookeeper"

  # Clean up manual/legacy launches not tracked by pid files.
  stop_by_pattern "dashboard" "streamlit run dashboard.py"
  stop_by_pattern "generator" "python3 gen.py|python gen.py"
  stop_by_pattern "process_batch" "python3 process_batch.py|python process_batch.py|SparkSubmit.*process_batch.py|spark-submit.*process_batch.py"

  if [[ -n "$KAFKA_BIN" && -x "$KAFKA_BIN/kafka-server-stop.sh" ]]; then
    "$KAFKA_BIN/kafka-server-stop.sh" >/dev/null 2>&1 || true
  fi
  if [[ -n "$KAFKA_BIN" && -x "$KAFKA_BIN/zookeeper-server-stop.sh" ]]; then
    "$KAFKA_BIN/zookeeper-server-stop.sh" >/dev/null 2>&1 || true
  fi

  stop_by_pattern "kafka" "kafka.Kafka"
  stop_by_pattern "zookeeper" "org.apache.zookeeper.server.quorum.QuorumPeerMain"
  stop_by_pattern "redis" "redis-server.*6379"

  # Final safeguard by well-known ports.
  kill_port_listeners "$STREAMLIT_PORT"
  kill_port_listeners 9092
  kill_port_listeners 2181
  kill_port_listeners 6379

  rm -f "$PID_DIR"/*.pid 2>/dev/null || true

  log "stop sequence done"
  verify_clean
}

main "$@"
