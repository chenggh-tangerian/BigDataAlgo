#!/bin/bash
PID_FILE=".runtime/pids/generator.pid"
if [[ -f "$PID_FILE" ]]; then
  PID=$(cat "$PID_FILE")
  if ! kill -0 "$PID" 2>/dev/null; then rm -f "$PID_FILE"; fi
fi
if [[ ! -f "$PID_FILE" ]]; then
  echo "Starting generator..."
  nohup python3 gen.py > .runtime/logs/generator.log 2>&1 &
  echo $! > "$PID_FILE"
fi
sleep 7
echo "LOGS:"
tail -n 10 .runtime/logs/generator.log
echo "KAFKA:"
~/tools/kafka_2.13-3.7.1/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic search_topic --max-messages 3 --timeout-ms 5000
echo "REDIS:"
redis-cli GET stream_stats_json
