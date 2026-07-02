#!/usr/bin/env bash
# One-shot benchmark runner. Uses a system-installed `clickhouse` binary if
# available (brew install clickhouse), otherwise downloads the official
# standalone binary (~250MB) to ./clickhouse. Starts a local server, applies
# the schema, runs the load benchmark at 10x average traffic, prints results.
set -euo pipefail
cd "$(dirname "$0")"

if command -v clickhouse >/dev/null 2>&1; then
  CH=clickhouse
elif [ -x ./clickhouse ]; then
  CH=./clickhouse
else
  case "$(uname -s)-$(uname -m)" in
    Darwin-arm64)  URL=https://builds.clickhouse.com/master/macos-aarch64/clickhouse ;;
    Darwin-x86_64) URL=https://builds.clickhouse.com/master/macos/clickhouse ;;
    Linux-aarch64) URL=https://builds.clickhouse.com/master/aarch64/clickhouse ;;
    Linux-x86_64)  URL=https://builds.clickhouse.com/master/amd64/clickhouse ;;
    *) echo "Unsupported platform; install ClickHouse manually" >&2; exit 1 ;;
  esac
  echo "Downloading ClickHouse binary from $URL ..."
  curl -fL -o clickhouse "$URL" && chmod +x clickhouse
  CH=./clickhouse
fi

if ! curl -fsS http://localhost:8123/ping >/dev/null 2>&1; then
  echo "Starting local ClickHouse server..."
  mkdir -p ch-data
  (cd ch-data && "$([ "$CH" = ./clickhouse ] && echo ../clickhouse || echo "$CH")" server >../server.log 2>&1 &)
  for _ in $(seq 1 60); do
    curl -fsS http://localhost:8123/ping >/dev/null 2>&1 && break
    sleep 0.5
  done
  curl -fsS http://localhost:8123/ping >/dev/null 2>&1 || {
    echo "ClickHouse failed to start; see server.log" >&2; exit 1;
  }
fi

$CH client --multiquery <schema.sql
$CH client --query "TRUNCATE TABLE analytics.events"

python3 run_benchmark.py "$@"
python3 measure.py | tee results/results.md
