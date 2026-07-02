#!/usr/bin/env bash
# One-shot benchmark runner. Uses a system-installed `clickhouse` if available;
# otherwise downloads ClickHouse v25.8.25.37-lts pinned by version and verified
# by checksum where one is published. Starts a local server, applies the schema,
# runs the load benchmark at 10x average traffic, prints results, and shuts the
# server down again if this script was the one that started it.
set -euo pipefail
cd "$(dirname "$0")"

CH_VERSION=25.8.25.37
CH_TAG="v${CH_VERSION}-lts"
CH_RELEASE_URL="https://github.com/ClickHouse/ClickHouse/releases/download/${CH_TAG}"
# sha256 of clickhouse-macos-aarch64 from the release above, computed and pinned
# at submission time (no sha256 is published for the macOS single-binary assets).
MACOS_ARM64_SHA256=0fc1b330514f8c5dfe92f8d2640ea1c85903359df2561c322fdef4a8daa23867

fetch_clickhouse() {
  case "$(uname -s)-$(uname -m)" in
    Darwin-arm64)
      curl -fL -o clickhouse "${CH_RELEASE_URL}/clickhouse-macos-aarch64"
      echo "${MACOS_ARM64_SHA256}  clickhouse" | shasum -a 256 -c -
      ;;
    Darwin-x86_64)
      curl -fL -o clickhouse "${CH_RELEASE_URL}/clickhouse-macos"
      echo "note: version pinned to ${CH_TAG}; no checksum published for this asset"
      ;;
    Linux-x86_64|Linux-aarch64)
      local arch tgz
      arch=$([ "$(uname -m)" = x86_64 ] && echo amd64 || echo arm64)
      tgz="clickhouse-common-static-${CH_VERSION}-${arch}.tgz"
      curl -fLO "${CH_RELEASE_URL}/${tgz}"
      curl -fLO "${CH_RELEASE_URL}/${tgz}.sha512"
      sha512sum -c "${tgz}.sha512"
      mkdir -p .chpkg && tar -xzf "$tgz" -C .chpkg
      find .chpkg -type f -name clickhouse -path '*/bin/*' -exec mv {} ./clickhouse \;
      rm -rf .chpkg "$tgz" "$tgz.sha512"
      ;;
    *) echo "Unsupported platform; install ClickHouse ${CH_TAG} manually" >&2; exit 1 ;;
  esac
  chmod +x clickhouse
}

if command -v clickhouse >/dev/null 2>&1; then
  CH="$(command -v clickhouse)"
else
  [ -x ./clickhouse ] || fetch_clickhouse
  CH="$PWD/clickhouse"
fi

CH_PID=""
cleanup() {
  if [ -n "$CH_PID" ]; then
    echo "Stopping ClickHouse server (pid $CH_PID)..."
    kill "$CH_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

if ! curl -fsS http://localhost:8123/ping >/dev/null 2>&1; then
  echo "Starting local ClickHouse server..."
  mkdir -p ch-data
  (cd ch-data && exec "$CH" server >../server.log 2>&1) &
  CH_PID=$!
  for _ in $(seq 1 60); do
    curl -fsS http://localhost:8123/ping >/dev/null 2>&1 && break
    sleep 0.5
  done
  curl -fsS http://localhost:8123/ping >/dev/null 2>&1 || {
    echo "ClickHouse failed to start; see server.log" >&2; exit 1;
  }
fi

"$CH" client --multiquery <schema.sql
"$CH" client --query "TRUNCATE TABLE analytics.events"

python3 run_benchmark.py "$@"
python3 measure.py | tee results/results.md
