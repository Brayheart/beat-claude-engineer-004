#!/usr/bin/env bash
# One-shot benchmark runner. Downloads and uses a local ClickHouse
# v25.8.25.37-lts binary only; cached binaries are version/hash checked before
# reuse. Starts a local throwaway server, applies the schema, runs the load
# benchmark at 10x average traffic, prints results, and shuts the server down.
set -euo pipefail
cd "$(dirname "$0")"

CH_VERSION=25.8.25.37
CH_TAG="v${CH_VERSION}-lts"
CH_RELEASE_URL="https://github.com/ClickHouse/ClickHouse/releases/download/${CH_TAG}"
CH_BIN="$PWD/clickhouse"
CH_CACHE_META="$PWD/.clickhouse-cache"
CH_DATA_DIR=""
PLATFORM=""
ASSET_NAME=""
ASSET_SHA256=""

sha256_file() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    sha256sum "$1" | awk '{print $1}'
  fi
}

select_asset() {
  case "$(uname -s)-$(uname -m)" in
    Darwin-arm64)
      PLATFORM="Darwin-arm64"
      ASSET_NAME="clickhouse-macos-aarch64"
      ASSET_SHA256="0fc1b330514f8c5dfe92f8d2640ea1c85903359df2561c322fdef4a8daa23867"
      ;;
    Darwin-x86_64)
      PLATFORM="Darwin-x86_64"
      ASSET_NAME="clickhouse-macos"
      ASSET_SHA256="92f50a5df5099a2725934c877ecfc31f144c7f5a369715aadb7a0ab2bdea1162"
      ;;
    Linux-x86_64)
      PLATFORM="Linux-x86_64"
      ASSET_NAME="clickhouse-common-static-${CH_VERSION}-amd64.tgz"
      ASSET_SHA256="c6bab4cffe23bbe5c9871cad722ce58e8ac7e39874e68e5f9b22bb19de0c70fd"
      ;;
    Linux-aarch64)
      PLATFORM="Linux-aarch64"
      ASSET_NAME="clickhouse-common-static-${CH_VERSION}-arm64.tgz"
      ASSET_SHA256="14fd16373208a9de687b68f4b9b18b5abdc9ac766ecf9a6feb330e70735c6f2e"
      ;;
    *) echo "Unsupported platform; install ClickHouse ${CH_TAG} manually" >&2; exit 1 ;;
  esac
}

write_cache_meta() {
  {
    echo "tag=${CH_TAG}"
    echo "platform=${PLATFORM}"
    echo "asset=${ASSET_NAME}"
    echo "asset_sha256=${ASSET_SHA256}"
    echo "binary_sha256=$(sha256_file "$CH_BIN")"
  } > "$CH_CACHE_META"
}

cache_is_valid() {
  [ -x "$CH_BIN" ] || return 1
  [ -f "$CH_CACHE_META" ] || return 1

  grep -qx "tag=${CH_TAG}" "$CH_CACHE_META" || return 1
  grep -qx "platform=${PLATFORM}" "$CH_CACHE_META" || return 1
  grep -qx "asset=${ASSET_NAME}" "$CH_CACHE_META" || return 1
  grep -qx "asset_sha256=${ASSET_SHA256}" "$CH_CACHE_META" || return 1

  local expected_binary_sha actual_binary_sha version
  expected_binary_sha=$(awk -F= '/^binary_sha256=/ {print $2}' "$CH_CACHE_META")
  actual_binary_sha=$(sha256_file "$CH_BIN")
  [ "$actual_binary_sha" = "$expected_binary_sha" ] || return 1

  version=$("$CH_BIN" --version 2>/dev/null || true)
  case "$version" in
    *"$CH_VERSION"*) return 0 ;;
    *) return 1 ;;
  esac
}

fetch_clickhouse() {
  local tmp_asset
  tmp_asset="${ASSET_NAME}.download"
  rm -f "$tmp_asset" "$CH_BIN" "$CH_CACHE_META"
  rm -rf .chpkg

  echo "Downloading ClickHouse ${CH_TAG} for ${PLATFORM}..."
  curl -fL -o "$tmp_asset" "${CH_RELEASE_URL}/${ASSET_NAME}"

  local actual_asset_sha
  actual_asset_sha=$(sha256_file "$tmp_asset")
  if [ "$actual_asset_sha" != "$ASSET_SHA256" ]; then
    echo "Checksum mismatch for ${ASSET_NAME}" >&2
    echo "expected: $ASSET_SHA256" >&2
    echo "actual:   $actual_asset_sha" >&2
    exit 1
  fi

  case "$ASSET_NAME" in
    *.tgz)
      mkdir -p .chpkg
      tar -xzf "$tmp_asset" -C .chpkg
      find .chpkg -type f -name clickhouse -path '*/bin/*' -exec mv {} "$CH_BIN" \;
      rm -rf .chpkg "$tmp_asset"
      ;;
    *)
      mv "$tmp_asset" "$CH_BIN"
      ;;
  esac

  chmod +x "$CH_BIN"
  "$CH_BIN" --version | grep -q "$CH_VERSION" || {
    echo "Downloaded ClickHouse does not report version ${CH_VERSION}" >&2
    exit 1
  }
  write_cache_meta
}

select_asset
if ! cache_is_valid; then
  fetch_clickhouse
fi
CH="$CH_BIN"

CH_PID=""
cleanup() {
  if [ -n "$CH_PID" ]; then
    echo "Stopping ClickHouse server (pid $CH_PID)..."
    kill "$CH_PID" 2>/dev/null || true
    wait "$CH_PID" 2>/dev/null || true
  fi
  if [ -n "$CH_DATA_DIR" ]; then
    rm -rf "$CH_DATA_DIR"
  fi
}
trap cleanup EXIT

if curl -fsS http://localhost:8123/ping >/dev/null 2>&1; then
  echo "Port 8123 already has a ClickHouse server. Stop it before running this benchmark." >&2
  exit 1
fi

echo "Starting local ClickHouse server..."
CH_DATA_DIR=$(mktemp -d "${TMPDIR:-/tmp}/engineer-004-clickhouse.XXXXXX")
(cd "$CH_DATA_DIR" && exec "$CH" server >"$PWD/server.log" 2>&1) &
CH_PID=$!
for _ in $(seq 1 60); do
  curl -fsS http://localhost:8123/ping >/dev/null 2>&1 && break
  sleep 0.5
done
curl -fsS http://localhost:8123/ping >/dev/null 2>&1 || {
  echo "ClickHouse failed to start; see server.log" >&2; exit 1;
}

"$CH" client --multiquery <schema.sql
"$CH" client --query "TRUNCATE TABLE analytics.events"

python3 run_benchmark.py "$@"
python3 measure.py | tee results/results.md
