#!/usr/bin/env bash

set -euo pipefail
set +x

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
metabase_tag="${METABASE_VERSION:-$(tr -d '[:space:]' < "$repo_dir/.metabase-version")}"
image="${METABASE_IMAGE:-metabase/metabase:$metabase_tag}"
container_name="maxcompute-metabase-e2e-$$"
plugins_dir="$(mktemp -d "${TMPDIR:-/tmp}/maxcompute-metabase-plugins.XXXXXX")"

cleanup() {
  set +e
  docker rm -f "$container_name" >/dev/null 2>&1
  rm -rf "$plugins_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

command -v docker >/dev/null || {
  echo "ERROR: docker is required" >&2
  exit 1
}

driver_version="$(tr -d '[:space:]' < "$repo_dir/VERSION")"
metabase_version="${metabase_tag#v}"
artifact="${DRIVER_JAR:-$repo_dir/dist/maxcompute-metabase-driver-${driver_version}.jar}"
[[ -f "$artifact" ]] || {
  echo "ERROR: build the driver first; artifact not found: $artifact" >&2
  exit 1
}
cp "$artifact" "$plugins_dir/maxcompute.metabase-driver.jar"
chmod 0777 "$plugins_dir"
chmod 0644 "$plugins_dir/maxcompute.metabase-driver.jar"

echo "Starting official Metabase image $image"
docker run -d \
  --name "$container_name" \
  -p 127.0.0.1::3000 \
  -v "$plugins_dir:/plugins" \
  -e MB_DB_TYPE=h2 \
  -e MB_DB_FILE=/tmp/metabase-e2e \
  -e MB_PLUGINS_DIR=/plugins \
  -e MB_ANON_TRACKING_ENABLED=false \
  "$image" >/dev/null

port="$(
  docker port "$container_name" 3000/tcp |
    awk -F: 'NR == 1 { print $NF }'
)"
[[ -n "$port" ]] || {
  echo "ERROR: could not determine Metabase container port" >&2
  exit 1
}

image_digest="$(
  docker image inspect "$image" --format '{{index .RepoDigests 0}}' 2>/dev/null |
    sed 's/.*@//' || true
)"

if ! METABASE_URL="http://127.0.0.1:$port" \
  E2E_JAVA_VERSION="Java 25 (official Metabase container $image)" \
  E2E_METABASE_IMAGE="$image" \
  E2E_METABASE_IMAGE_DIGEST="$image_digest" \
  "$repo_dir/scripts/run-real-e2e.sh"; then
  echo "Metabase plugin diagnostics:" >&2
  docker logs "$container_name" 2>&1 |
    grep -Ei 'plugin|driver|maxcompute' |
    tail -120 >&2 || true
  exit 1
fi
