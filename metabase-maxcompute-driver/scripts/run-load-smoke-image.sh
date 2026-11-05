#!/usr/bin/env bash

# Plugin-load smoke against a published Metabase release image.
#
# scripts/run-load-smoke.sh needs an exact Metabase *source* checkout, which is
# only practical where that checkout already exists. This variant runs the same
# check against the exact Metabase *release image* instead, so the gate can be
# replayed against the artifact users actually deploy:
#
#   METABASE_IMAGE=metabase/metabase:v0.63.19 scripts/run-load-smoke-image.sh
#
# The image is pinned by version tag and reported by digest, and the driver JAR
# is reported by SHA256, so every run is attributable to one artifact.

set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
driver_version="$(tr -d '[:space:]' < "$repo_dir/VERSION")"
artifact="${DRIVER_JAR:-$repo_dir/dist/maxcompute-metabase-driver-${driver_version}.jar}"
image="${METABASE_IMAGE:-}"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

[[ -n "$image" ]] ||
  fail "METABASE_IMAGE must name an exact Metabase release image, e.g. metabase/metabase:v0.63.19"
[[ "$image" =~ :v[0-9]+\.[0-9]+(\.[0-9]+){1,2}(-[0-9A-Za-z.]+)?$ ]] ||
  fail "METABASE_IMAGE must be pinned to an exact version tag: $image"
[[ -f "$artifact" ]] || fail "driver artifact not found: $artifact"
command -v docker >/dev/null || fail "docker is required"

"$repo_dir/scripts/verify-artifact.sh" "$artifact"

docker image inspect "$image" >/dev/null 2>&1 || docker pull "$image" >&2

jar_sha256="$(sha256sum "$artifact" | awk '{print $1}')"
image_digest="$(
  docker image inspect --format '{{index .RepoDigests 0}}' "$image" 2>/dev/null ||
    echo "unavailable"
)"

check_script="$(mktemp "${TMPDIR:-/tmp}/load-smoke-XXXXXX.clj")"
trap 'rm -f "$check_script"' EXIT

cat > "$check_script" <<'CLJ'
(require (quote metabase.driver.maxcompute)
         (quote metabase.driver.util))
(assert
  (contains?
    (set (metabase.driver.util/available-drivers))
    :maxcompute))
(println "MaxCompute plugin load smoke passed")
CLJ

docker run --rm --entrypoint java \
  --volume "$artifact:/tmp/maxcompute-driver.jar:ro" \
  --volume "$check_script:/tmp/check.clj:ro" \
  "$image" \
  -cp "/app/metabase.jar:/tmp/maxcompute-driver.jar" \
  clojure.main /tmp/check.clj

echo "load-smoke-image result=pass"
echo "  image=$image"
echo "  image-digest=$image_digest"
echo "  artifact=$(basename "$artifact")"
echo "  artifact-sha256=$jar_sha256"
