#!/usr/bin/env bash

set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
driver_version="$(tr -d '[:space:]' < "$repo_dir/VERSION")"
artifact="${DRIVER_JAR:-$repo_dir/dist/maxcompute-metabase-driver-${driver_version}.jar}"
metabase_dir="${METABASE_SOURCE_DIR:-}"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

[[ -n "$metabase_dir" ]] ||
  fail "METABASE_SOURCE_DIR must point to the exact Metabase version under test"
metabase_dir="$(cd "$metabase_dir" && pwd)"
[[ -f "$metabase_dir/deps.edn" ]] ||
  fail "METABASE_SOURCE_DIR is not a Metabase checkout: $metabase_dir"
[[ -f "$artifact" ]] || fail "driver artifact not found: $artifact"

for command_name in clojure java jar; do
  command -v "$command_name" >/dev/null || fail "$command_name is required"
done

"$repo_dir/scripts/verify-artifact.sh" "$artifact"

extra_paths="$(
  printf '{:paths ["src" "resources" "%s"]}' "$artifact"
)"

(
  cd "$metabase_dir"
  clojure -Sdeps "$extra_paths" -M -e '
    (require (quote metabase.driver.maxcompute)
             (quote metabase.driver.util))
    (assert
      (contains?
        (set (metabase.driver.util/available-drivers))
        :maxcompute))
    (println "MaxCompute plugin load smoke passed")'
)
