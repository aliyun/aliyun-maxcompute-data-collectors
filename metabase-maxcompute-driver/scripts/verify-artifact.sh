#!/usr/bin/env bash

set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
driver_version="$(tr -d '[:space:]' < "$repo_dir/VERSION")"
artifact="${1:-$repo_dir/dist/maxcompute-metabase-driver-${driver_version}.jar}"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

[[ -f "$artifact" ]] || fail "artifact not found: $artifact"
command -v jar >/dev/null || fail "the JDK jar command is required"
command -v python3 >/dev/null || fail "Python 3 is required"

entries="$(jar tf "$artifact")"
grep -Fqx 'metabase-plugin.yaml' <<<"$entries" ||
  fail "metabase-plugin.yaml is missing"
grep -Fqx 'metabase/driver/maxcompute__init.class' <<<"$entries" ||
  fail "compiled MaxCompute driver namespace is missing"
grep -Fqx 'com/aliyun/odps/jdbc/OdpsDriver.class' <<<"$entries" ||
  fail "bundled ODPS JDBC driver is missing"

if grep -Eq '^clojure/(core|spec/alpha)(__init)?\.class$|^clojure/core\.clj$' <<<"$entries"; then
  fail "Metabase-provided Clojure classes must not be bundled"
fi

manifest="$(
  python3 - "$artifact" <<'PY'
import sys
import zipfile

with zipfile.ZipFile(sys.argv[1]) as archive:
    corrupt_entry = archive.testzip()
    if corrupt_entry is not None:
        raise SystemExit(f"corrupt JAR entry: {corrupt_entry}")
    print(archive.read("metabase-plugin.yaml").decode("utf-8"), end="")
PY
)"
manifest_version="$(awk '$1 == "version:" { print $2; exit }' <<<"$manifest")"
[[ "$manifest_version" == "$driver_version" ]] ||
  fail "artifact manifest version $manifest_version differs from $driver_version"

if grep -Eq '^dependencies:' <<<"$manifest"; then
  fail "artifact incorrectly declares bundled JDBC as an external dependency"
fi

echo "Artifact verification passed: $artifact"
