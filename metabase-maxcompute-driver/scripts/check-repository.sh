#!/usr/bin/env bash

set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
version="$(tr -d '[:space:]' < "$repo_dir/VERSION")"
metabase_tag="$(tr -d '[:space:]' < "$repo_dir/.metabase-version")"
manifest_version="$(
  awk '$1 == "version:" { print $2; exit }' \
    "$repo_dir/resources/metabase-plugin.yaml"
)"
jdbc_version="$(
  sed -n 's/.*:mvn\/version "\([^"]*\)".*/\1/p' "$repo_dir/deps.edn"
)"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
  fail "VERSION must contain a semantic version"
[[ "$metabase_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(\.[0-9]+)?$ ]] ||
  fail ".metabase-version must contain an exact Metabase tag"
[[ "$manifest_version" == "$version" ]] ||
  fail "VERSION ($version) differs from plugin manifest ($manifest_version)"
[[ -n "$jdbc_version" ]] || fail "ODPS JDBC dependency is missing"

grep -Fq "driver_version: $version" "$repo_dir/compatibility.yaml" ||
  fail "compatibility.yaml does not contain driver version $version"
grep -Fq "build_baseline: ${metabase_tag#v}" "$repo_dir/compatibility.yaml" ||
  fail "compatibility.yaml build baseline differs from ${metabase_tag#v}"
grep -Fq "version: $jdbc_version" "$repo_dir/compatibility.yaml" ||
  fail "compatibility.yaml does not contain ODPS JDBC $jdbc_version"
grep -Fq "artifact: maxcompute-metabase-driver-$version.jar" \
  "$repo_dir/compatibility.yaml" ||
  fail "compatibility.yaml does not declare the release artifact"

if grep -Eq '^dependencies:' "$repo_dir/resources/metabase-plugin.yaml"; then
  fail "bundled JDBC drivers must not be declared as external plugin dependencies"
fi

if find "$repo_dir" -path "$repo_dir/.git" -prune -o \
  -path "$repo_dir/.cache" -prune -o \
  -path "$repo_dir/dist" -prune -o \
  -path "$repo_dir/target" -prune -o \
  -name '*.jar' -print | grep -q .; then
  fail "binary JARs must not be committed to the source tree"
fi

for script in "$repo_dir"/scripts/*.sh; do
  bash -n "$script"
done

python3 -m py_compile "$repo_dir/scripts/generate-sbom.py"

if git -C "$repo_dir" rev-parse --git-dir >/dev/null 2>&1; then
  git -C "$repo_dir" diff --check
fi

echo "Repository checks passed:"
echo "  driver=$version"
echo "  metabase=$metabase_tag"
echo "  odps-jdbc=$jdbc_version"
