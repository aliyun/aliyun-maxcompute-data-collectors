#!/usr/bin/env bash

set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
driver_version="$(tr -d '[:space:]' < "$repo_dir/VERSION")"
metabase_tag="${METABASE_VERSION:-$(tr -d '[:space:]' < "$repo_dir/.metabase-version")}"
metabase_version="${metabase_tag#v}"
dist_dir="${DIST_DIR:-$repo_dir/dist}"
cache_root="${MAXCOMPUTE_DRIVER_CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/maxcompute-metabase-driver}"
metabase_repository="${METABASE_REPOSITORY_URL:-https://github.com/metabase/metabase.git}"
jdbc_version="$(
  sed -n 's/.*:mvn\/version "\([^"]*\)".*/\1/p' "$repo_dir/deps.edn"
)"

artifact_name="maxcompute-metabase-driver-${driver_version}.jar"
artifact_path="$dist_dir/$artifact_name"
raw_dir="$repo_dir/target/metabase-${metabase_version}"
raw_jar="$raw_dir/maxcompute.metabase-driver.jar"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

command -v git >/dev/null || fail "git is required"
command -v clojure >/dev/null || fail "Clojure CLI is required"
command -v java >/dev/null || fail "Java is required"
command -v jar >/dev/null || fail "the JDK jar command is required"
command -v python3 >/dev/null || fail "Python 3 is required for SBOM generation"

java_major="$(
  java -version 2>&1 |
    awk -F[.\"] '/version/ { if ($2 == "1") print $3; else print $2; exit }'
)"
[[ "$java_major" =~ ^[0-9]+$ ]] || fail "could not determine Java version"
(( java_major >= 21 )) ||
  fail "Java 21 or newer is required; found Java $java_major"

"$repo_dir/scripts/check-repository.sh"

if [[ -n "${METABASE_SOURCE_DIR:-}" ]]; then
  metabase_dir="$(cd "$METABASE_SOURCE_DIR" && pwd)"
  [[ -f "$metabase_dir/deps.edn" ]] ||
    fail "METABASE_SOURCE_DIR is not a Metabase checkout: $metabase_dir"
else
  metabase_dir="$cache_root/$metabase_tag"
  if [[ ! -d "$metabase_dir/.git" ]]; then
    mkdir -p "$cache_root"
    clone_args=(--depth 1 --branch "$metabase_tag")
    if [[ "${METABASE_PARTIAL_CLONE:-false}" == "true" ]]; then
      clone_args+=(--filter=blob:none)
    fi
    git clone "${clone_args[@]}" "$metabase_repository" "$metabase_dir"
  fi

  checked_out_tag="$(git -C "$metabase_dir" describe --tags --exact-match 2>/dev/null || true)"
  if [[ "$checked_out_tag" != "$metabase_tag" ]]; then
    fail "cached Metabase checkout is at ${checked_out_tag:-an untagged commit}, expected $metabase_tag"
  fi
fi

mkdir -p "$dist_dir" "$raw_dir"

echo "Building MaxCompute driver $driver_version against Metabase $metabase_tag"
(
  cd "$metabase_dir"
  external_deps="$(
    printf '{:paths ["%s" "%s" "%s" "%s"] :deps {com.aliyun.odps/odps-jdbc {:mvn/version "%s"}}}' \
      "$metabase_dir/src" "$metabase_dir/resources" \
      "$repo_dir/src" "$repo_dir/resources" "$jdbc_version"
  )"
  clojure -Sdeps "$external_deps" \
    -X:build build-drivers.build-driver/build-driver! \
    :driver :maxcompute \
    :edition :oss \
    :project-dir "\"$repo_dir\"" \
    :target-dir "\"$raw_dir\""
)

[[ -f "$raw_jar" ]] || fail "Metabase build did not produce $raw_jar"
cp "$raw_jar" "$artifact_path"

"$repo_dir/scripts/verify-artifact.sh" "$artifact_path"

sha256="$(
  if command -v sha256sum >/dev/null; then
    sha256sum "$artifact_path" | awk '{print $1}'
  else
    shasum -a 256 "$artifact_path" | awk '{print $1}'
  fi
)"
printf '%s  %s\n' "$sha256" "$artifact_name" > "$dist_dir/SHA256SUMS"

sbom_path="$dist_dir/${artifact_name%.jar}.cdx.json"
python3 "$repo_dir/scripts/generate-sbom.py" \
  "$artifact_path" \
  "$sbom_path" \
  "$driver_version"

metabase_revision="$(git -C "$metabase_dir" rev-parse HEAD)"
driver_revision="$(git -C "$repo_dir" rev-parse HEAD 2>/dev/null || printf 'uncommitted')"
if ! git -C "$repo_dir" diff --quiet --ignore-submodules -- 2>/dev/null ||
   ! git -C "$repo_dir" diff --cached --quiet --ignore-submodules -- 2>/dev/null; then
  driver_revision="${driver_revision}-dirty"
fi
build_time="$(
  if [[ -n "${SOURCE_DATE_EPOCH:-}" ]]; then
    date -u -r "$SOURCE_DATE_EPOCH" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null ||
      date -u -d "@$SOURCE_DATE_EPOCH" '+%Y-%m-%dT%H:%M:%SZ'
  else
    date -u '+%Y-%m-%dT%H:%M:%SZ'
  fi
)"

cat > "$dist_dir/${artifact_name%.jar}.build-info.json" <<EOF
{
  "artifact": "$artifact_name",
  "sha256": "$sha256",
  "driver_version": "$driver_version",
  "driver_revision": "$driver_revision",
  "metabase_tag": "$metabase_tag",
  "metabase_revision": "$metabase_revision",
  "java_version": "$(java -version 2>&1 | head -1 | sed 's/"/\\"/g')",
  "built_at": "$build_time"
}
EOF

echo "Build complete:"
echo "  $artifact_path"
echo "  $dist_dir/SHA256SUMS"
echo "  $sbom_path"
echo "  $dist_dir/${artifact_name%.jar}.build-info.json"
