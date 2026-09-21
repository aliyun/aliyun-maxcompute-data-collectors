#!/usr/bin/env bash

set -euo pipefail

version="1.12.5.1664"
expected_sha256="77dd6868948074adcc93e83a796f8e8f15a1a92bcb1b9002d715fd2210e476f3"
prefix="${1:-}"

if [[ -z "$prefix" ]]; then
  echo "Usage: $0 <install-prefix>" >&2
  exit 2
fi

for command_name in curl install sed sha256sum tar; do
  command -v "$command_name" >/dev/null || {
    echo "ERROR: $command_name is required" >&2
    exit 1
  }
done

work_dir="$(mktemp -d)"
cleanup() {
  rm -rf "$work_dir"
}
trap cleanup EXIT

archive="clojure-tools-${version}.tar.gz"
archive_path="$work_dir/$archive"
download_url="https://download.clojure.org/install/$archive"

echo "Downloading Clojure CLI $version from download.clojure.org"
curl --fail --location --silent --show-error \
  --connect-timeout 10 \
  --max-time 300 \
  --retry 5 \
  --output "$archive_path" \
  "$download_url"

printf '%s  %s\n' "$expected_sha256" "$archive_path" | sha256sum --check
tar -xzf "$archive_path" -C "$work_dir"

lib_dir="$prefix/lib"
bin_dir="$prefix/bin"
man_dir="$prefix/share/man/man1"
clojure_lib_dir="$lib_dir/clojure"
tools_dir="$work_dir/clojure-tools"

mkdir -p "$bin_dir" "$man_dir" "$clojure_lib_dir/libexec"
install -m 0644 "$tools_dir/deps.edn" "$clojure_lib_dir/deps.edn"
install -m 0644 "$tools_dir/example-deps.edn" "$clojure_lib_dir/example-deps.edn"
install -m 0644 "$tools_dir/tools.edn" "$clojure_lib_dir/tools.edn"
install -m 0644 "$tools_dir/exec.jar" "$clojure_lib_dir/libexec/exec.jar"
install -m 0644 \
  "$tools_dir/clojure-tools-${version}.jar" \
  "$clojure_lib_dir/libexec/clojure-tools-${version}.jar"

sed -e "s@PREFIX@$clojure_lib_dir@g" \
  "$tools_dir/clojure" > "$bin_dir/clojure"
sed -e "s@BINDIR@$bin_dir@g" \
  "$tools_dir/clj" > "$bin_dir/clj"
chmod 0755 "$bin_dir/clojure" "$bin_dir/clj"

install -m 0644 "$tools_dir/clojure.1" "$man_dir/clojure.1"
install -m 0644 "$tools_dir/clj.1" "$man_dir/clj.1"

echo "Installed Clojure CLI $version to $prefix"
