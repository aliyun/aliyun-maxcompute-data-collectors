#!/usr/bin/env bash

set -euo pipefail
set +x

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
driver_version="$(tr -d '[:space:]' < "$repo_dir/VERSION")"
metabase_tag="${METABASE_VERSION:-$(tr -d '[:space:]' < "$repo_dir/.metabase-version")}"
metabase_version="${metabase_tag#v}"
artifact="${DRIVER_JAR:-$repo_dir/dist/maxcompute-metabase-driver-${driver_version}.jar}"
evidence_file="${E2E_EVIDENCE_FILE:-$repo_dir/dist/e2e-metabase-${metabase_version}.json}"
sync_timeout="${E2E_SYNC_TIMEOUT_SECONDS:-1800}"
external_metabase_url="${METABASE_URL:-}"
minimum_java_version="${E2E_MIN_JAVA_VERSION:-25}"
metabase_image="${E2E_METABASE_IMAGE:-}"
metabase_image_digest="${E2E_METABASE_IMAGE_DIGEST:-}"

required_environment=(
  ALIBABA_CLOUD_ACCESS_KEY_ID
  ALIBABA_CLOUD_ACCESS_KEY_SECRET
  MAXCOMPUTE_ENDPOINT
  MAXCOMPUTE_PROJECT
)

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

for name in "${required_environment[@]}"; do
  [[ -n "${!name:-}" ]] || fail "required environment variable is missing: $name"
done

for command_name in curl jq jar unzip; do
  command -v "$command_name" >/dev/null || fail "$command_name is required"
done
if [[ -z "$external_metabase_url" ]]; then
  for command_name in java python3; do
    command -v "$command_name" >/dev/null || fail "$command_name is required"
  done
fi

[[ -f "$artifact" ]] || fail "build the driver first; artifact not found: $artifact"
"$repo_dir/scripts/verify-artifact.sh" "$artifact"

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/maxcompute-metabase-e2e.XXXXXX")"
plugins_dir="$work_dir/plugins"
metabase_cache_dir="${MAXCOMPUTE_DRIVER_CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/maxcompute-metabase-driver}"
metabase_jar="${METABASE_JAR:-$metabase_cache_dir/$metabase_tag/metabase.jar}"
server_log="$work_dir/metabase.log"
cookie_jar="$work_dir/cookies"
server_pid=""
session_id=""
database_id=""
base_url="${external_metabase_url%/}"
runtime_java_version="${E2E_JAVA_VERSION:-external-runtime}"

cleanup() {
  set +e
  if [[ -n "$database_id" && -n "$session_id" ]]; then
    curl -fsS -X DELETE \
      -H "X-Metabase-Session: $session_id" \
      "$base_url/api/database/$database_id" >/dev/null
  fi
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null
    wait "$server_pid" 2>/dev/null
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "$(dirname "$evidence_file")"

if [[ -z "$external_metabase_url" ]]; then
  java_major="$(
    java -version 2>&1 |
      awk -F[.\"] '/version/ { if ($2 == "1") print $3; else print $2; exit }'
  )"
  (( java_major >= minimum_java_version )) ||
    fail "Metabase $metabase_version E2E requires Java $minimum_java_version or newer; found Java $java_major"
  runtime_java_version="$(java -version 2>&1 | head -1)"

  mkdir -p "$plugins_dir" "$(dirname "$metabase_jar")"
  cp "$artifact" "$plugins_dir/maxcompute.metabase-driver.jar"

  if [[ ! -f "$metabase_jar" ]]; then
    download_url="${METABASE_DOWNLOAD_URL:-https://downloads.metabase.com/$metabase_tag/metabase.jar}"
    echo "Downloading Metabase $metabase_tag"
    curl -fL --retry 3 --retry-delay 2 "$download_url" -o "$metabase_jar.part"
    mv "$metabase_jar.part" "$metabase_jar"
  fi

  if [[ -n "${MB_JETTY_PORT:-}" ]]; then
    port="$MB_JETTY_PORT"
  else
    port="$(
      python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
    )"
  fi

  base_url="http://127.0.0.1:$port"
  echo "Starting clean Metabase $metabase_tag on port $port"
  MB_DB_TYPE=h2 \
  MB_DB_FILE="$work_dir/metabase-app-db" \
  MB_JETTY_HOST=127.0.0.1 \
  MB_JETTY_PORT="$port" \
  MB_PLUGINS_DIR="$plugins_dir" \
  MB_ANON_TRACKING_ENABLED=false \
  java -Xmx2g -jar "$metabase_jar" >"$server_log" 2>&1 &
  server_pid=$!
else
  echo "Using clean external Metabase $metabase_tag at $base_url"
fi

healthy=false
for _ in $(seq 1 180); do
  if curl -fsS "$base_url/api/health" >/dev/null 2>&1; then
    healthy=true
    break
  fi
  if [[ -n "$server_pid" ]] && ! kill -0 "$server_pid" 2>/dev/null; then
    echo "Metabase exited before becoming healthy." >&2
    tail -80 "$server_log" >&2
    exit 1
  fi
  sleep 2
done
[[ "$healthy" == true ]] || fail "Metabase did not become healthy within 360 seconds"

properties="$(
  curl -fsS "$base_url/api/session/properties"
)"
setup_token="$(jq -er '."setup-token"' <<<"$properties")"
admin_email="maxcompute-e2e@example.invalid"
admin_password="MaxCompute-E2E-Only-42!"

setup_payload="$(
  jq -n \
    --arg token "$setup_token" \
    --arg email "$admin_email" \
    --arg password "$admin_password" \
    '{
      token: $token,
      user: {
        email: $email,
        password: $password,
        first_name: "MaxCompute",
        last_name: "E2E"
      },
      prefs: {
        site_name: "MaxCompute Driver E2E",
        site_locale: "en"
      }
    }'
)"
curl -fsS -c "$cookie_jar" \
  -H 'Content-Type: application/json' \
  -d "$setup_payload" \
  "$base_url/api/setup" >/dev/null

session_payload="$(
  jq -n --arg username "$admin_email" --arg password "$admin_password" \
    '{username: $username, password: $password}'
)"
session_id="$(
  curl -fsS \
    -H 'Content-Type: application/json' \
    -d "$session_payload" \
    "$base_url/api/session" |
    jq -er '.id'
)"

details="$(
  jq -n \
    --arg project "$MAXCOMPUTE_PROJECT" \
    --arg endpoint "$MAXCOMPUTE_ENDPOINT" \
    --arg ak "$ALIBABA_CLOUD_ACCESS_KEY_ID" \
    --arg sk "$ALIBABA_CLOUD_ACCESS_KEY_SECRET" \
    --arg quota "${MAXCOMPUTE_QUOTA_NAME:-}" \
    --arg settings "${MAXCOMPUTE_SETTINGS_JSON:-}" \
    --argjson namespace_schema "${MAXCOMPUTE_NAMESPACE_SCHEMA:-true}" \
    '{
      project: $project,
      endpoint: $endpoint,
      ak: $ak,
      "sk-value": $sk,
      "namespace-schema": $namespace_schema
    }
    + (if $quota == "" then {} else {quotaName: $quota} end)
    + (if $settings == "" then {} else {settings: $settings} end)'
)"
database_payload="$(
  jq -n \
    --argjson details "$details" \
    '{
      engine: "maxcompute",
      name: "MaxCompute Driver Real E2E",
      details: $details,
      is_full_sync: true,
      auto_run_queries: true
    }'
)"

echo "Creating a real MaxCompute database"
database_response="$work_dir/database-create.json"
database_status="$(
  curl -sS \
    -o "$database_response" \
    -w '%{http_code}' \
    -H "X-Metabase-Session: $session_id" \
    -H 'Content-Type: application/json' \
    -d "$database_payload" \
    "$base_url/api/database"
)"
if [[ ! "$database_status" =~ ^2 ]]; then
  diagnostic="$(
    jq -c '{
      message: (.message // null),
      engine_error: (.errors.engine // null),
      error_type: (."error-type" // .error_type // null),
      response_keys: (keys | sort)
    }' "$database_response" 2>/dev/null || printf '{"message":"non-JSON response"}'
  )"
  fail "database creation returned HTTP $database_status: $diagnostic"
fi
database_id="$(jq -er '.id' "$database_response")"

deadline=$((SECONDS + sync_timeout))
last_status=""
while (( SECONDS < deadline )); do
  database="$(
    curl -fsS \
      -H "X-Metabase-Session: $session_id" \
      "$base_url/api/database/$database_id"
  )"
  sync_status="$(jq -r '.initial_sync_status // "unknown"' <<<"$database")"
  if [[ "$sync_status" != "$last_status" ]]; then
    echo "Schema sync status: $sync_status"
    last_status="$sync_status"
  fi
  [[ "$sync_status" == "complete" ]] && break
  sleep 10
done
[[ "$last_status" == "complete" ]] ||
  fail "full schema sync did not complete within $sync_timeout seconds"

metadata_file="$work_dir/metadata.json"
curl -fsS \
  -H "X-Metabase-Session: $session_id" \
  "$base_url/api/database/$database_id/metadata" \
  >"$metadata_file"
table_count="$(jq '(.tables // []) | length' "$metadata_file")"
field_count="$(jq '[.tables[]?.fields[]?] | length' "$metadata_file")"
(( table_count > 0 )) || fail "schema sync returned no tables"
(( field_count > 0 )) || fail "schema sync returned no fields"
echo "Synced metadata: $table_count tables, $field_count fields"

native_payload="$(
  jq -n --argjson database "$database_id" \
    '{
      database: $database,
      type: "native",
      native: {
        query: "SELECT 1 AS e2e_value",
        "template-tags": {}
      },
      parameters: []
    }'
)"
native_result="$(
  curl -fsS \
    -H "X-Metabase-Session: $session_id" \
    -H 'Content-Type: application/json' \
    -d "$native_payload" \
    "$base_url/api/dataset"
)"
jq -e '
  .status == "completed"
  and (.data.rows | length) == 1
  and ((.data.rows[0][0] | tostring) == "1")
' <<<"$native_result" >/dev/null ||
  fail "native SELECT 1 did not return one row with value 1"
echo "Native query passed"

candidate_ids="$work_dir/candidate-table-ids"
expected_table="${MAXCOMPUTE_E2E_TABLE:-}"
jq -r --arg expected "$expected_table" '
  [
    .tables[]?
    | select((.active // true) == true)
    | select(($expected == "") or (.name == $expected))
    | select(((.fields // []) | length) > 0)
  ]
  | sort_by(-(.estimated_row_count // 0))
  | .[0:20]
  | .[].id
' "$metadata_file" >"$candidate_ids"
[[ -s "$candidate_ids" ]] ||
  fail "no synced table matched MAXCOMPUTE_E2E_TABLE=${expected_table:-<automatic>}"

mbql_passed=false
while IFS= read -r table_id; do
  mbql_payload="$(
    jq -n --argjson database "$database_id" --argjson table "$table_id" \
      '{
        database: $database,
        type: "query",
        query: {
          "source-table": $table,
          limit: 1
        },
        parameters: []
      }'
  )"
  mbql_result="$(
    curl -fsS \
      -H "X-Metabase-Session: $session_id" \
      -H 'Content-Type: application/json' \
      -d "$mbql_payload" \
      "$base_url/api/dataset"
  )"
  if jq -e '.status == "completed" and (.data.rows | length) == 1' \
    <<<"$mbql_result" >/dev/null; then
    mbql_passed=true
    break
  fi
done <"$candidate_ids"
[[ "$mbql_passed" == true ]] ||
  fail "no candidate synced table completed an MBQL query with one row"
echo "MBQL query passed"

artifact_sha256="$(
  if command -v sha256sum >/dev/null; then
    sha256sum "$artifact" | awk '{print $1}'
  else
    shasum -a 256 "$artifact" | awk '{print $1}'
  fi
)"

jq -n \
  --arg driver_version "$driver_version" \
  --arg metabase_version "$metabase_version" \
  --arg metabase_image "$metabase_image" \
  --arg metabase_image_digest "$metabase_image_digest" \
  --arg java_version "$runtime_java_version" \
  --arg artifact_sha256 "$artifact_sha256" \
  --arg tested_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
  --argjson tables "$table_count" \
  --argjson fields "$field_count" \
  '{
    driver_version: $driver_version,
    metabase_version: $metabase_version,
    metabase_image: (if $metabase_image == "" then null else $metabase_image end),
    metabase_image_digest: (if $metabase_image_digest == "" then null else $metabase_image_digest end),
    java_version: $java_version,
    artifact_sha256: $artifact_sha256,
    tested_at: $tested_at,
    real_maxcompute: true,
    plugin_load: "pass",
    database_connection: "pass",
    full_schema_sync: "pass",
    table_count: $tables,
    field_count: $fields,
    native_query: "pass",
    mbql_query: "pass"
  }' >"$evidence_file"

echo "Real E2E passed. Sanitized evidence: $evidence_file"
