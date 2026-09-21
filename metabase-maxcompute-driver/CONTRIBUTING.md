# Contributing

## Development baseline

The default Metabase source tag is stored in `.metabase-version`. Keep the
driver version in `VERSION` and `resources/metabase-plugin.yaml` identical.

Build and verify before opening a pull request:

```bash
./scripts/check-repository.sh
./scripts/build-driver.sh
```

Changes that affect connection, metadata sync, SQL generation, query result
decoding, or dependencies must also pass:

```bash
./scripts/run-real-e2e.sh
```

## Compatibility claims

Do not add an exact Metabase version to `compatibility.yaml` from compile-only
evidence. The minimum acceptance suite is:

1. Plugin loads without initialization errors.
2. A real MaxCompute database can be created.
3. Full schema and field sync completes.
4. `SELECT 1 AS e2e_value` completes.
5. An MBQL query against a synced real table completes.

Record sanitized results under `docs/e2e/`. Never include access keys, project
credentials, session tokens, or full server logs.

## Scope

Keep Metabase minor-line compatibility changes isolated. Do not silently widen
the supported matrix to untested minor lines.
