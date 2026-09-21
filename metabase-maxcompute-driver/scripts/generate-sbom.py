#!/usr/bin/env python3

"""Generate a compact CycloneDX SBOM from Maven metadata inside the driver JAR."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote


def fail(message: str) -> None:
    raise SystemExit(f"ERROR: {message}")


def parse_properties(raw: bytes) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in raw.decode("utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "!")):
            continue
        match = re.match(r"([^:=\s]+)\s*[:=]\s*(.*)", line)
        if match:
            result[match.group(1)] = match.group(2)
    return result


def timestamp() -> str:
    epoch = os.getenv("SOURCE_DATE_EPOCH")
    instant = (
        datetime.fromtimestamp(int(epoch), timezone.utc)
        if epoch
        else datetime.now(timezone.utc)
    )
    return instant.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main() -> None:
    if len(sys.argv) != 4:
        fail("usage: generate-sbom.py <driver.jar> <output.json> <driver-version>")

    jar_path = Path(sys.argv[1]).resolve()
    output_path = Path(sys.argv[2]).resolve()
    driver_version = sys.argv[3]
    if not jar_path.is_file():
        fail(f"JAR not found: {jar_path}")

    digest = hashlib.sha256(jar_path.read_bytes()).hexdigest()
    components: dict[str, dict[str, object]] = {}

    with zipfile.ZipFile(jar_path) as archive:
        for name in archive.namelist():
            if not re.fullmatch(
                r"META-INF/maven/[^/]+/[^/]+/pom\.properties", name
            ):
                continue
            properties = parse_properties(archive.read(name))
            group = properties.get("groupId")
            artifact = properties.get("artifactId")
            version = properties.get("version")
            if not all((group, artifact, version)):
                continue
            purl = (
                f"pkg:maven/{quote(group, safe='')}/"
                f"{quote(artifact, safe='')}@{quote(version, safe='')}"
            )
            components[purl] = {
                "type": "library",
                "group": group,
                "name": artifact,
                "version": version,
                "purl": purl,
                "bom-ref": purl,
            }

    root_ref = f"pkg:generic/maxcompute-metabase-driver@{driver_version}"
    bom = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "serialNumber": f"urn:uuid:{uuid.uuid5(uuid.NAMESPACE_URL, digest)}",
        "version": 1,
        "metadata": {
            "timestamp": timestamp(),
            "component": {
                "type": "application",
                "name": "maxcompute-metabase-driver",
                "version": driver_version,
                "bom-ref": root_ref,
                "supplier": {"name": "Alibaba Cloud MaxCompute"},
                "hashes": [{"alg": "SHA-256", "content": digest}],
            },
        },
        "components": [components[key] for key in sorted(components)],
        "dependencies": [
            {"ref": root_ref, "dependsOn": sorted(components)},
            *({"ref": key, "dependsOn": []} for key in sorted(components)),
        ],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(bom, indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )
    print(
        f"SBOM written: {output_path} "
        f"({len(components)} embedded Maven components)"
    )


if __name__ == "__main__":
    main()
