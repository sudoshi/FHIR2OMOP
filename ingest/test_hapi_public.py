"""
End-to-end smoke test of the HAPI → NDJSON → (simulated) BigQuery pipeline
against the public HAPI FHIR R4 test server.

Why this exists
---------------
The real ingest pipeline (hapi_export.py → ndjson_to_bq.py) needs GCS and
BigQuery. This script exercises the same logic locally:
    1. kickoff_export + poll_until_complete against https://hapi.fhir.org/baseR4
    2. download each manifest file to a local temp dir
    3. run rewrite_ndjson (the same transform ndjson_to_bq.py would run) and
       validate the output is loadable by BigQuery

Run:
    python ingest/test_hapi_public.py

Optional flags:
    --types Patient,Observation     (default: Patient,Encounter)
    --poll-timeout 1800
    --keep                          (keep the temp dir for inspection)
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import tempfile
import time
from pathlib import Path

import requests

# Re-use the production kickoff/poll functions to make sure *those* are tested.
from hapi_export import (
    ExportArgs,
    kickoff_export,
    make_session,
    poll_until_complete,
    iter_manifest_files,
)

PUBLIC_HAPI = "https://hapi.fhir.org/baseR4"
LOG = logging.getLogger("test_hapi_public")


def download_to_local(session: requests.Session, file_url: str,
                      dst: Path) -> int:
    """
    Stream a manifest file to a local path. Mirrors stream_to_gcs but writes
    to disk. Uses the same Accept-header override that stream_to_gcs uses.
    """
    LOG.info("downloading %s -> %s", file_url, dst)
    headers = {"Accept": "application/fhir+ndjson"}
    with session.get(file_url, stream=True, headers=headers) as r:
        r.raise_for_status()
        total = 0
        with dst.open("wb") as out:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    out.write(chunk)
                    total += len(chunk)
    return total


def rewrite_ndjson_local(src: Path, dst: Path, run_date: str,
                         source_uri: str) -> int:
    """
    Local mirror of ndjson_to_bq.rewrite_ndjson — writes the BigQuery-shaped
    rows to disk so we can validate the output without touching GCS/BQ.
    """
    n = 0
    with src.open("r") as sfh, dst.open("w") as dfh:
        for line in sfh:
            line = line.strip()
            if not line:
                continue
            try:
                resource = json.loads(line)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Bad NDJSON line in {src}: {e}")
            out = {
                "resource_id": resource.get("id", ""),
                "resource_type": resource.get("resourceType", ""),
                "last_updated": (resource.get("meta") or {}).get("lastUpdated"),
                "raw": resource,
                "_ingest_run_date": run_date,
                "_ingest_file_uri": source_uri,
            }
            dfh.write(json.dumps(out, ensure_ascii=False))
            dfh.write("\n")
            n += 1
    return n


def validate_rewritten(path: Path, expected_type: str) -> dict:
    """
    Check that every row matches the BigQuery schema expectations:
      resource_id    REQUIRED STRING  (non-empty)
      resource_type  REQUIRED STRING  (matches filename)
      last_updated   NULLABLE TIMESTAMP
      raw            REQUIRED JSON    (non-empty object)
    """
    rows = 0
    empty_id = 0
    empty_type = 0
    wrong_type = 0
    empty_raw = 0
    empty_lu = 0
    with path.open() as f:
        for line in f:
            row = json.loads(line)
            rows += 1
            if not row.get("resource_id"):
                empty_id += 1
            if not row.get("resource_type"):
                empty_type += 1
            elif row["resource_type"] != expected_type:
                wrong_type += 1
            if not row.get("raw"):
                empty_raw += 1
            if not row.get("last_updated"):
                empty_lu += 1
    return {
        "rows": rows,
        "empty_id": empty_id,
        "empty_type": empty_type,
        "wrong_type": wrong_type,
        "empty_raw": empty_raw,
        "empty_last_updated": empty_lu,
    }


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    ap = argparse.ArgumentParser()
    ap.add_argument("--hapi-base-url", default=PUBLIC_HAPI)
    ap.add_argument("--types", default="Patient,Encounter")
    ap.add_argument("--poll-interval-s", type=int, default=10)
    ap.add_argument("--poll-timeout-s", type=int, default=1800)
    ap.add_argument("--keep", action="store_true",
                    help="Keep the temp directory for inspection")
    ns = ap.parse_args()

    args = ExportArgs(
        hapi_base_url=ns.hapi_base_url.rstrip("/") + "/",
        gcs_landing="gs://local-test",  # unused here
        run_date=time.strftime("%Y-%m-%d"),
        resource_types=[t.strip() for t in ns.types.split(",") if t.strip()],
        since=None,
        poll_interval_s=ns.poll_interval_s,
        poll_timeout_s=ns.poll_timeout_s,
        http_user=None,
        http_password=None,
    )

    session = make_session(args)

    # 1. kickoff
    LOG.info("=== kickoff_export ===")
    poll_url = kickoff_export(session, args)

    # 2. poll
    LOG.info("=== poll_until_complete ===")
    manifest = poll_until_complete(
        session, poll_url, args.poll_interval_s, args.poll_timeout_s,
    )

    files = list(iter_manifest_files(manifest))
    if not files:
        LOG.error("manifest empty — nothing to download")
        return 2
    LOG.info("manifest: %d files", len(files))

    # 3. download + 4. rewrite + 5. validate
    tmp = Path(tempfile.mkdtemp(prefix="fhir2omop_test_"))
    LOG.info("tempdir: %s", tmp)

    try:
        counts: dict[str, int] = {}
        totals: dict[str, int] = {}
        errors: list[str] = []

        for resource_type, file_url in files:
            idx = counts.get(resource_type, 0)
            counts[resource_type] = idx + 1
            fname = f"{resource_type}-{idx:04d}.ndjson"
            raw_path = tmp / fname
            download_to_local(session, file_url, raw_path)

            staged_path = tmp / f"_staged_{fname}"
            n = rewrite_ndjson_local(
                raw_path, staged_path, args.run_date,
                source_uri=file_url,
            )
            stats = validate_rewritten(staged_path, resource_type)
            totals[resource_type] = totals.get(resource_type, 0) + stats["rows"]

            bad = (stats["empty_id"] + stats["empty_type"]
                   + stats["wrong_type"] + stats["empty_raw"])
            status = "OK" if bad == 0 else f"FAIL ({bad} bad rows)"
            LOG.info(
                "%s: rewrote=%d empty_id=%d empty_type=%d wrong_type=%d "
                "empty_raw=%d empty_last_updated=%d -- %s",
                fname, n,
                stats["empty_id"], stats["empty_type"], stats["wrong_type"],
                stats["empty_raw"], stats["empty_last_updated"], status,
            )
            if bad > 0:
                errors.append(f"{fname}: {stats}")

        LOG.info("=== summary ===")
        for t, n in sorted(totals.items()):
            LOG.info("  %s: %d rows across %d files", t, n, counts[t])

        if errors:
            LOG.error("FAILED: %d files had schema violations", len(errors))
            for e in errors:
                LOG.error("  %s", e)
            return 1
        LOG.info("PASS")
        return 0
    finally:
        if ns.keep:
            LOG.info("tempdir kept at: %s", tmp)
        else:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
