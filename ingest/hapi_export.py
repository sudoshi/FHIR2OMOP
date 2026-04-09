"""
Kick off a HAPI FHIR $export (Bulk Data Access) job, poll until complete,
download the NDJSON files, and land them in GCS under
    gs://<landing>/YYYY-MM-DD/<Resource>.ndjson

Why this exists
---------------
HAPI FHIR implements FHIR Bulk Data Access (kickoff URL: $export).
It returns an NDJSON file per resource type. We want those files in GCS
so the downstream loader (ndjson_to_bq.py) can pull them into the
fhir_raw dataset.

HAPI can be configured to write $export output directly to S3/GCS, but
many deployments don't have that plumbed in. This script works against
a vanilla HAPI and does the download + upload itself.

Reference:
  https://hl7.org/fhir/uv/bulkdata/export/index.html
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import requests
from google.cloud import storage
from tenacity import retry, stop_after_attempt, wait_exponential

LOG = logging.getLogger("hapi_export")

# Default set of resource types we care about for a LIMS-dominant pipeline.
# Everything else is excluded to keep the export small.
DEFAULT_TYPES = [
    "Patient",
    "Encounter",
    "DiagnosticReport",
    "Observation",
    "Specimen",
    "Practitioner",
    "Organization",
    "Location",
]


@dataclass
class ExportArgs:
    hapi_base_url: str
    gcs_landing: str  # gs://bucket/prefix (no trailing slash)
    run_date: str     # YYYY-MM-DD
    resource_types: list[str]
    since: str | None
    poll_interval_s: int
    poll_timeout_s: int
    http_user: str | None
    http_password: str | None


def parse_args() -> ExportArgs:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--hapi-base-url", required=True,
                   help="HAPI FHIR base URL, e.g. https://hapi.internal/fhir")
    p.add_argument("--gcs-landing", required=True,
                   help="gs://bucket or gs://bucket/prefix")
    p.add_argument("--run-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--types", default=",".join(DEFAULT_TYPES),
                   help="Comma-separated FHIR resource types to export")
    p.add_argument("--since", default=None,
                   help="FHIR instant: only export resources updated since then")
    p.add_argument("--poll-interval-s", type=int, default=15)
    p.add_argument("--poll-timeout-s", type=int, default=3600)
    p.add_argument("--http-user", default=None)
    p.add_argument("--http-password", default=None)
    ns = p.parse_args()
    return ExportArgs(
        hapi_base_url=ns.hapi_base_url.rstrip("/") + "/",
        gcs_landing=ns.gcs_landing.rstrip("/"),
        run_date=ns.run_date,
        resource_types=[t.strip() for t in ns.types.split(",") if t.strip()],
        since=ns.since,
        poll_interval_s=ns.poll_interval_s,
        poll_timeout_s=ns.poll_timeout_s,
        http_user=ns.http_user,
        http_password=ns.http_password,
    )


def make_session(args: ExportArgs) -> requests.Session:
    s = requests.Session()
    if args.http_user and args.http_password:
        s.auth = (args.http_user, args.http_password)
    s.headers.update({
        "Accept": "application/fhir+json",
        "Prefer": "respond-async",
    })
    return s


@retry(stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=2, max=30))
def kickoff_export(session: requests.Session, args: ExportArgs) -> str:
    """POST $export, return the Content-Location poll URL."""
    params = {"_outputFormat": "application/fhir+ndjson"}
    if args.resource_types:
        params["_type"] = ",".join(args.resource_types)
    if args.since:
        params["_since"] = args.since

    url = urljoin(args.hapi_base_url, "$export")
    LOG.info("kickoff %s params=%s", url, params)
    r = session.get(url, params=params)
    if r.status_code != 202:
        raise RuntimeError(
            f"$export kickoff expected 202, got {r.status_code}: {r.text[:500]}")
    poll_url = r.headers.get("Content-Location")
    if not poll_url:
        raise RuntimeError("$export kickoff missing Content-Location header")
    LOG.info("poll url: %s", poll_url)
    return poll_url


def poll_until_complete(session: requests.Session, poll_url: str,
                        interval_s: int, timeout_s: int) -> dict:
    """Poll the status URL until 200 OK, returning the manifest body."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = session.get(poll_url)
        if r.status_code == 200:
            LOG.info("export complete")
            return r.json()
        if r.status_code == 202:
            pct = r.headers.get("X-Progress", "in progress")
            LOG.info("still running: %s", pct)
            time.sleep(interval_s)
            continue
        raise RuntimeError(
            f"Unexpected poll response {r.status_code}: {r.text[:500]}")
    raise TimeoutError(f"$export did not complete within {timeout_s}s")


def iter_manifest_files(manifest: dict) -> Iterable[tuple[str, str]]:
    """Yield (resource_type, file_url) tuples from a bulk data manifest."""
    for entry in manifest.get("output", []):
        yield entry["type"], entry["url"]


@retry(stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=2, max=30))
def stream_to_gcs(session: requests.Session, file_url: str,
                  bucket: storage.Bucket, blob_name: str) -> int:
    """Stream a single NDJSON file from HAPI to GCS. Returns bytes uploaded."""
    LOG.info("downloading %s -> gs://%s/%s", file_url, bucket.name, blob_name)
    # Override Accept header for binary download — the session default
    # (application/fhir+json) causes HAPI to wrap the Binary in a FHIR
    # JSON envelope instead of returning the raw NDJSON content.
    headers = {"Accept": "application/fhir+ndjson"}
    with session.get(file_url, stream=True, headers=headers) as r:
        r.raise_for_status()
        blob = bucket.blob(blob_name)
        # Chunked upload — avoids buffering the whole file in memory.
        with blob.open("wb") as out:
            total = 0
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    out.write(chunk)
                    total += len(chunk)
            return total


def parse_gcs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Not a GCS URI: {uri}")
    without = uri[len("gs://"):]
    if "/" in without:
        bucket, prefix = without.split("/", 1)
    else:
        bucket, prefix = without, ""
    return bucket, prefix.rstrip("/")


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    session = make_session(args)

    poll_url = kickoff_export(session, args)
    manifest = poll_until_complete(
        session, poll_url, args.poll_interval_s, args.poll_timeout_s)

    gcs = storage.Client()
    bucket_name, prefix = parse_gcs_uri(args.gcs_landing)
    bucket = gcs.bucket(bucket_name)

    # Counter per resource so multiple files of the same type don't overwrite.
    counts: dict[str, int] = {}
    for resource_type, file_url in iter_manifest_files(manifest):
        idx = counts.get(resource_type, 0)
        counts[resource_type] = idx + 1
        blob_name = "/".join([
            p for p in [prefix, args.run_date,
                        f"{resource_type}-{idx:04d}.ndjson"] if p
        ])
        stream_to_gcs(session, file_url, bucket, blob_name)

    LOG.info("landed files: %s", counts)
    if not counts:
        LOG.warning("manifest contained zero output files — nothing to load")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
