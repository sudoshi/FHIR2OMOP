"""
Load NDJSON files landed in GCS into the fhir_raw.* BigQuery tables.

Strategy
--------
Schema-on-read. For each resource type we create (or append to) a table
with a single JSON column plus bookkeeping fields. This is by design:

  - You don't have to maintain a hand-written schema for every FHIR
    resource version.
  - The dbt staging layer does the typing and flattening. That keeps
    the "schema surface" in one place (dbt models) where it can be
    reviewed and tested.
  - It interoperates cleanly with the Cloud Healthcare API route: if
    you later switch to the managed Analytics V2 streaming, you just
    re-point sources.yml at the new tables.

Table layout in fhir_raw:
  <Resource>  (e.g. Observation)
    resource_id STRING           -- FHIR logical id
    resource_type STRING
    last_updated TIMESTAMP       -- meta.lastUpdated (extracted client-side)
    raw JSON                     -- the full FHIR resource
    _ingest_run_date DATE        -- partition key
    _ingest_file_uri STRING      -- provenance

Tables are partitioned on _ingest_run_date (daily) and clustered on
resource_id so MERGE/dedupe in the dbt staging layer is cheap.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import PurePosixPath

from google.cloud import bigquery, storage

LOG = logging.getLogger("ndjson_to_bq")


@dataclass
class Args:
    project: str
    dataset: str
    location: str
    gcs_landing: str
    run_date: str


def parse_args() -> Args:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--project", required=True)
    p.add_argument("--dataset", default="fhir_raw")
    p.add_argument("--location", default="southamerica-west1")
    p.add_argument("--gcs-landing", required=True,
                   help="gs://bucket/prefix — same as hapi_export.py")
    p.add_argument("--run-date", required=True, help="YYYY-MM-DD")
    ns = p.parse_args()
    return Args(**vars(ns))


def parse_gcs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Not a GCS URI: {uri}")
    without = uri[len("gs://"):]
    if "/" in without:
        bucket, prefix = without.split("/", 1)
    else:
        bucket, prefix = without, ""
    return bucket, prefix.rstrip("/")


def ensure_table(bq: bigquery.Client, project: str, dataset: str,
                 resource_type: str) -> bigquery.Table:
    table_id = f"{project}.{dataset}.{resource_type}"
    try:
        return bq.get_table(table_id)
    except Exception:  # noqa: BLE001 - NotFound
        pass

    schema = [
        bigquery.SchemaField("resource_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("resource_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_updated", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("raw", "JSON", mode="REQUIRED"),
        bigquery.SchemaField("_ingest_run_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("_ingest_file_uri", "STRING", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="_ingest_run_date",
    )
    table.clustering_fields = ["resource_id"]
    LOG.info("creating table %s", table_id)
    return bq.create_table(table)


def rewrite_ndjson(src_blob: storage.Blob, dst_blob: storage.Blob,
                   run_date: str, source_uri: str) -> int:
    """
    Re-emit NDJSON with bookkeeping columns so a simple LOAD works.
    We stream line-by-line; HAPI NDJSON files can be large.
    """
    n = 0
    # google-cloud-storage supports file-like open() on blobs.
    with src_blob.open("r") as src, dst_blob.open("w") as dst:
        for line in src:
            line = line.strip()
            if not line:
                continue
            try:
                resource = json.loads(line)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Bad NDJSON line in {src_blob.name}: {e}")
            out = {
                "resource_id": resource.get("id", ""),
                "resource_type": resource.get("resourceType", ""),
                "last_updated": (resource.get("meta") or {}).get("lastUpdated"),
                "raw": resource,
                "_ingest_run_date": run_date,
                "_ingest_file_uri": source_uri,
            }
            dst.write(json.dumps(out, ensure_ascii=False))
            dst.write("\n")
            n += 1
    return n


def load_file(bq: bigquery.Client, project: str, dataset: str,
              resource_type: str, uri: str, location: str) -> int:
    table_id = f"{project}.{dataset}.{resource_type}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
    )
    LOG.info("bq load %s -> %s", uri, table_id)
    load = bq.load_table_from_uri(
        [uri], table_id, job_config=job_config, location=location)
    load.result()  # wait
    return load.output_rows or 0


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    bucket_name, prefix = parse_gcs_uri(args.gcs_landing)

    gcs = storage.Client(project=args.project)
    bq = bigquery.Client(project=args.project, location=args.location)
    bucket = gcs.bucket(bucket_name)

    day_prefix = "/".join(p for p in [prefix, args.run_date] if p)
    staged_prefix = "/".join(p for p in [prefix, f"_staged/{args.run_date}"] if p)

    total_rows = 0
    seen_types: set[str] = set()
    for blob in bucket.list_blobs(prefix=day_prefix + "/"):
        if not blob.name.endswith(".ndjson"):
            continue
        # filename like Observation-0000.ndjson
        stem = PurePosixPath(blob.name).name
        resource_type = stem.split("-", 1)[0]
        seen_types.add(resource_type)
        ensure_table(bq, args.project, args.dataset, resource_type)

        staged_name = f"{staged_prefix}/{stem}"
        staged_blob = bucket.blob(staged_name)
        count = rewrite_ndjson(
            src_blob=blob,
            dst_blob=staged_blob,
            run_date=args.run_date,
            source_uri=f"gs://{bucket_name}/{blob.name}",
        )
        LOG.info("rewrote %s (%d rows)", staged_name, count)

        total_rows += load_file(
            bq, args.project, args.dataset, resource_type,
            f"gs://{bucket_name}/{staged_name}", args.location,
        )

    LOG.info("loaded %d rows across %d resource types: %s",
             total_rows, len(seen_types), sorted(seen_types))
    return 0 if total_rows > 0 else 2


if __name__ == "__main__":
    sys.exit(main())
