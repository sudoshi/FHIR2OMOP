"""
Load an OHDSI Athena vocabulary download (CSV bundle) into BigQuery.

Usage
-----
    python vocab/load_athena_vocab.py \
        --zip ./vocabulary_download_v5.zip \
        --project chile-omop-prod \
        --dataset omop_vocab \
        --location southamerica-west1

Athena downloads are a zip of pipe-delimited CSVs, one per vocabulary
table (CONCEPT, CONCEPT_RELATIONSHIP, CONCEPT_ANCESTOR, CONCEPT_SYNONYM,
CONCEPT_CLASS, DOMAIN, DRUG_STRENGTH, RELATIONSHIP, VOCABULARY).

We create each destination table with the OMOP v5.4 vocabulary schema
and load via `bq load --source_format=CSV` using TAB as the separator
(Athena uses real tabs inside quotes, not pipes).
"""
from __future__ import annotations

import argparse
import logging
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

from google.cloud import bigquery

LOG = logging.getLogger("load_athena_vocab")

# Schemas match OMOP CDM v5.4 vocabulary tables.
# https://ohdsi.github.io/CommonDataModel/cdm54.html#standardized_vocabularies
SCHEMAS: dict[str, list[bigquery.SchemaField]] = {
    "CONCEPT": [
        bigquery.SchemaField("concept_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("concept_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("domain_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("vocabulary_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("concept_class_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("standard_concept", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("concept_code", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("valid_start_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("valid_end_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("invalid_reason", "STRING", mode="NULLABLE"),
    ],
    "CONCEPT_RELATIONSHIP": [
        bigquery.SchemaField("concept_id_1", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("concept_id_2", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("relationship_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("valid_start_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("valid_end_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("invalid_reason", "STRING", mode="NULLABLE"),
    ],
    "CONCEPT_ANCESTOR": [
        bigquery.SchemaField("ancestor_concept_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("descendant_concept_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("min_levels_of_separation", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("max_levels_of_separation", "INT64", mode="REQUIRED"),
    ],
    "CONCEPT_SYNONYM": [
        bigquery.SchemaField("concept_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("concept_synonym_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("language_concept_id", "INT64", mode="REQUIRED"),
    ],
    "CONCEPT_CLASS": [
        bigquery.SchemaField("concept_class_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("concept_class_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("concept_class_concept_id", "INT64", mode="REQUIRED"),
    ],
    "DOMAIN": [
        bigquery.SchemaField("domain_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("domain_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("domain_concept_id", "INT64", mode="REQUIRED"),
    ],
    "DRUG_STRENGTH": [
        bigquery.SchemaField("drug_concept_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ingredient_concept_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("amount_value", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("amount_unit_concept_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("numerator_value", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("numerator_unit_concept_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("denominator_value", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("denominator_unit_concept_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("box_size", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("valid_start_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("valid_end_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("invalid_reason", "STRING", mode="NULLABLE"),
    ],
    "RELATIONSHIP": [
        bigquery.SchemaField("relationship_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("relationship_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("is_hierarchical", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("defines_ancestry", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("reverse_relationship_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("relationship_concept_id", "INT64", mode="REQUIRED"),
    ],
    "VOCABULARY": [
        bigquery.SchemaField("vocabulary_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("vocabulary_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("vocabulary_reference", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("vocabulary_version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("vocabulary_concept_id", "INT64", mode="REQUIRED"),
    ],
}


@dataclass
class Args:
    zip_path: Path
    project: str
    dataset: str
    location: str


def parse_args() -> Args:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--zip", required=True, dest="zip_path", type=Path)
    p.add_argument("--project", required=True)
    p.add_argument("--dataset", default="omop_vocab")
    p.add_argument("--location", default="southamerica-west1")
    ns = p.parse_args()
    return Args(**vars(ns))


def load_one(bq: bigquery.Client, args: Args, table_name: str,
             csv_path: Path) -> int:
    table_id = f"{args.project}.{args.dataset}.{table_name.lower()}"
    schema = SCHEMAS[table_name]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter="\t",
        quote_character="",           # Athena CSVs are tab-sep without quoting
        skip_leading_rows=1,
        allow_quoted_newlines=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # Athena uses YYYYMMDD for dates
        # bq will parse ISO; we convert in-place below if needed
    )
    LOG.info("loading %s -> %s", csv_path.name, table_id)
    with csv_path.open("rb") as f:
        load = bq.load_table_from_file(
            f, table_id, job_config=job_config, location=args.location,
            rewind=True,
        )
        load.result()
    return load.output_rows or 0


def _fix_dates_in_place(csv_path: Path) -> None:
    """
    Athena CSVs use YYYYMMDD for valid_start_date / valid_end_date.
    BigQuery wants YYYY-MM-DD. Rewrite the file once on disk, streaming.
    We only touch columns known to be dates for each table (by name via
    the header row), so non-date columns are untouched.
    """
    import csv
    from io import StringIO

    tmp = csv_path.with_suffix(csv_path.suffix + ".fixed")
    with csv_path.open("r", encoding="utf-8", newline="") as src, \
         tmp.open("w", encoding="utf-8", newline="") as dst:
        reader = csv.reader(src, delimiter="\t", quoting=csv.QUOTE_NONE)
        writer = csv.writer(dst, delimiter="\t", quoting=csv.QUOTE_NONE,
                            escapechar="\\")
        header = next(reader)
        writer.writerow(header)
        date_cols = [i for i, h in enumerate(header)
                     if h.strip().lower().endswith("_date")]
        for row in reader:
            for i in date_cols:
                v = row[i]
                if len(v) == 8 and v.isdigit():
                    row[i] = f"{v[:4]}-{v[4:6]}-{v[6:8]}"
            writer.writerow(row)
    tmp.replace(csv_path)


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    if not args.zip_path.exists():
        LOG.error("zip not found: %s", args.zip_path)
        return 1

    bq = bigquery.Client(project=args.project, location=args.location)

    with tempfile.TemporaryDirectory() as td:
        workdir = Path(td)
        LOG.info("extracting %s to %s", args.zip_path, workdir)
        with zipfile.ZipFile(args.zip_path) as zf:
            zf.extractall(workdir)

        total = 0
        for table in SCHEMAS.keys():
            candidates = list(workdir.rglob(f"{table}.csv"))
            if not candidates:
                LOG.warning("skipping %s — not present in bundle", table)
                continue
            csv_path = candidates[0]
            _fix_dates_in_place(csv_path)
            total += load_one(bq, args, table, csv_path)
        LOG.info("done — loaded %d rows across %d tables",
                 total, len(SCHEMAS))
    return 0


if __name__ == "__main__":
    sys.exit(main())
