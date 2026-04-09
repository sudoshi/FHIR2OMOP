"""
Nightly orchestration for HAPI FHIR → OMOP CDM on BigQuery.

Schedule: 02:15 America/Santiago (just after HAPI's daily Roche LIMS
pull finishes at ~01:00). Adjust to suit.

Tasks:
    1. hapi_export             — kick off $export, land NDJSON in GCS
    2. ndjson_to_bq             — load NDJSON into fhir_raw.*
    3. dbt_seed                 — refresh seed tables (unit/test maps)
    4. dbt_build_omop           — build all models tagged 'omop'
    5. dbt_test                 — relational tests on the CDM
    6. dqd_run                  — optional, gated on the prior 5 succeeding

Compatible with Composer 2 / Airflow 2.6+.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator  # noqa: F401
from airflow.operators.empty import EmptyOperator

# --- config ---------------------------------------------------------------
GCP_PROJECT = "{{ var.value.gcp_project }}"
GCP_REGION = "{{ var.value.gcp_region | default('southamerica-west1') }}"
HAPI_BASE_URL = "{{ var.value.hapi_base_url }}"
GCS_LANDING = "{{ var.value.gcs_landing }}"
DBT_PROJECT_DIR = "/home/airflow/gcs/dags/fhir2omop/dbt"
INGEST_DIR = "/home/airflow/gcs/dags/fhir2omop/ingest"
RUN_DATE = "{{ ds }}"

default_args = {
    "owner": "chile-omop",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
}

with DAG(
    dag_id="fhir2omop_nightly",
    description="HAPI FHIR → OMOP CDM on BigQuery",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="15 2 * * *",          # 02:15 local
    catchup=False,
    max_active_runs=1,
    tags=["omop", "fhir", "chile"],
) as dag:

    start = EmptyOperator(task_id="start")

    hapi_export = BashOperator(
        task_id="hapi_export",
        bash_command=(
            f"python {INGEST_DIR}/hapi_export.py "
            f"--hapi-base-url {HAPI_BASE_URL} "
            f"--gcs-landing {GCS_LANDING} "
            f"--run-date {RUN_DATE} "
            "--poll-interval-s 15 "
            "--poll-timeout-s 7200"
        ),
    )

    ndjson_to_bq = BashOperator(
        task_id="ndjson_to_bq",
        bash_command=(
            f"python {INGEST_DIR}/ndjson_to_bq.py "
            f"--project {GCP_PROJECT} "
            "--dataset fhir_raw "
            f"--location {GCP_REGION} "
            f"--gcs-landing {GCS_LANDING} "
            f"--run-date {RUN_DATE}"
        ),
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt seed --target prod --full-refresh"
        ),
    )

    dbt_build_omop = BashOperator(
        task_id="dbt_build_omop",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt build --target prod --select tag:staging+ tag:intermediate+ tag:omop+"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt test --target prod --select tag:omop"
        ),
    )

    dqd_run = BashOperator(
        task_id="dqd_run",
        bash_command=(
            "Rscript /home/airflow/gcs/dags/fhir2omop/quality/run_dqd.R "
            f"{GCP_PROJECT} omop_cdm omop_vocab {RUN_DATE}"
        ),
        trigger_rule="all_success",
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    (
        start
        >> hapi_export
        >> ndjson_to_bq
        >> dbt_seed
        >> dbt_build_omop
        >> dbt_test
        >> dqd_run
        >> end
    )
