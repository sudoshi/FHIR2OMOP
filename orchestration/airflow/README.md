# Airflow orchestration

## Deploying to Cloud Composer

```bash
# Set Composer env variables (one-time)
gcloud composer environments run <env> --location=southamerica-west1 \
    variables set -- gcp_project chile-omop-prod
gcloud composer environments run <env> --location=southamerica-west1 \
    variables set -- hapi_base_url https://hapi.internal/fhir
gcloud composer environments run <env> --location=southamerica-west1 \
    variables set -- gcs_landing gs://chile-omop-prod-fhir-landing

# Upload the DAG and code dependencies (dbt project, ingest scripts, quality)
gcloud composer environments storage dags import \
    --environment=<env> --location=southamerica-west1 \
    --source=orchestration/airflow/dags/fhir2omop_nightly.py

# The DAG expects fhir2omop/{dbt,ingest,quality} to be present alongside it.
gsutil -m rsync -r ./dbt     gs://<composer-bucket>/dags/fhir2omop/dbt
gsutil -m rsync -r ./ingest  gs://<composer-bucket>/dags/fhir2omop/ingest
gsutil -m rsync -r ./quality gs://<composer-bucket>/dags/fhir2omop/quality
```

## Python/R deps in Composer

Add to your Composer PYPI packages:

```
dbt-bigquery>=1.7.0
google-cloud-bigquery>=3.17.0
google-cloud-storage>=2.14.0
requests>=2.31.0
tenacity>=8.2.0
```

For DQD (R) you'll need a Cloud Run job or a custom Composer image with
R + the OHDSI `DatabaseConnector` and `DataQualityDashboard` packages.
See `quality/run_dqd.R` for the actual call.
