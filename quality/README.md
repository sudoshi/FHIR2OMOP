# Data quality

## DataQualityDashboard (DQD)

`run_dqd.R` runs the OHDSI DQD check pack against the BigQuery CDM and
writes a JSON report under `quality/output/<run_date>/results.json`.

First-time R setup (inside a Cloud Run job image or a Composer worker):

```r
install.packages(c("DatabaseConnector", "SqlRender"))
remotes::install_github("OHDSI/DataQualityDashboard")

# Download the BigQuery JDBC driver into ~/jdbc_drivers
DatabaseConnector::downloadJdbcDrivers("bigquery", pathToDriver = "~/jdbc_drivers")
```

Invoke:

```bash
Rscript quality/run_dqd.R chile-omop-prod omop_cdm omop_vocab 2026-04-08
```

### Land the report for Looker Studio

After each nightly run, copy `results.json` into GCS so Looker Studio
(or your BI tool of choice) can pick it up:

```bash
gsutil cp quality/output/${RUN_DATE}/results.json \
  gs://chile-omop-prod-dqd-reports/${RUN_DATE}/results.json
```

Create an external BigQuery table over the GCS bucket and you've got
a tablelike view of every DQD run, keyed on date.

## Achilles (weekly)

Not scripted yet. The pattern is identical to DQD — same
`DatabaseConnector` connection, different package. See §3 "Layer 6" of
the research brief for the recommended cadence.
