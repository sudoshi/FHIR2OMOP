-- First-run raw-layer smoke check.
--
-- Compile with:
--   dbt compile --select raw_resource_counts
--
-- Then run the compiled SQL in BigQuery.

select 'Patient' as resource_type, count(*) as row_count, max(_ingest_run_date) as latest_ingest_date
from {{ source('fhir_raw', 'Patient') }}

union all

select 'Encounter' as resource_type, count(*) as row_count, max(_ingest_run_date) as latest_ingest_date
from {{ source('fhir_raw', 'Encounter') }}

union all

select 'DiagnosticReport' as resource_type, count(*) as row_count, max(_ingest_run_date) as latest_ingest_date
from {{ source('fhir_raw', 'DiagnosticReport') }}

union all

select 'Observation' as resource_type, count(*) as row_count, max(_ingest_run_date) as latest_ingest_date
from {{ source('fhir_raw', 'Observation') }}

union all

select 'Specimen' as resource_type, count(*) as row_count, max(_ingest_run_date) as latest_ingest_date
from {{ source('fhir_raw', 'Specimen') }}

union all

select 'Practitioner' as resource_type, count(*) as row_count, max(_ingest_run_date) as latest_ingest_date
from {{ source('fhir_raw', 'Practitioner') }}

union all

select 'Organization' as resource_type, count(*) as row_count, max(_ingest_run_date) as latest_ingest_date
from {{ source('fhir_raw', 'Organization') }}

union all

select 'Location' as resource_type, count(*) as row_count, max(_ingest_run_date) as latest_ingest_date
from {{ source('fhir_raw', 'Location') }}

order by resource_type
