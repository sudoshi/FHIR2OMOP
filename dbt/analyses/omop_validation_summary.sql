-- First-run OMOP validation summary.
--
-- Compile with:
--   dbt compile --select omop_validation_summary
--
-- Then run the compiled SQL in BigQuery.

select 'person' as table_name, 'row_count' as metric_name, cast(count(*) as int64) as metric_value
from {{ ref('person') }}

union all

select 'visit_occurrence', 'row_count', cast(count(*) as int64)
from {{ ref('visit_occurrence') }}

union all

select 'visit_detail', 'row_count', cast(count(*) as int64)
from {{ ref('visit_detail') }}

union all

select 'specimen', 'row_count', cast(count(*) as int64)
from {{ ref('specimen') }}

union all

select 'measurement', 'row_count', cast(count(*) as int64)
from {{ ref('measurement') }}

union all

select 'measurement', 'unknown_concept_rows', cast(countif(measurement_concept_id = {{ var('unknown_concept_id') }}) as int64)
from {{ ref('measurement') }}

union all

select 'measurement', 'unknown_unit_rows', cast(countif(unit_concept_id = {{ var('unknown_concept_id') }}) as int64)
from {{ ref('measurement') }}

union all

select 'measurement', 'null_visit_occurrence_rows', cast(countif(visit_occurrence_id is null) as int64)
from {{ ref('measurement') }}

union all

select 'observation', 'row_count', cast(count(*) as int64)
from {{ ref('observation') }}

union all

select 'observation', 'unknown_concept_rows', cast(countif(observation_concept_id = {{ var('unknown_concept_id') }}) as int64)
from {{ ref('observation') }}

union all

select 'fact_relationship', 'row_count', cast(count(*) as int64)
from {{ ref('fact_relationship') }}

order by table_name, metric_name
