{{ config(materialized='table') }}

-- Observation id → visit_detail_id map derived by unnesting the child
-- observation references on each DiagnosticReport. This is the lookup
-- measurement.sql uses to attach visit_detail_id.

select
    {{ hash_id('dr.fhir_report_id') }} as visit_detail_id,
    child_obs_id                       as fhir_observation_id
from {{ ref('stg_fhir__diagnostic_report') }} dr,
     unnest(dr.child_observation_ids) as child_obs_id
where child_obs_id is not null
