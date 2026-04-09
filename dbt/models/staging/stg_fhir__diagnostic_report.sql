{{ config(materialized='view') }}

-- DiagnosticReport → intermediate view. The brief (§4) says we map the
-- DiagnosticReport to a synthesized VISIT_DETAIL so that all of its
-- child Observations share a visit_detail_id. That's what this view
-- supports: one row per report with the fields the visit_detail model
-- needs.

with source as (
    select resource_id, raw, last_updated
    from {{ source('fhir_raw', 'DiagnosticReport') }}
),

latest as (
    select * except(rn) from (
        select s.*, row_number() over (
            partition by resource_id order by last_updated desc) as rn
        from source s
    )
    where rn = 1
)

select
    resource_id                                                        as fhir_report_id,
    {{ fhir_reference_id(json_string('raw', 'subject.reference')) }}   as patient_ref,
    {{ fhir_reference_id(json_string('raw', 'encounter.reference')) }} as encounter_ref,
    coalesce(
      {{ json_ts('raw', 'effectiveDateTime') }},
      {{ json_ts('raw', 'effectivePeriod.start') }}
    )                                                                  as effective_datetime,
    {{ json_ts('raw', 'issued') }}                                     as issued_datetime,
    {{ get_coding('raw.code', 'http://loinc.org') }}                   as report_loinc_code,
    (
      select safe.string(c.display)
      from unnest(json_query_array(raw, '$.code.coding')) c
      limit 1
    )                                                                  as report_display,
    -- Result references are the child Observations. We emit them as an
    -- ARRAY so downstream models can unnest if they need to know which
    -- report each Observation belongs to. (Observation.encounter is
    -- often empty; this is the reliable linkage.)
    array(
      select {{ fhir_reference_id('safe.string(r.reference)') }}
      from unnest(json_query_array(raw, '$.result')) r
    )                                                                  as child_observation_ids,
    {{ json_string('raw', 'status') }}                                 as status,
    last_updated                                                       as _loaded_at
from latest
