{{ config(materialized='view') }}

-- Observations that are not categorized as laboratory. These flow into
-- OMOP OBSERVATION (not MEASUREMENT) — e.g. vital signs, cancelled
-- tests, micro identification results that prefer the OBSERVATION domain.

with source as (
    select resource_id, raw, last_updated
    from {{ source('fhir_raw', 'Observation') }}
),

latest as (
    select * except(rn) from (
        select s.*, row_number() over (
            partition by resource_id order by last_updated desc) as rn
        from source s
    )
    where rn = 1
),

with_category as (
    select
        resource_id,
        raw,
        last_updated,
        exists(
          select 1
          from unnest(json_query_array(raw, '$.category')) cat,
               unnest(json_query_array(cat, '$.coding')) c
          where lower(safe.string(c.code)) in ('laboratory', 'lab')
        ) as is_lab
    from latest
)

select
    resource_id                                                       as fhir_observation_id,
    {{ fhir_reference_id(json_string('raw', 'subject.reference')) }}  as patient_ref,
    {{ fhir_reference_id(json_string('raw', 'encounter.reference')) }} as encounter_ref,
    {{ get_coding('raw.code', 'http://loinc.org') }}                  as loinc_code,
    {{ get_coding('raw.code', 'http://snomed.info/sct') }}            as snomed_code,
    (
      select safe.string(c.code)
      from unnest(json_query_array(raw, '$.code.coding')) c
      limit 1
    )                                                                 as source_code,
    (
      select safe.string(c.system)
      from unnest(json_query_array(raw, '$.code.coding')) c
      limit 1
    )                                                                 as source_code_system,
    (
      select safe.string(c.display)
      from unnest(json_query_array(raw, '$.code.coding')) c
      limit 1
    )                                                                 as source_code_display,
    {{ json_ts('raw', 'effectiveDateTime') }}                         as effective_datetime,
    {{ json_ts('raw', 'effectivePeriod.start') }}                     as effective_period_start,
    {{ json_number('raw', 'valueQuantity.value') }}                   as value_number,
    {{ json_string('raw', 'valueQuantity.unit') }}                    as unit_source,
    {{ json_string('raw', 'valueQuantity.code') }}                    as unit_ucum_code,
    {{ json_string('raw', 'valueString') }}                           as value_string,
    (
      select safe.string(c.display)
      from unnest(json_query_array(raw, '$.valueCodeableConcept.coding')) c
      limit 1
    )                                                                 as value_codeable_display,
    (
      select safe.string(c.code)
      from unnest(json_query_array(raw, '$.valueCodeableConcept.coding')) c
      where safe.string(c.system) = 'http://snomed.info/sct'
      limit 1
    )                                                                 as value_snomed_code,
    last_updated                                                      as _loaded_at
from with_category
where not is_lab
