{{ config(materialized='view') }}

-- Flatten fhir_raw.Observation rows whose category contains "laboratory".
-- Everything a MEASUREMENT row needs is pulled out here; the marts
-- layer joins to vocabulary and doesn't touch JSON directly.
--
-- Read the research brief §4 in parallel with this file — every column
-- below corresponds to a row in the mapping table.

with source as (
    select resource_id, raw, last_updated
    from {{ source('fhir_raw', 'Observation') }}
),

latest as (
    select * except(rn) from (
        select
            s.*,
            row_number() over (
                partition by resource_id
                order by last_updated desc
            ) as rn
        from source s
    )
    where rn = 1
),

with_category as (
    select
        resource_id,
        raw,
        last_updated,
        -- See dbt/macros/is_observation_lab.sql for the full list of
        -- variants matched (coding.code / coding.display / text, all
        -- case-insensitive). Both observation_lab and observation_nonlab
        -- must use the same macro or the split drifts.
        {{ is_observation_lab('raw') }} as is_lab
    from latest
)

select
    resource_id                                                       as fhir_observation_id,
    {{ fhir_reference_id(json_string('raw', 'subject.reference')) }}  as patient_ref,
    {{ fhir_reference_id(json_string('raw', 'encounter.reference')) }} as encounter_ref,
    {{ fhir_reference_id(json_string('raw', 'specimen.reference')) }} as specimen_ref,

    -- code / source code
    {{ get_coding('raw.code', 'http://loinc.org') }}                  as loinc_code,
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

    -- times
    {{ json_ts('raw', 'effectiveDateTime') }}                         as effective_datetime,
    {{ json_ts('raw', 'effectivePeriod.start') }}                     as effective_period_start,
    {{ json_ts('raw', 'issued') }}                                    as issued_datetime,

    -- value as number + unit (UCUM path)
    {{ json_number('raw', 'valueQuantity.value') }}                   as value_number,
    {{ json_string('raw', 'valueQuantity.comparator') }}              as comparator,
    {{ json_string('raw', 'valueQuantity.unit') }}                    as unit_source,
    {{ json_string('raw', 'valueQuantity.code') }}                    as unit_ucum_code,
    {{ json_string('raw', 'valueQuantity.system') }}                  as unit_system,

    -- categorical value (SNOMED preferred)
    (
      select safe.string(c.code)
      from unnest(json_query_array(raw, '$.valueCodeableConcept.coding')) c
      where safe.string(c.system) = 'http://snomed.info/sct'
      limit 1
    )                                                                 as value_codeable_snomed_code,
    (
      select safe.string(c.display)
      from unnest(json_query_array(raw, '$.valueCodeableConcept.coding')) c
      limit 1
    )                                                                 as value_codeable_display,
    {{ json_string('raw', 'valueString') }}                           as value_string,

    -- reference range — take the first entry; see brief §4 "Common traps".
    -- rr.low.value / rr.high.value are JSON numbers, so we need LAX_FLOAT64
    -- (not safe.string, which returns NULL on non-string JSON scalars —
    -- see dbt/macros/json_lax.sql for the full rationale).
    (
      select safe_cast(lax_float64(rr.low.value) as numeric)
      from unnest(json_query_array(raw, '$.referenceRange')) rr
      limit 1
    )                                                                 as range_low,
    (
      select safe_cast(lax_float64(rr.high.value) as numeric)
      from unnest(json_query_array(raw, '$.referenceRange')) rr
      limit 1
    )                                                                 as range_high,

    -- interpretation — kept for OBSERVATION fallback; brief says we
    -- usually drop this for numeric labs.
    (
      select safe.string(c.code)
      from unnest(json_query_array(raw, '$.interpretation')) i,
           unnest(json_query_array(i, '$.coding')) c
      limit 1
    )                                                                 as interpretation_code,

    -- performer — PROVIDER lookup later
    (
      select {{ fhir_reference_id('safe.string(p.reference)') }}
      from unnest(json_query_array(raw, '$.performer')) p
      limit 1
    )                                                                 as performer_ref,

    {{ json_string('raw', 'status') }}                                as status,
    last_updated                                                      as _loaded_at
from with_category
where is_lab
