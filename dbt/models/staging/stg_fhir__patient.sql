{{ config(materialized='view') }}

-- Flatten fhir_raw.Patient into a typed, one-row-per-patient view.
-- Handles the common Patient fields; deceased/extension handling is
-- deliberately conservative — add to the SELECT list as your HAPI
-- feed fills in more.

with source as (
    select
        resource_id,
        raw,
        last_updated,
        _ingest_run_date,
        _ingest_file_uri
    from {{ source('fhir_raw', 'Patient') }}
),

latest as (
    -- Bulk exports can contain the same resource twice across days;
    -- keep the newest by meta.lastUpdated.
    select * except(rn) from (
        select
            s.*,
            row_number() over (
                partition by resource_id
                order by last_updated desc, _ingest_run_date desc
            ) as rn
        from source s
    )
    where rn = 1
)

select
    resource_id                                                      as fhir_patient_id,
    {{ json_string('raw', 'gender') }}                               as gender,
    {{ json_date('raw', 'birthDate') }}                              as birth_date,
    {{ json_ts('raw', 'deceasedDateTime') }}                         as deceased_datetime,
    {{ json_bool('raw', 'deceasedBoolean') }}                        as deceased_bool,
    -- First identifier (MRN-ish). You may need to filter by system in real data.
    (
      select safe.string(i.value)
      from unnest(json_query_array(raw, '$.identifier')) i
      limit 1
    )                                                                as identifier_value,
    (
      select safe.string(i.system)
      from unnest(json_query_array(raw, '$.identifier')) i
      limit 1
    )                                                                as identifier_system,
    -- First address — OMOP LOCATION populates from here.
    (
      select safe.string(a.city)
      from unnest(json_query_array(raw, '$.address')) a
      limit 1
    )                                                                as address_city,
    (
      select safe.string(a.state)
      from unnest(json_query_array(raw, '$.address')) a
      limit 1
    )                                                                as address_state,
    (
      select safe.string(a.postalCode)
      from unnest(json_query_array(raw, '$.address')) a
      limit 1
    )                                                                as address_postal_code,
    (
      select safe.string(a.country)
      from unnest(json_query_array(raw, '$.address')) a
      limit 1
    )                                                                as address_country,
    -- US Core race & ethnicity extensions — harmless if absent.
    (
      select safe.string(c.code)
      from unnest(json_query_array(raw, '$.extension')) e,
           unnest(json_query_array(e, '$.extension')) ee,
           unnest(json_query_array(ee, '$.valueCoding')) c
      where safe.string(e.url) = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race'
      limit 1
    )                                                                as race_code,
    (
      select safe.string(c.code)
      from unnest(json_query_array(raw, '$.extension')) e,
           unnest(json_query_array(e, '$.extension')) ee,
           unnest(json_query_array(ee, '$.valueCoding')) c
      where safe.string(e.url) = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity'
      limit 1
    )                                                                as ethnicity_code,
    last_updated                                                     as _loaded_at
from latest
