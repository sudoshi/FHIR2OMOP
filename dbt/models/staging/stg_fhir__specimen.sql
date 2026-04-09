{{ config(materialized='view') }}

with source as (
    select resource_id, raw, last_updated
    from {{ source('fhir_raw', 'Specimen') }}
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
    resource_id                                                                        as fhir_specimen_id,
    {{ fhir_reference_id(json_string('raw', 'subject.reference')) }}                   as patient_ref,
    {{ json_ts('raw', 'collection.collectedDateTime') }}                               as collection_datetime,
    {{ json_ts('raw', 'collection.collectedPeriod.start') }}                           as collection_period_start,
    -- Specimen.type.coding[0] — SNOMED body fluid / tissue type
    (
      select safe.string(c.code)
      from unnest(json_query_array(raw, '$.type.coding')) c
      where safe.string(c.system) = 'http://snomed.info/sct'
      limit 1
    )                                                                                  as type_snomed_code,
    (
      select safe.string(c.display)
      from unnest(json_query_array(raw, '$.type.coding')) c
      limit 1
    )                                                                                  as type_display,
    -- Collection body site
    (
      select safe.string(c.code)
      from unnest(json_query_array(raw, '$.collection.bodySite.coding')) c
      where safe.string(c.system) = 'http://snomed.info/sct'
      limit 1
    )                                                                                  as body_site_snomed_code,
    {{ json_string('raw', 'status') }}                                                 as status,
    last_updated                                                                       as _loaded_at
from latest
