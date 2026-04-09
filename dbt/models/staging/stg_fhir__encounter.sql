{{ config(materialized='view') }}

with source as (
    select resource_id, raw, last_updated
    from {{ source('fhir_raw', 'Encounter') }}
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
)

select
    resource_id                                                       as fhir_encounter_id,
    {{ fhir_reference_id(json_string('raw', 'subject.reference')) }}  as patient_ref,
    {{ json_ts('raw', 'period.start') }}                              as period_start,
    {{ json_ts('raw', 'period.end') }}                                as period_end,
    {{ json_string('raw', 'class.code') }}                            as class_code,
    {{ json_string('raw', 'class.system') }}                          as class_system,
    {{ json_string('raw', 'status') }}                                as status,
    {{ fhir_reference_id(json_string('raw', 'serviceProvider.reference')) }} as service_provider_ref,
    last_updated                                                      as _loaded_at
from latest
