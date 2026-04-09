{{ config(materialized='table', alias='observation',
          partition_by={'field':'observation_date','data_type':'date'},
          cluster_by=['person_id']) }}

-- Non-lab Observations land here. For a LIMS-dominant feed this table
-- may be small (vital signs, cancelled tests, micro-only results). The
-- shape mirrors measurement.sql without the numeric-range and operator
-- columns.

with obs as (
    select * from {{ ref('stg_fhir__observation_nonlab') }}
),

p as (
    select person_id from {{ ref('person') }}
),

loinc as (
    select concept_code, concept_id from {{ ref('int_loinc_lookup') }}
),

snomed as (
    select concept_code, concept_id from {{ ref('int_snomed_value_lookup') }}
),

ucum as (
    select concept_code, concept_id from {{ ref('int_ucum_lookup') }}
),

source_concept as (
    select system_uri, concept_code, concept_id
    from {{ ref('int_fhir_source_code_lookup') }}
),

vo as (
    select visit_occurrence_id, visit_source_value from {{ ref('visit_occurrence') }}
)

select
    {{ hash_id('o.fhir_observation_id') }}                                              as observation_id,
    p.person_id                                                                          as person_id,
    coalesce(l.concept_id, s_code.concept_id, {{ var('unknown_concept_id') }})          as observation_concept_id,
    coalesce(date(o.effective_datetime), date(o.effective_period_start))                 as observation_date,
    coalesce(o.effective_datetime, o.effective_period_start)                             as observation_datetime,
    38000280                                                                             as observation_type_concept_id, -- "Observation recorded from EHR"
    o.value_number                                                                       as value_as_number,
    coalesce(o.value_string, o.value_codeable_display)                                   as value_as_string,
    s_value.concept_id                                                                   as value_as_concept_id,
    cast(null as int64)                                                                  as qualifier_concept_id,
    coalesce(u.concept_id, {{ var('unknown_concept_id') }})                              as unit_concept_id,
    cast(null as int64)                                                                  as provider_id,
    vo.visit_occurrence_id                                                               as visit_occurrence_id,
    cast(null as int64)                                                                  as visit_detail_id,
    o.source_code                                                                        as observation_source_value,
    coalesce(src.concept_id, {{ var('unknown_concept_id') }})                            as observation_source_concept_id,
    o.unit_source                                                                        as unit_source_value,
    cast(null as string)                                                                 as qualifier_source_value,
    coalesce(cast(o.value_number as string), o.value_string, o.value_codeable_display)   as value_source_value,
    cast(null as int64)                                                                  as observation_event_id,
    cast(null as int64)                                                                  as obs_event_field_concept_id
from obs o
join p on p.person_id = {{ hash_id('o.patient_ref') }}
left join loinc l on l.concept_code = o.loinc_code
left join snomed s_code on s_code.concept_code = o.snomed_code
left join snomed s_value on s_value.concept_code = o.value_snomed_code
left join ucum u on lower(u.concept_code) = lower(o.unit_ucum_code)
left join source_concept src
  on lower(src.system_uri) = lower(o.source_code_system)
 and src.concept_code = o.source_code
left join vo on vo.visit_source_value = o.encounter_ref
