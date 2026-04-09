{{ config(materialized='table', alias='visit_occurrence',
          partition_by={'field':'visit_start_date','data_type':'date'},
          cluster_by=['person_id']) }}

-- VISIT_OCCURRENCE: one row per FHIR Encounter that resolves to a known person.
-- The brief notes many LIMS feeds have no Encounter at all — in that case this
-- table is empty and MEASUREMENT.visit_occurrence_id is NULL, which is OMOP-legal.

with enc as (
    select * from {{ ref('stg_fhir__encounter') }}
),

p as (
    select person_id from {{ ref('person') }}
),

visit_type as (
    -- 32817 = "EHR" (common default); override if your source is different
    select 32817 as visit_type_concept_id
),

visit_concept as (
    select concept_id, concept_code
    from {{ source('omop_vocab', 'concept') }}
    where vocabulary_id = 'Visit'
      and standard_concept = 'S'
)

select
    {{ hash_id('enc.fhir_encounter_id') }}                                   as visit_occurrence_id,
    p.person_id                                                              as person_id,
    coalesce(vc.concept_id, {{ var('unknown_concept_id') }})                 as visit_concept_id,
    date(enc.period_start)                                                   as visit_start_date,
    enc.period_start                                                         as visit_start_datetime,
    date(coalesce(enc.period_end, enc.period_start))                         as visit_end_date,
    coalesce(enc.period_end, enc.period_start)                               as visit_end_datetime,
    vt.visit_type_concept_id                                                 as visit_type_concept_id,
    cast(null as int64)                                                      as provider_id,
    cast(null as int64)                                                      as care_site_id,
    enc.fhir_encounter_id                                                    as visit_source_value,
    cast(null as int64)                                                      as visit_source_concept_id,
    cast(null as int64)                                                      as admitted_from_concept_id,
    cast(null as string)                                                     as admitted_from_source_value,
    cast(null as int64)                                                      as discharged_to_concept_id,
    cast(null as string)                                                     as discharged_to_source_value,
    cast(null as int64)                                                      as preceding_visit_occurrence_id
from enc
join p on p.person_id = {{ hash_id('enc.patient_ref') }}
cross join visit_type vt
left join visit_concept vc on lower(vc.concept_code) = lower(enc.class_code)
