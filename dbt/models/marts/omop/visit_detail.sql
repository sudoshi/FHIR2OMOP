{{ config(materialized='table', alias='visit_detail',
          partition_by={'field':'visit_detail_start_date','data_type':'date'},
          cluster_by=['person_id']) }}

-- Per the brief §4: DiagnosticReport → VISIT_DETAIL. Every child Observation
-- of a report inherits this visit_detail_id so researchers can group results
-- by report. The Observation → visit_detail linkage lives in
-- int_report_child_obs (intermediate) to keep this table clean OMOP.

with dr as (
    select * from {{ ref('stg_fhir__diagnostic_report') }}
),

p as (
    select person_id from {{ ref('person') }}
),

vo as (
    select visit_occurrence_id, visit_source_value from {{ ref('visit_occurrence') }}
)

select
    {{ hash_id('dr.fhir_report_id') }}                                        as visit_detail_id,
    p.person_id                                                               as person_id,
    32817                                                                     as visit_detail_concept_id, -- EHR
    date(dr.effective_datetime)                                               as visit_detail_start_date,
    dr.effective_datetime                                                     as visit_detail_start_datetime,
    date(coalesce(dr.issued_datetime, dr.effective_datetime))                 as visit_detail_end_date,
    coalesce(dr.issued_datetime, dr.effective_datetime)                       as visit_detail_end_datetime,
    32879                                                                     as visit_detail_type_concept_id, -- Lab result
    cast(null as int64)                                                       as provider_id,
    cast(null as int64)                                                       as care_site_id,
    dr.fhir_report_id                                                         as visit_detail_source_value,
    cast(null as int64)                                                       as visit_detail_source_concept_id,
    cast(null as int64)                                                       as admitted_from_concept_id,
    cast(null as string)                                                      as admitted_from_source_value,
    cast(null as int64)                                                       as discharged_to_concept_id,
    cast(null as string)                                                      as discharged_to_source_value,
    cast(null as int64)                                                       as preceding_visit_detail_id,
    cast(null as int64)                                                       as parent_visit_detail_id,
    vo.visit_occurrence_id                                                    as visit_occurrence_id
from dr
join p on p.person_id = {{ hash_id('dr.patient_ref') }}
left join vo on vo.visit_source_value = dr.encounter_ref
