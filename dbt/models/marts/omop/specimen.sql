{{ config(materialized='table', alias='specimen',
          partition_by={'field':'specimen_date','data_type':'date'},
          cluster_by=['person_id']) }}

with s as (
    select * from {{ ref('stg_fhir__specimen') }}
),

p as (
    select person_id from {{ ref('person') }}
),

-- Map SNOMED codes to standard concept_ids. SNOMED in Athena is usually standard already.
snomed as (
    select concept_code, concept_id
    from {{ source('omop_vocab', 'concept') }}
    where vocabulary_id = 'SNOMED'
      and standard_concept = 'S'
)

select
    {{ hash_id('s.fhir_specimen_id') }}                                       as specimen_id,
    p.person_id                                                               as person_id,
    coalesce(type_c.concept_id, {{ var('unknown_concept_id') }})              as specimen_concept_id,
    581378                                                                    as specimen_type_concept_id, -- "Specimen from Measurement"
    coalesce(date(s.collection_datetime), date(s.collection_period_start))    as specimen_date,
    coalesce(s.collection_datetime, s.collection_period_start)                as specimen_datetime,
    cast(null as numeric)                                                     as quantity,
    cast(null as int64)                                                       as unit_concept_id,
    coalesce(site_c.concept_id, {{ var('unknown_concept_id') }})              as anatomic_site_concept_id,
    cast(null as int64)                                                       as disease_status_concept_id,
    s.fhir_specimen_id                                                        as specimen_source_id,
    s.type_snomed_code                                                        as specimen_source_value,
    cast(null as string)                                                      as unit_source_value,
    s.body_site_snomed_code                                                   as anatomic_site_source_value,
    cast(null as string)                                                      as disease_status_source_value
from s
join p on p.person_id = {{ hash_id('s.patient_ref') }}
left join snomed type_c on type_c.concept_code = s.type_snomed_code
left join snomed site_c on site_c.concept_code = s.body_site_snomed_code
