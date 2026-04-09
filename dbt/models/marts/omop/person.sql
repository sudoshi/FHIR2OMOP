{{ config(materialized='table', alias='person', cluster_by=['person_id']) }}

-- PERSON table. person_id is a surrogate derived from the FHIR patient id.

with p as (
    select * from {{ ref('stg_fhir__patient') }}
),

loc as (
    select * from {{ ref('location') }}
),

gender as (
    select concept_id, concept_code
    from {{ source('omop_vocab', 'concept') }}
    where vocabulary_id = 'Gender'
      and standard_concept = 'S'
),

race as (
    select concept_id, concept_code
    from {{ source('omop_vocab', 'concept') }}
    where vocabulary_id = 'Race'
      and standard_concept = 'S'
),

ethnicity as (
    select concept_id, concept_code
    from {{ source('omop_vocab', 'concept') }}
    where vocabulary_id = 'Ethnicity'
      and standard_concept = 'S'
)

select
    {{ hash_id('p.fhir_patient_id') }}                        as person_id,
    coalesce(g.concept_id, {{ var('unknown_concept_id') }})   as gender_concept_id,
    extract(year  from p.birth_date)                          as year_of_birth,
    extract(month from p.birth_date)                          as month_of_birth,
    extract(day   from p.birth_date)                          as day_of_birth,
    cast(null as timestamp)                                   as birth_datetime,
    coalesce(r.concept_id, {{ var('unknown_concept_id') }})   as race_concept_id,
    coalesce(e.concept_id, {{ var('unknown_concept_id') }})   as ethnicity_concept_id,
    l.location_id                                             as location_id,
    cast(null as int64)                                       as provider_id,
    cast(null as int64)                                       as care_site_id,
    -- Keep the source-system identifier in OMOP and optionally hash it.
    -- Downstream joins use person_id (derived from the FHIR logical id), not
    -- person_source_value, so pseudonymization here is safe.
    {{ hash_mrn("coalesce(p.identifier_value, p.fhir_patient_id)") }} as person_source_value,
    p.gender                                                  as gender_source_value,
    cast(null as int64)                                       as gender_source_concept_id,
    p.race_code                                               as race_source_value,
    cast(null as int64)                                       as race_source_concept_id,
    p.ethnicity_code                                          as ethnicity_source_value,
    cast(null as int64)                                       as ethnicity_source_concept_id
from p
left join gender g on lower(g.concept_code) = lower(p.gender)
left join race r on r.concept_code = p.race_code
left join ethnicity e on e.concept_code = p.ethnicity_code
left join loc l
  on l.location_source_value = concat(
       coalesce(p.address_city,''),'|',
       coalesce(p.address_state,''),'|',
       coalesce(p.address_postal_code,''),'|',
       coalesce(p.address_country,'')
     )
