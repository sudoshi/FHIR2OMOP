{{ config(materialized='table') }}

-- LOINC lookup: code → standard concept_id.
-- Non-standard LOINCs get resolved via concept_relationship ('Maps to').
-- This is the §4 gotcha: ~20% of your LOINCs will need the indirect path.

with loinc_direct as (
    select
        concept_code,
        concept_id,
        concept_name,
        standard_concept
    from {{ source('omop_vocab', 'concept') }}
    where vocabulary_id = 'LOINC'
      and standard_concept = 'S'
),

loinc_mapped as (
    select
        src.concept_code,
        tgt.concept_id,
        tgt.concept_name,
        'S' as standard_concept
    from {{ source('omop_vocab', 'concept') }} src
    join {{ source('omop_vocab', 'concept_relationship') }} cr
      on cr.concept_id_1 = src.concept_id
     and cr.relationship_id = 'Maps to'
     and cr.invalid_reason is null
    join {{ source('omop_vocab', 'concept') }} tgt
      on tgt.concept_id = cr.concept_id_2
     and tgt.standard_concept = 'S'
    where src.vocabulary_id = 'LOINC'
      and (src.standard_concept is null or src.standard_concept != 'S')
)

select * from loinc_direct
union all
select * from loinc_mapped
