{{ config(materialized='table') }}

-- Map FHIR valueQuantity.comparator ('<', '<=', '>=', '>', 'ad') to
-- OMOP operator concepts (vocabulary_id = 'Meas Value Operator').
-- We pin these by name to stay resilient to minor code changes across
-- vocabulary releases.

with concepts as (
    select concept_id, concept_name, concept_code
    from {{ source('omop_vocab', 'concept') }}
    where vocabulary_id = 'Meas Value Operator'
)

select 'lt'  as comparator, concept_id from concepts where concept_name = 'Less than'
union all
select 'lte' as comparator, concept_id from concepts where concept_name = 'Less than or equal to'
union all
select 'gt'  as comparator, concept_id from concepts where concept_name = 'Greater than'
union all
select 'gte' as comparator, concept_id from concepts where concept_name = 'Greater than or equal to'
union all
select 'eq'  as comparator, concept_id from concepts where concept_name = 'Equal'
