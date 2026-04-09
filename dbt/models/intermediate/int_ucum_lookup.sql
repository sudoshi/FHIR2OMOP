{{ config(materialized='table') }}

-- UCUM unit lookup: UCUM code → concept_id.
-- UCUM in Athena is already standard, so no 'Maps to' hop needed.

select
    concept_code,
    concept_id,
    concept_name
from {{ source('omop_vocab', 'concept') }}
where vocabulary_id = 'UCUM'
  and invalid_reason is null
