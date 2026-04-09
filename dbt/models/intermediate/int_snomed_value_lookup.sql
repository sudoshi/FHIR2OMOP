{{ config(materialized='table') }}

-- SNOMED values for MEASUREMENT.value_as_concept_id (e.g. Positive,
-- Negative, organism identifications). Only standard concepts.

select
    concept_code,
    concept_id,
    concept_name
from {{ source('omop_vocab', 'concept') }}
where vocabulary_id = 'SNOMED'
  and standard_concept = 'S'
  and invalid_reason is null
