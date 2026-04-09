{{ config(materialized='table') }}

-- Exact source-code lookup used for *_source_concept_id fields.
-- This preserves the original source concept when the code exists in Athena,
-- even if the standard concept used in the target row is reached via "Maps to".

select
    'http://loinc.org' as system_uri,
    concept_code,
    concept_id,
    vocabulary_id,
    standard_concept
from {{ source('omop_vocab', 'concept') }}
where vocabulary_id = 'LOINC'
  and invalid_reason is null

union all

select
    'http://snomed.info/sct' as system_uri,
    concept_code,
    concept_id,
    vocabulary_id,
    standard_concept
from {{ source('omop_vocab', 'concept') }}
where vocabulary_id = 'SNOMED'
  and invalid_reason is null
