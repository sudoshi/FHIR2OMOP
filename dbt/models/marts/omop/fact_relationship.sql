{{ config(materialized='table', alias='fact_relationship') }}

-- Minimal FACT_RELATIONSHIP: link each MEASUREMENT row to its SPECIMEN.
-- Domain concept ids: 21 = Measurement, 36 = Specimen. Relationship
-- concept ids below are the generic bidirectional "has specimen" /
-- "specimen of" from the Relationship vocabulary.

with rel as (
    select
        21                           as domain_concept_id_1,
        m.measurement_id             as fact_id_1,
        36                           as domain_concept_id_2,
        m.specimen_id                as fact_id_2,
        32676                        as relationship_concept_id  -- 'Has specimen'
    from {{ ref('measurement') }} m
    where m.specimen_id is not null
)

select * from rel
union all
select
    domain_concept_id_2 as domain_concept_id_1,
    fact_id_2           as fact_id_1,
    domain_concept_id_1 as domain_concept_id_2,
    fact_id_1           as fact_id_2,
    32677               as relationship_concept_id  -- 'Specimen of'
from rel
