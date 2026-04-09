-- Phase-2 coverage study — the "next concrete step (1)" from the brief §9.
--
-- Counts the distinct lab test codes currently in the raw FHIR feed and
-- joins them to the LOINC lookup so you can see which ones will be
-- unknown_concept on day one. Feed the "needs mapping" subset into
-- seed_test_source_to_concept.csv.
--
-- Run with: dbt compile --select inventory_source_codes
-- Then paste the compiled SQL into the BigQuery console.

with obs as (
    select
        loinc_code,
        source_code,
        source_code_system,
        source_code_display,
        count(*) as n_rows
    from {{ ref('stg_fhir__observation_lab') }}
    group by 1,2,3,4
),
matched as (
    select
        o.*,
        l.concept_id,
        case when l.concept_id is not null then 'LOINC'
             else 'NEEDS_MAPPING'
        end as resolution
    from obs o
    left join {{ ref('int_loinc_lookup') }} l
      on l.concept_code = o.loinc_code
)
select
    resolution,
    count(*)                       as n_distinct_codes,
    sum(n_rows)                    as n_rows,
    100.0 * sum(n_rows) / sum(sum(n_rows)) over () as pct_rows
from matched
group by resolution
order by pct_rows desc
