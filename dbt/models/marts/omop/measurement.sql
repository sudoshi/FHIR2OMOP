{{ config(
    materialized='incremental',
    alias='measurement',
    unique_key='measurement_id',
    on_schema_change='append_new_columns',
    partition_by={'field':'measurement_date','data_type':'date'},
    cluster_by=['person_id','measurement_concept_id']
) }}

-- The single most important model in this project.
-- Every column below corresponds to a row in the mapping table in §4
-- of the research brief. Keep them in the same order.
--
-- Joining strategy:
--   1) LOINC direct lookup via int_loinc_lookup (standard + mapped).
--   2) Fall back to seed_test_source_to_concept.csv for any Roche-proprietary
--      test codes that aren't in LOINC at all.
--   3) UCUM direct lookup via int_ucum_lookup.
--   4) Fall back to seed_unit_source_to_concept.csv for non-UCUM unit strings
--      (e.g. "x10^9/L", "mg/dl" lowercase, Spanish unit strings from the LIMS).
--   5) value_as_concept_id resolved from SNOMED when the FHIR value is a
--      valueCodeableConcept (Positive/Negative, organism name, etc).
--   6) operator_concept_id looked up from int_operator_lookup keyed on the
--      FHIR valueQuantity.comparator.
--   7) visit_detail_id from int_report_child_obs (DiagnosticReport grouping).
--   8) visit_occurrence_id from stg encounter ref.
--
-- Incremental strategy:
--   We filter by Observation.meta.lastUpdated (_loaded_at) and use
--   MERGE semantics keyed on measurement_id (farm_fingerprint of the
--   FHIR id — stable across runs).

with lab as (
    select * from {{ ref('stg_fhir__observation_lab') }}
    {% if is_incremental() %}
      where _loaded_at >= (
        select coalesce(max(_loaded_at), timestamp('1970-01-01'))
        from {{ this }}
      )
    {% endif %}
),

p as (
    select person_id from {{ ref('person') }}
),

loinc as (
    select concept_code, concept_id from {{ ref('int_loinc_lookup') }}
),

ucum as (
    select concept_code, concept_id from {{ ref('int_ucum_lookup') }}
),

snomed_values as (
    select concept_code, concept_id from {{ ref('int_snomed_value_lookup') }}
),

operator as (
    select comparator, concept_id from {{ ref('int_operator_lookup') }}
),

test_seed as (
    select source_code, source_code_system, target_concept_id, source_concept_id
    from {{ ref('seed_test_source_to_concept') }}
),

unit_seed as (
    select source_unit, target_concept_id from {{ ref('seed_unit_source_to_concept') }}
),

source_concept as (
    select system_uri, concept_code, concept_id
    from {{ ref('int_fhir_source_code_lookup') }}
),

visit_detail_lookup as (
    select visit_detail_id, fhir_observation_id from {{ ref('int_report_child_obs') }}
),

vo as (
    select visit_occurrence_id, visit_source_value from {{ ref('visit_occurrence') }}
),

sp as (
    select specimen_id, specimen_source_id from {{ ref('specimen') }}
),

-- Normalize the comparator string into the short code used by int_operator_lookup.
lab_normalized as (
    select
        *,
        case comparator
            when '<'  then 'lt'
            when '<=' then 'lte'
            when '>'  then 'gt'
            when '>=' then 'gte'
            when '='  then 'eq'
            else null
        end as comparator_norm
    from lab
),

joined as (
    select
        {{ hash_id('l.fhir_observation_id') }}                                              as measurement_id,
        p.person_id                                                                         as person_id,

        coalesce(
            l_loinc.concept_id,       -- primary: LOINC standard/mapped
            l_seed.target_concept_id, -- secondary: hand map in seed
            {{ var('unknown_concept_id') }}
        )                                                                                   as measurement_concept_id,

        coalesce(date(l.effective_datetime),
                 date(l.effective_period_start))                                            as measurement_date,
        coalesce(l.effective_datetime, l.effective_period_start)                            as measurement_datetime,
        cast(null as time)                                                                  as measurement_time,

        {{ var('lab_result_type_concept_id') }}                                             as measurement_type_concept_id,
        op.concept_id                                                                       as operator_concept_id,
        l.value_number                                                                      as value_as_number,
        v_snomed.concept_id                                                                 as value_as_concept_id,

        coalesce(
            u_ucum.concept_id,
            u_seed.target_concept_id,
            {{ var('unknown_concept_id') }}
        )                                                                                   as unit_concept_id,

        l.range_low                                                                         as range_low,
        l.range_high                                                                        as range_high,

        cast(null as int64)                                                                 as provider_id,
        vo.visit_occurrence_id                                                              as visit_occurrence_id,
        vdl.visit_detail_id                                                                 as visit_detail_id,

        l.source_code                                                                       as measurement_source_value,
        coalesce(
            nullif(l_seed.source_concept_id, 0),
            src.concept_id,
                 {{ var('unknown_concept_id') }})                                           as measurement_source_concept_id,
        l.unit_source                                                                       as unit_source_value,
        coalesce(
            cast(l.value_number as string),
            l.value_string,
            l.value_codeable_display
        )                                                                                   as value_source_value,

        -- Custom-but-useful: specimen_id linkage. Not in the core v5.4 spec
        -- but widely used by LIMS-heavy CDMs; remove the column if you
        -- strictly want v5.4 only.
        sp.specimen_id                                                                      as specimen_id,

        l._loaded_at                                                                        as _loaded_at
    from lab_normalized l
    join p             on p.person_id = {{ hash_id('l.patient_ref') }}
    left join loinc         l_loinc on l_loinc.concept_code = l.loinc_code
    left join test_seed     l_seed
      on l_seed.source_code = l.source_code
     and lower(l_seed.source_code_system) = lower(l.source_code_system)
    left join ucum          u_ucum  on lower(u_ucum.concept_code) = lower(l.unit_ucum_code)
    left join unit_seed     u_seed  on lower(u_seed.source_unit)  = lower(l.unit_source)
    left join snomed_values v_snomed on v_snomed.concept_code = l.value_codeable_snomed_code
    left join operator      op      on op.comparator = l.comparator_norm
    left join source_concept src
      on lower(src.system_uri) = lower(l.source_code_system)
     and src.concept_code = l.source_code
    left join visit_detail_lookup vdl on vdl.fhir_observation_id = l.fhir_observation_id
    left join vo            on vo.visit_source_value = l.encounter_ref
    left join sp            on sp.specimen_source_id = l.specimen_ref
)

select * from joined
-- Drop rows where we couldn't resolve a person; the OHDSI DQD will flag
-- them as orphans anyway and they're almost always upstream data issues.
where person_id is not null
