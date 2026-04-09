{{ config(materialized='table', alias='cdm_source') }}

-- One-row table describing this CDM instance. Required by Achilles/DQD.

select
    'Chile LIMS OMOP CDM'                      as cdm_source_name,
    'chile-lims-omop'                          as cdm_source_abbreviation,
    'Hospital Clínico de la Universidad'       as cdm_holder,
    'HAPI FHIR populated nightly from Roche LIMS (cobas/navify); FHIR→OMOP via dbt-bigquery.' as source_description,
    'Internal'                                 as source_documentation_reference,
    current_date()                             as cdm_etl_reference,
    current_date()                             as source_release_date,
    current_date()                             as cdm_release_date,
    'v5.4'                                     as cdm_version,
    cast(null as int64)                        as cdm_version_concept_id,
    (select max(vocabulary_version)
     from {{ source('omop_vocab','vocabulary') }})  as vocabulary_version
