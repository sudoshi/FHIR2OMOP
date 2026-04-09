{{ config(materialized='table', alias='location') }}

-- One row per distinct patient address. LOCATION feeds PERSON.location_id.

with distinct_addrs as (
    select distinct
        address_city,
        address_state,
        address_postal_code,
        address_country
    from {{ ref('stg_fhir__patient') }}
    where address_city is not null
       or address_state is not null
       or address_postal_code is not null
       or address_country is not null
)

select
    {{ hash_id("concat(coalesce(address_city,''),'|',coalesce(address_state,''),'|',coalesce(address_postal_code,''),'|',coalesce(address_country,''))") }} as location_id,
    cast(null as string)       as address_1,
    cast(null as string)       as address_2,
    address_city               as city,
    address_state              as state,
    address_postal_code        as zip,
    cast(null as string)       as county,
    concat(
      coalesce(address_city,''), '|',
      coalesce(address_state,''), '|',
      coalesce(address_postal_code,''), '|',
      coalesce(address_country,'')
    )                          as location_source_value,
    cast(null as int64)        as country_concept_id,
    address_country            as country_source_value,
    cast(null as float64)      as latitude,
    cast(null as float64)      as longitude
from distinct_addrs
