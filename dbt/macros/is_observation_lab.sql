{#
  Returns a BOOL expression that is TRUE if the FHIR Observation row
  looks like a laboratory observation.

  Shared by stg_fhir__observation_lab and stg_fhir__observation_nonlab
  so the lab / non-lab split can't drift out of sync.

  The standard way to flag a lab Observation is:
    category[].coding[].system = 'http://terminology.hl7.org/CodeSystem/observation-category'
    category[].coding[].code   = 'laboratory'

  ...but real-world feeds are messier. The brief (§9.2) and first-light
  testing against the public HAPI server showed these variants in the
  wild:

    - coding[].code: 'laboratory' (canonical), 'lab' (short form)
    - coding[].display: 'Laboratory' / 'Lab' (case-inconsistent)
    - text: 'Laboratory' (CodeableConcept.text only, no coding[])

  We match any of the above, case-insensitive. This is intentionally
  permissive — missed lab rows silently drop into OBSERVATION instead of
  MEASUREMENT, which is harder to notice than a few extra rows landing
  in MEASUREMENT. When the Chile HAPI feed comes online, spot-check the
  split and tighten here if needed.

  Usage:
    select
      ...,
      {{ is_observation_lab('raw') }} as is_lab
    from latest
#}
{% macro is_observation_lab(raw_col) %}
  exists(
    select 1
    from unnest(json_query_array({{ raw_col }}, '$.category')) cat
    where
      exists(
        select 1
        from unnest(json_query_array(cat, '$.coding')) c
        where lower(lax_string(c.code))    in ('laboratory', 'lab')
           or lower(lax_string(c.display)) in ('laboratory', 'lab')
      )
      or lower(lax_string(cat.text)) in ('laboratory', 'lab')
  )
{% endmacro %}
