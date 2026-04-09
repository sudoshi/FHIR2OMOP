{#
  Extract the first code from a FHIR CodeableConcept .coding[] array whose
  system matches the given URI. Returns NULL if no match.

  Example:
    {{ get_coding('raw.code', 'http://loinc.org') }}
#}
{% macro get_coding(codeable_expr, system_uri) %}
  (
    select safe.string(c.code)
    from unnest(json_query_array({{ codeable_expr }}, '$.coding')) c
    where safe.string(c.system) = '{{ system_uri }}'
    limit 1
  )
{% endmacro %}
