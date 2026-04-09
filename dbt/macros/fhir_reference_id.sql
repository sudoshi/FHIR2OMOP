{#
  FHIR references come as "Patient/123" or full URLs. This helper strips
  the resource-type prefix so you can join on bare ids.

  Use:
    {{ fhir_reference_id('subject_reference') }}  -- returns '123'
#}
{% macro fhir_reference_id(expr) %}
  regexp_extract({{ expr }}, r'(?:^|/)([^/]+)$')
{% endmacro %}
