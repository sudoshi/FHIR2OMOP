{#
  Hash PERSON.person_source_value when running in environments that require
  pseudonymization before the value lands in OMOP. Disabled by default so
  local/dev builds stay simple.

  Required vars when enabled:
    hash_person_source_value: true
    person_source_value_pepper: "<project-wide secret pepper>"
#}
{% macro hash_mrn(expr) %}
  {% set should_hash = var('hash_person_source_value', false) %}
  {% set pepper = var('person_source_value_pepper', '') %}

  {% if should_hash and not pepper %}
    {{ exceptions.raise_compiler_error(
      "Set person_source_value_pepper when hash_person_source_value=true"
    ) }}
  {% endif %}

  {% if should_hash %}
    to_hex(sha256(concat(coalesce(cast({{ expr }} as string), ''), '{{ pepper }}')))
  {% else %}
    cast({{ expr }} as string)
  {% endif %}
{% endmacro %}
