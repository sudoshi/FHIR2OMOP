{#
  Tiny helpers for reading the raw JSON column consistently across staging models.

  json_string(col, path) → STRING or NULL
  json_number(col, path) → NUMERIC or NULL
  json_ts(col, path)     → TIMESTAMP or NULL
#}

{% macro json_string(col, path) %}
  safe.string({{ col }}.{{ path }})
{% endmacro %}

{% macro json_number(col, path) %}
  safe_cast(safe.string({{ col }}.{{ path }}) as numeric)
{% endmacro %}

{% macro json_ts(col, path) %}
  safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', safe.string({{ col }}.{{ path }}))
{% endmacro %}

{% macro json_date(col, path) %}
  safe.parse_date('%Y-%m-%d', safe.string({{ col }}.{{ path }}))
{% endmacro %}
