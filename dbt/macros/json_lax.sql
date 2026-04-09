{#
  Tiny helpers for reading the raw JSON column consistently across staging models.

  json_string(col, path) → STRING or NULL
  json_number(col, path) → NUMERIC or NULL
  json_bool(col, path)   → BOOL or NULL
  json_ts(col, path)     → TIMESTAMP or NULL
  json_date(col, path)   → DATE or NULL

  Backed by BigQuery's LAX_* family of JSON conversion functions. See
  https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions
  (search for "LAX_STRING", "LAX_FLOAT64", "LAX_BOOL", "LAX_INT64").

  WHY LAX and not SAFE.STRING: the strict STRING(json) extractor requires the
  underlying JSON type to literally be a JSON string, and returns an error on
  mismatches — SAFE.STRING returns NULL instead. Crucially, SAFE.STRING does
  NOT coerce across JSON types: SAFE.STRING(JSON '42') → NULL, because the
  underlying type is NUMBER. FHIR fields like valueQuantity.value (JSON number)
  and deceasedBoolean (JSON boolean) would therefore silently null out on every
  row. LAX_STRING / LAX_FLOAT64 / LAX_BOOL do coerce within sensible bounds,
  which is what we want for schema-on-read staging.
#}

{% macro json_string(col, path) %}
  lax_string({{ col }}.{{ path }})
{% endmacro %}

{% macro json_number(col, path) %}
  safe_cast(lax_float64({{ col }}.{{ path }}) as numeric)
{% endmacro %}

{% macro json_bool(col, path) %}
  lax_bool({{ col }}.{{ path }})
{% endmacro %}

{% macro json_ts(col, path) %}
  safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', lax_string({{ col }}.{{ path }}))
{% endmacro %}

{% macro json_date(col, path) %}
  safe.parse_date('%Y-%m-%d', lax_string({{ col }}.{{ path }}))
{% endmacro %}
