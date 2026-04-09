{#
  Turn a FHIR logical id (string) into a stable INT64 surrogate key
  for OMOP table PKs. FARM_FINGERPRINT is deterministic and collision
  probability is negligible at this scale.

  Use:
    {{ hash_id('observation_id') }}
    {{ hash_id("concat('Patient/', patient_id)") }}
#}
{% macro hash_id(expr) %}
  farm_fingerprint(cast({{ expr }} as string))
{% endmacro %}
