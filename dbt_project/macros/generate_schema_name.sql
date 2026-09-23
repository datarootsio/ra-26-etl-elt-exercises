{#
  Force every model into target.schema, ignoring any `+schema:` config.

  dbt's default behavior appends a custom schema as "<target_schema>_<custom>"
  (e.g. hcelik_gold). That made sense in the old shared-catalog-per-handle
  layout. Now each participant has exactly ONE grants-locked UC schema
  (ra26_elt_ex_<name>), so staging views and mart tables all need to land in
  that single schema — a second implicit schema would fail with a permission
  error, since nobody was granted anything on it.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ target.schema }}
{%- endmacro %}
