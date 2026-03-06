{% macro generate_surrogate_key(field_list) %}
    {{ dbt_utils.generate_surrogate_key(field_list) }}
{% endmacro %}

{#
  Thin wrapper around dbt_utils.generate_surrogate_key so callers
  use a project-local macro name rather than referencing dbt_utils directly.

  Usage:
    {{ generate_surrogate_key(['transaction_id', 'customer_id']) }}

  Returns:
    A SHA-256 surrogate key as a hex string (Snowflake: SHA2_HEX).
#}
