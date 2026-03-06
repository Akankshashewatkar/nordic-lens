{% snapshot customer_snapshot %}

{{
    config(
        target_schema = 'SNAPSHOTS',
        unique_key    = 'customer_id',
        strategy      = 'check',
        check_cols    = ['city', 'postal_code', 'rfm_segment'],
    )
}}

{#
  SCD Type 2 snapshot of customer dimension.

  Tracks changes to:
    - city / postal_code   : customer relocation
    - rfm_segment          : segment transitions over time

  Generates standard dbt snapshot columns:
    dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to (NULL = current row)
#}

select
    customer_id,
    first_name,
    last_name,
    date_of_birth,
    age,
    gender,
    city,
    postal_code,
    is_domestic_only,
    customer_since,
    rfm_segment,
    total_spend_nok,
    current_timestamp() as updated_at
from {{ ref('dim_customers') }}

{% endsnapshot %}
