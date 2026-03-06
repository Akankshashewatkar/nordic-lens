{{
    config(
        materialized = 'view',
        schema       = 'STAGING',
        tags         = ['staging', 'customers']
    )
}}

{#
  Staging layer — RAW.CUSTOMERS → STAGING.STG_CUSTOMERS

  Responsibilities:
    - Cast demographic fields to correct types
    - Normalise gender to M / F / U (unknown)
    - Trim / upper-case city names for consistency
    - NO customer segmentation — that lives in int_customer_segments
#}

select
    customer_id::varchar(36)                            as customer_id,
    first_name::varchar(100)                            as first_name,
    last_name::varchar(100)                             as last_name,
    date_of_birth::date                                 as date_of_birth,
    datediff('year', date_of_birth, current_date())     as age,
    upper(trim(gender))::varchar(1)                     as gender,
    upper(trim(city))::varchar(100)                     as city,
    postal_code::varchar(10)                            as postal_code,
    is_domestic_only::boolean                           as is_domestic_only,
    customer_since::date                                as customer_since,
    current_timestamp()                                 as _loaded_at

from {{ source('raw', 'customers') }}
where customer_id is not null
