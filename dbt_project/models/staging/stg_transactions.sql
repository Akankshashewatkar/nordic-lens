{{
    config(
        materialized = 'view',
        schema       = 'STAGING',
        tags         = ['staging', 'transactions']
    )
}}

{#
  Staging layer — RAW.TRANSACTIONS → STAGING.STG_TRANSACTIONS

  Responsibilities (light cleaning only):
    - Cast all columns to correct Snowflake types
    - Rename snake_case columns to consistent naming convention
    - Coalesce or replace NULL sentinel values
    - NO business logic — that lives in intermediate/
#}

select
    transaction_id::varchar(36)                         as transaction_id,
    customer_id::varchar(36)                            as customer_id,
    merchant_id::varchar(36)                            as merchant_id,
    merchant_name::varchar(255)                         as merchant_name,
    mcc_code::varchar(4)                                as mcc_code,
    mcc_category::varchar(100)                          as mcc_category,
    amount_nok::number(18, 2)                           as amount_nok,
    coalesce(currency, 'NOK')::varchar(3)               as currency,
    transaction_type::varchar(20)                       as transaction_type,
    transaction_date::timestamp_ntz                     as transaction_date,
    is_weekend::boolean                                 as is_weekend,
    hour_of_day::number(2, 0)                           as hour_of_day,
    is_flagged::boolean                                 as is_flagged,
    country_code::varchar(2)                            as country_code,
    channel::varchar(20)                                as channel,
    current_timestamp()                                 as _loaded_at

from {{ source('raw', 'transactions') }}
where transaction_id is not null
  and amount_nok > 0
