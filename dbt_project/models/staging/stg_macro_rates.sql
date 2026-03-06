{{
    config(
        materialized = 'view',
        schema       = 'STAGING',
        tags         = ['staging', 'macro']
    )
}}

{#
  Staging layer — RAW.MACRO_CPI + RAW.MACRO_RATES → STAGING.STG_MACRO_RATES

  Unions CPI and interest rate series into a single tidy macro table.
  Adds a `rate_type` discriminator column: 'cpi_index' | 'policy_rate_pct'
#}

-- TODO (Phase 4): implement staging SQL
select
    period::date                as period,
    value::number(12, 4)        as value,
    'cpi_index'::varchar(20)    as rate_type,
    'SSB'::varchar(20)          as source,
    current_timestamp()         as _loaded_at
from {{ source('raw', 'macro_cpi') }}
where period is not null

union all

select
    period::date                as period,
    value::number(12, 4)        as value,
    'policy_rate_pct'::varchar(20) as rate_type,
    'NORGES_BANK'::varchar(20)  as source,
    current_timestamp()         as _loaded_at
from {{ source('raw', 'macro_rates') }}
where period is not null
