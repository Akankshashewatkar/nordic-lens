{{
    config(
        materialized = 'table',
        schema       = 'MARTS',
        tags         = ['marts', 'dimension']
    )
}}

{#
  Merchant dimension — deduplicated merchant entities enriched with MCC metadata.
#}

-- TODO (Phase 4): implement dimension SQL
with merchants as (
    select distinct
        merchant_id,
        merchant_name,
        mcc_code,
        mcc_category
    from {{ ref('stg_transactions') }}
),

enriched as (
    select
        {{ generate_surrogate_key(['merchant_id']) }}     as merchant_sk,
        merchant_id,
        merchant_name,
        mcc_code,
        mcc_category,

        -- MCC category grouping for dashboard roll-ups
        case mcc_code
            when '5411' then 'Essential Retail'
            when '5541' then 'Essential Retail'
            when '5912' then 'Essential Retail'
            when '4111' then 'Travel & Transport'
            when '4121' then 'Travel & Transport'
            when '7011' then 'Travel & Transport'
            when '5812' then 'Food & Dining'
            when '7832' then 'Entertainment'
            when '4814' then 'Utilities & Telecom'
            when '8099' then 'Healthcare'
            when '9311' then 'Government'
            else 'Other'
        end                                              as mcc_group,

        current_timestamp()                              as _loaded_at
    from merchants
)

select * from enriched
