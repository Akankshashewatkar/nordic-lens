{{
    config(
        materialized  = 'table',
        schema        = 'MARTS',
        cluster_by    = ['DATE(transaction_date)'],
        tags          = ['marts', 'core']
    )
}}

{#
  Fact table — consumption-ready transaction grain.

  Joins enriched transactions + anomaly flags + customer segments.
  Adds surrogate keys via generate_surrogate_key macro.
#}

with transactions as (
    select * from {{ ref('int_transactions_enriched') }}
),

anomaly_flags as (
    select * from {{ ref('int_anomaly_flags') }}
),

customer_segments as (
    select * from {{ ref('int_customer_segments') }}
),

-- Resolve all joins first so surrogate key macros see unambiguous column names
joined as (
    select
        t.transaction_id,
        t.customer_id,
        t.merchant_id,
        t.merchant_name,
        t.mcc_code,
        t.mcc_category,
        t.amount_nok,
        t.currency,
        t.transaction_type,
        t.transaction_date,
        t.transaction_date::date                              as transaction_date_key,
        t.is_weekend,
        t.hour_of_day,
        t.country_code,
        t.channel,
        t.is_flagged                                          as is_simulated_fraud,
        t.is_round_amount,
        t.is_late_night,
        t.rolling_7d_avg_amount,
        t.days_since_last_txn,
        cs.rfm_segment,
        af.is_statistical_outlier,
        af.is_velocity_spike,
        af.is_large_round_amount,
        af.is_suspicious_late_night,
        af.is_unusual_geography,
        af.has_any_anomaly_flag
    from transactions t
    left join anomaly_flags    af on af.transaction_id = t.transaction_id
    left join customer_segments cs on cs.customer_id   = t.customer_id
),

final as (
    select
        {{ generate_surrogate_key(['transaction_id']) }}      as transaction_sk,
        {{ generate_surrogate_key(['customer_id']) }}         as customer_sk,
        {{ generate_surrogate_key(['merchant_id']) }}         as merchant_sk,
        *,
        current_timestamp()                                   as _loaded_at
    from joined
)

select * from final
