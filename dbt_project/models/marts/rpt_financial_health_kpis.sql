{{
    config(
        materialized = 'table',
        schema       = 'MARTS',
        tags         = ['marts', 'reporting']
    )
}}

{#
  Monthly financial health KPI report — correlated with macro indicators.

  Metrics computed per calendar month:
    - total_volume_nok           : total transaction value
    - avg_transaction_nok        : mean transaction value
    - transaction_count          : number of transactions
    - active_customer_count      : distinct customers transacting
    - fraud_rate_pct             : % of transactions marked is_simulated_fraud
    - anomaly_rate_pct           : % of transactions with has_any_anomaly_flag
    - top_merchant_category      : highest-volume MCC category
    - customer_churn_risk_count  : customers with recency_days > 90
    - norges_bank_rate           : policy rate for that month (joined from macro)
    - cpi_index                  : CPI index for that month (joined from macro)
#}

with monthly_txns as (
    select
        date_trunc('month', transaction_date)        as report_month,
        count(*)                                     as transaction_count,
        sum(amount_nok)                              as total_volume_nok,
        avg(amount_nok)                              as avg_transaction_nok,
        count(distinct customer_id)                  as active_customer_count,
        avg(is_simulated_fraud::int)    * 100        as fraud_rate_pct,
        avg(has_any_anomaly_flag::int)  * 100        as anomaly_rate_pct
    from {{ ref('fct_transactions') }}
    group by 1
),

-- Top MCC category per month by transaction count (Snowflake QUALIFY pattern)
top_category as (
    select
        date_trunc('month', transaction_date)        as report_month,
        mcc_category                                 as top_merchant_category
    from {{ ref('fct_transactions') }}
    group by 1, 2
    qualify row_number() over (
        partition by date_trunc('month', transaction_date)
        order by count(*) desc
    ) = 1
),

macro as (
    select
        date_trunc('month', period)                  as report_month,
        max(case when rate_type = 'policy_rate_pct'  then value end) as norges_bank_rate,
        max(case when rate_type = 'cpi_index'        then value end) as cpi_index
    from {{ ref('stg_macro_rates') }}
    group by 1
),

-- Point-in-time churn risk snapshot: customers with no transactions in > 90 days.
-- Cross-joined so every report month shows the current churn risk count.
churn_risk as (
    select count(*) as customer_churn_risk_count
    from {{ ref('dim_customers') }}
    where recency_days > 90
),

final as (
    select
        m.report_month,
        m.transaction_count,
        round(m.total_volume_nok, 2)                 as total_volume_nok,
        round(m.avg_transaction_nok, 2)              as avg_transaction_nok,
        m.active_customer_count,
        round(m.fraud_rate_pct, 4)                   as fraud_rate_pct,
        round(m.anomaly_rate_pct, 4)                 as anomaly_rate_pct,
        tc.top_merchant_category,
        cr.customer_churn_risk_count,
        mac.norges_bank_rate,
        mac.cpi_index,
        current_timestamp()                          as _loaded_at
    from monthly_txns m
    cross join churn_risk cr
    left join top_category  tc  on m.report_month = tc.report_month
    left join macro         mac on m.report_month = mac.report_month
)

select * from final
order by report_month
