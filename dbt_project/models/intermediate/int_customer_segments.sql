{{
    config(
        materialized = 'ephemeral',
        tags         = ['intermediate', 'customers']
    )
}}

{#
  Intermediate — RFM (Recency, Frequency, Monetary) customer segmentation.

  Segment labels assigned:
    - Champions  : high R + high F + high M
    - Loyal      : high F + high M, moderate R
    - At Risk    : previously high-value, low recent activity
    - New        : joined < 90 days ago
    - Dormant    : no transactions in 180+ days
#}

-- TODO (Phase 4): implement RFM segmentation SQL
with rfm_raw as (
    select
        customer_id,
        max(transaction_date)                                    as last_transaction_date,
        count(*)                                                 as frequency,
        sum(amount_nok)                                          as monetary_value,
        datediff('day', max(transaction_date), current_date())   as recency_days
    from {{ ref('stg_transactions') }}
    group by customer_id
),

rfm_scored as (
    select
        *,
        ntile(4) over (order by recency_days asc)      as r_score,   -- lower = more recent
        ntile(4) over (order by frequency desc)        as f_score,
        ntile(4) over (order by monetary_value desc)   as m_score
    from rfm_raw
),

segmented as (
    select
        customer_id,
        last_transaction_date,
        frequency,
        monetary_value,
        recency_days,
        r_score,
        f_score,
        m_score,
        case
            when r_score = 4 and f_score = 4 and m_score = 4           then 'Champions'
            when r_score >= 3 and f_score >= 3                         then 'Loyal'
            when r_score <= 2 and f_score >= 3                         then 'At Risk'
            when recency_days <= 90                                     then 'New'
            when recency_days >= 180                                    then 'Dormant'
            else 'Potential Loyalist'
        end                                                              as rfm_segment
    from rfm_scored
)

select * from segmented
