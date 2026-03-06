{#
  Singular test: assert no transaction has amount_nok <= 0.

  Returns rows that violate the constraint — dbt marks the test as FAILED
  if any rows are returned.

  Applied to: fct_transactions.amount_nok
#}

select
    transaction_id,
    amount_nok
from {{ ref('fct_transactions') }}
where amount_nok <= 0
