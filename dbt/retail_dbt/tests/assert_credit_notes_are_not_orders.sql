-- Issue 2 guard: a 'C' invoice is a credit note and must never be an order,
-- and a sale order must not carry credit-note lines.
select invoice, order_type
from {{ ref('fact_orders') }}
where (invoice like 'C%' and order_type <> 'credit_note')
   or (invoice not like 'C%' and order_type <> 'sale')
