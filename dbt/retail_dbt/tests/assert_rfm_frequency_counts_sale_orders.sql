-- Issue 2 guard: RFM frequency = number of distinct SALE orders in fact_orders.
with sale_orders as (
    select customer_id, count(*) as n_sale_orders
    from {{ ref('fact_orders') }}
    where order_type = 'sale'
    group by customer_id
)
select r.customer_id, r.frequency, s.n_sale_orders
from {{ ref('mart_customer_rfm') }} r
left join sale_orders s using (customer_id)
where s.n_sale_orders is distinct from r.frequency
