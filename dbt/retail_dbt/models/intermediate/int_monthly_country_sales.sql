-- Monthly x country sales trends (net revenue, orders, customers).
-- num_orders counts sale invoices only (credit notes are not orders);
-- num_customers counts customers who bought in the month.

with txn as (
    select * from {{ ref('stg_retail_transactions') }}
)

select
    date_trunc('month', invoice_date)::date              as sales_month,
    country,
    sum(case when is_sale then line_amount else 0 end)   as gross_revenue,
    sum(case when is_return then abs(line_amount) else 0 end) as returned_revenue,
    sum(case when is_sale then line_amount else -abs(line_amount) end) as net_revenue,
    count(distinct invoice) filter (where is_sale)       as num_orders,
    count(distinct customer_id) filter (where is_sale)   as num_customers,
    sum(case when is_sale then quantity else 0 end)      as items_sold
from txn
group by 1, 2
