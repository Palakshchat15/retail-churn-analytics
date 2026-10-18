-- Monthly x country sales trends (net revenue, orders, customers).
-- num_orders counts sale invoices only (credit notes are not orders);
-- num_customers counts customers who bought in the month.

with txn as (
    select * from {{ ref('stg_retail_transactions') }}
)

select
    {{ month_start('invoice_date') }}                    as sales_month,
    country,
    sum(case when is_sale then line_amount else 0 end)   as gross_revenue,
    sum(case when is_return then abs(line_amount) else 0 end) as returned_revenue,
    sum(case when is_sale then line_amount else -abs(line_amount) end) as net_revenue,
    {{ filtered_agg('count', 'invoice', 'is_sale', distinct=true) }} as num_orders,
    {{ filtered_agg('count', 'customer_id', 'is_sale', distinct=true) }} as num_customers,
    sum(case when is_sale then quantity else 0 end)      as items_sold
from txn
group by 1, 2
