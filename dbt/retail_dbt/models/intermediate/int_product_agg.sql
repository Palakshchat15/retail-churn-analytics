-- Product-level revenue/quantity aggregates (sales only, net of returns for revenue).
-- Description is the most common one on sale lines: return/adjustment lines carry
-- warehouse notes ("damaged", "wonky bottom/broken") that must not become the name.

with txn as (
    select * from {{ ref('stg_retail_transactions') }}
    where not is_non_merchandise
),

product_sales as (
    select
        stock_code,
        coalesce(
            mode() within group (order by description)
                filter (where is_sale and coalesce(description, '') <> ''),
            max(description)
        )                                                     as description,
        sum(case when is_sale then line_amount else 0 end)    as gross_revenue,
        sum(case when is_return then abs(line_amount) else 0 end) as returned_revenue,
        sum(case when is_sale then quantity else 0 end)       as quantity_sold,
        sum(case when is_return then abs(quantity) else 0 end) as quantity_returned,
        count(distinct case when is_sale then invoice end)    as num_orders,
        count(distinct customer_id) filter (where is_sale)    as num_distinct_customers
    from txn
    group by stock_code
)

select
    *,
    (gross_revenue - returned_revenue) as net_revenue,
    case when quantity_sold > 0
         then round(returned_revenue::numeric / nullif(gross_revenue, 0), 4)
         else 0 end as return_rate_revenue
from product_sales
