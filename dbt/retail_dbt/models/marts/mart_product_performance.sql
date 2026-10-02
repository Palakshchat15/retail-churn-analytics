-- Product performance mart with ABC classification by cumulative revenue
-- contribution (classic Pareto analysis):
--   A: top products contributing up to 80% of cumulative net revenue
--   B: next tier up to 95% of cumulative net revenue
--   C: remaining long-tail products

with products as (
    select * from {{ ref('int_product_agg') }}
    where net_revenue > 0
),

ranked as (
    select
        *,
        row_number() over (order by net_revenue desc) as revenue_rank,
        sum(net_revenue) over ()                       as total_net_revenue,
        sum(net_revenue) over (order by net_revenue desc
                                rows between unbounded preceding and current row) as running_revenue
    from products
),

classified as (
    select
        *,
        round(running_revenue / nullif(total_net_revenue, 0), 4) as cumulative_revenue_pct,
        case
            when running_revenue / nullif(total_net_revenue, 0) <= 0.80 then 'A'
            when running_revenue / nullif(total_net_revenue, 0) <= 0.95 then 'B'
            else 'C'
        end as abc_class
    from ranked
)

select
    stock_code,
    description,
    quantity_sold,
    quantity_returned,
    gross_revenue,
    returned_revenue,
    net_revenue,
    return_rate_revenue,
    num_orders,
    num_distinct_customers,
    revenue_rank,
    cumulative_revenue_pct,
    abc_class
from classified
