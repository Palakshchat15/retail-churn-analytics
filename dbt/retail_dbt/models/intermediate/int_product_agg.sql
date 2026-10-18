-- Product-level revenue/quantity aggregates (sales only, net of returns for revenue).
-- Description is the most common one on sale lines: return/adjustment lines carry
-- warehouse notes ("damaged", "wonky bottom/broken") that must not become the name.

with txn as (
    select * from {{ ref('stg_retail_transactions') }}
    where not is_non_merchandise
),
{%- if target.type == 'bigquery' %}

-- BigQuery only (Postgres uses mode() in product_sales): most common sale description
-- per product, ties broken like the Postgres en_US collation (see macros/cross_db.sql).
sale_description as (
    select stock_code, description as mode_description
    from (
        select stock_code, description, count(*) as n
        from txn
        where is_sale and coalesce(description, '') <> ''
        group by stock_code, description
    )
    qualify row_number() over (
        partition by stock_code
        order by n desc,
                 regexp_replace(lower(description), r'[^a-z0-9]', ''),
                 lower(description),
                 description
    ) = 1
),

-- Fallback for products with no sale lines: Postgres max(description) under en_US
-- (letters/digits first; for case-only ties lower case sorts first, so max picks upper
-- case, e.g. 20852 -> 'Given away'). BigQuery max() would compare raw bytes instead.
max_description as (
    select stock_code, description as max_description
    from (select distinct stock_code, description from txn where description is not null)
    qualify row_number() over (
        partition by stock_code
        order by regexp_replace(lower(description), r'[^a-z0-9]', '') desc,
                 lower(description) desc,
                 description
    ) = 1
),

txn_with_mode as (
    select t.*, d.mode_description, m.max_description
    from txn as t
    left join sale_description as d on d.stock_code = t.stock_code
    left join max_description as m on m.stock_code = t.stock_code
),
{%- endif %}

product_sales as (
    select
        stock_code,
        coalesce(
            {{ mode_where('description', "is_sale and coalesce(description, '') <> ''") }},
            {{ 'any_value(max_description)' if target.type == 'bigquery' else 'max(description)' }}
        )                                                     as description,
        sum(case when is_sale then line_amount else 0 end)    as gross_revenue,
        sum(case when is_return then abs(line_amount) else 0 end) as returned_revenue,
        sum(case when is_sale then quantity else 0 end)       as quantity_sold,
        sum(case when is_return then abs(quantity) else 0 end) as quantity_returned,
        count(distinct case when is_sale then invoice end)    as num_orders,
        {{ filtered_agg('count', 'customer_id', 'is_sale', distinct=true) }} as num_distinct_customers
    from {{ 'txn_with_mode' if target.type == 'bigquery' else 'txn' }}
    group by stock_code
)

select
    *,
    (gross_revenue - returned_revenue) as net_revenue,
    case when quantity_sold > 0
         then round({{ retail_cast('returned_revenue', 'numeric') }} / nullif(gross_revenue, 0), 4)
         else 0 end as return_rate_revenue
from product_sales
