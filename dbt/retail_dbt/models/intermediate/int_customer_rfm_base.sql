-- Customer-level RFM base metrics computed relative to the dataset's max
-- observed invoice_date (this is a static historical dataset, not a live
-- feed, so "today" = the last transaction date in the data).
--
-- Recency, first/last order date and frequency use SALE orders only: a credit
-- note (return) is not a purchase and must not make a customer look recent or
-- frequent. Monetary is net (sales minus returns). Customers who only have
-- credit notes in the data (their purchases predate Dec 2009) get
-- frequency = 0 and null order dates; mart_customer_rfm excludes them.

with txn as (
    select * from {{ ref('stg_retail_transactions') }}
    where customer_id is not null
),

dataset_bounds as (
    select max(invoice_date) as max_date from txn
),

orders as (
    select * from {{ ref('int_customer_orders') }}
),

customer_agg as (
    select
        o.customer_id,
        max(o.order_date) filter (where o.is_sale_order)    as last_order_date,
        min(o.order_date) filter (where o.is_sale_order)    as first_order_date,
        count(distinct o.invoice) filter (where o.is_sale_order) as frequency,
        count(distinct o.invoice) filter (where not o.is_sale_order) as num_credit_notes,
        sum(o.net_amount)                                   as monetary,
        sum(o.gross_sale_amount)                            as gross_sales,
        sum(o.items_purchased)                              as total_items_purchased,
        sum(o.items_returned)                               as total_items_returned,
        avg(o.distinct_products) filter (where o.is_sale_order) as avg_distinct_products_per_order,
        max(o.country)                                      as country
    from orders o
    group by o.customer_id
)

select
    c.customer_id,
    c.country,
    c.first_order_date,
    c.last_order_date,
    (select max_date from dataset_bounds) as dataset_max_date,
    date_part('day', (select max_date from dataset_bounds) - c.last_order_date)::int as recency_days,
    c.frequency,
    c.num_credit_notes,
    c.monetary,
    c.gross_sales,
    c.total_items_purchased,
    c.total_items_returned,
    case when c.total_items_purchased > 0
         then round(c.total_items_returned::numeric / c.total_items_purchased, 4)
         else 0 end as return_rate,
    c.avg_distinct_products_per_order,
    date_part('day', c.last_order_date - c.first_order_date)::int as customer_tenure_days
from customer_agg c
