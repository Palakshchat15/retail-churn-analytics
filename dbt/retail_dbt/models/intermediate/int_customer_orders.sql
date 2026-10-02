-- One row per (customer, invoice) - order-level rollup used as the base for RFM.
-- Only customers with a known customer_id can be analyzed at the customer grain
-- (guest checkouts, ~25% of rows, are excluded here but retained in product/
-- country marts which don't require customer identity).
--
-- Each invoice is either a SALE order or a CREDIT NOTE ('C' invoice). Credit
-- notes are kept (they carry the returns that make revenue net), but they are
-- not orders: order counts, AOV and RFM frequency use order_type = 'sale' only.

with txn as (
    select * from {{ ref('stg_retail_transactions') }}
    where customer_id is not null
),

orders as (
    select
        customer_id,
        invoice,
        case when bool_or(is_credit_note) then 'credit_note' else 'sale' end as order_type,
        min(invoice_date)                                      as order_date,
        max(country)                                           as country,
        sum(case when is_sale then line_amount else 0 end)     as gross_sale_amount,
        sum(case when is_return then abs(line_amount) else 0 end) as return_amount,
        sum(case when is_sale then quantity else 0 end)        as items_purchased,
        sum(case when is_return then abs(quantity) else 0 end) as items_returned,
        bool_or(is_return)                                     as has_return,
        count(distinct stock_code)                             as distinct_products
    from txn
    group by customer_id, invoice
)

select
    *,
    (order_type = 'sale')                 as is_sale_order,
    (gross_sale_amount - return_amount)   as net_amount
from orders
