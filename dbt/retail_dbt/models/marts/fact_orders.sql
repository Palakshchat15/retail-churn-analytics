-- Star-schema fact table: one row per (customer, invoice).
-- order_type = 'sale' for orders, 'credit_note' for 'C' invoices (returns).
-- Count orders with `where order_type = 'sale'`; sum net_amount over all rows
-- for net revenue (credit notes carry the negative side).
select
    md5(customer_id || '-' || invoice) as order_key,
    customer_id,
    invoice,
    order_type,
    is_sale_order,
    order_date,
    country,
    gross_sale_amount,
    return_amount,
    net_amount,
    items_purchased,
    items_returned,
    has_return,
    distinct_products
from {{ ref('int_customer_orders') }}
