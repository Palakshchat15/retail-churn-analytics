select
    stock_code,
    description,
    quantity_sold,
    net_revenue
from {{ ref('int_product_agg') }}
