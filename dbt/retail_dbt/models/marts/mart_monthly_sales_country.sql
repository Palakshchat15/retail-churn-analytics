select
    sales_month,
    country,
    gross_revenue,
    returned_revenue,
    net_revenue,
    num_orders,
    num_customers,
    items_sold
from {{ ref('int_monthly_country_sales') }}
