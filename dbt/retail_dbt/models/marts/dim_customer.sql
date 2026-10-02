select
    customer_id,
    country,
    first_order_date,
    last_order_date,
    customer_tenure_days
from {{ ref('int_customer_rfm_base') }}
