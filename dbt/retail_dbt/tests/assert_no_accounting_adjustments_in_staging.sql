-- 'A' invoices ("Adjust bad debt") are ledger entries, not sales or returns.
select invoice, stock_code, line_amount
from {{ ref('stg_retail_transactions') }}
where invoice like 'A%'
