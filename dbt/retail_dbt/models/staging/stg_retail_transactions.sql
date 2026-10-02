-- Clean / standardize raw.retail_transactions and flag return vs. sale rows.
--
-- Business rule (documented in README_PIPELINE.md): a row is a RETURN if
-- Quantity < 0 OR the Invoice number starts with 'C' (UCI's own convention
-- for a credit-note/cancellation invoice). Returns are legitimate business
-- events, not data errors, and are kept (not filtered out) so that net
-- revenue and return-rate features can be computed downstream.
--
-- Rows with null invoice/stock_code/invoice_date are structurally invalid
-- and excluded here (GE also gates on this upstream).
-- Rows with price <= 0 on a SALE (non-return) are excluded as bad/
-- administrative records (e.g. "Manual", "AMAZON FEE", "POSTAGE" adjustment
-- lines with 0 price) that would otherwise distort revenue.
--
-- SHEET OVERLAP DEDUPE: the source workbook has two sheets ("Year 2009-2010",
-- "Year 2010-2011") and BOTH contain 2010-12-01..2010-12-09 (1,088 invoices,
-- 22,523 identical lines in each sheet). Each invoice is kept only from the
-- first sheet it appears in. Exact duplicate lines *within* one sheet are kept:
-- the source has no line number, so two identical lines on one invoice may be
-- the same item scanned twice, and there is no evidence they are errors.
--
-- ACCOUNTING ADJUSTMENTS: 'A' invoices ("Adjust bad debt", stock code B) are
-- ledger entries, not sales or returns, and are excluded. (Previously the one
-- positive line, +£11,062, passed as a sale while its negative reversals were
-- dropped by the price <= 0 rule.)

with source as (
    select * from {{ source('raw', 'retail_transactions') }}
),

invoice_first_sheet as (
    select upper(trim(invoice)) as invoice_key, min(source_sheet) as first_sheet
    from source
    where invoice is not null
    group by 1
),

deduped as (
    select s.*
    from source s
    join invoice_first_sheet f
      on upper(trim(s.invoice)) = f.invoice_key
     and s.source_sheet = f.first_sheet
),

cleaned as (
    select
        id                                              as transaction_id,
        upper(trim(invoice))                            as invoice,
        upper(trim(stock_code))                         as stock_code,
        trim(description)                               as description,
        quantity::int                                   as quantity,
        invoice_date::timestamp                         as invoice_date,
        price::numeric(12, 4)                           as unit_price,
        nullif(trim(customer_id), '')                   as customer_id,
        trim(country)                                   as country,
        source_sheet,
        (quantity < 0 or upper(trim(invoice)) like 'C%') as is_return,
        (quantity > 0 and upper(trim(invoice)) not like 'C%') as is_sale,
        -- invoice-level document type: 'C' invoices are credit notes, never orders
        (upper(trim(invoice)) like 'C%')                 as is_credit_note,
        -- Postage, fees, accounting adjustments and gift vouchers: real revenue lines,
        -- but not merchandise, so excluded from product-level analytics only.
        (upper(trim(stock_code)) in (
            'M', 'DOT', 'POST', 'AMAZONFEE', 'C2', 'C3', 'B', 'D', 'S', 'CRUK', 'PADS',
            'ADJUST', 'ADJUST2', 'BANK CHARGES', 'TEST001', 'TEST002'
        ) or upper(trim(stock_code)) like 'GIFT\_%')     as is_non_merchandise,
        (quantity * price)::numeric(14, 4)              as line_amount
    from deduped
    where invoice is not null
      and stock_code is not null
      and invoice_date is not null
      and quantity <> 0
      and price is not null
      and upper(trim(invoice)) not like 'A%'
      and not (
          -- exclude bad admin/zero-price rows on non-return lines only
          price <= 0 and quantity > 0 and upper(trim(invoice)) not like 'C%'
      )
)

select * from cleaned
