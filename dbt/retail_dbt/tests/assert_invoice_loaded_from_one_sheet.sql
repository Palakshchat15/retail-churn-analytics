-- Issue 1 guard: the source workbook's two sheets overlap (2010-12-01..09).
-- Every invoice in staging must come from exactly one source sheet; if the
-- same invoice shows up from both sheets its lines are being double counted.
select invoice, count(distinct source_sheet) as n_sheets
from {{ ref('stg_retail_transactions') }}
group by invoice
having count(distinct source_sheet) > 1
