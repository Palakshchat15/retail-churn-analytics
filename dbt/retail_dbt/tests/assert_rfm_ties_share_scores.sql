-- Issue 6 guard: customers with identical recency/frequency/monetary values
-- must receive identical R/F/M scores (ntile() used to split ties arbitrarily).
select 'r' as score, recency_days::text as value
from {{ ref('mart_customer_rfm') }} group by recency_days having count(distinct r_score) > 1
union all
select 'f', frequency::text
from {{ ref('mart_customer_rfm') }} group by frequency having count(distinct f_score) > 1
union all
select 'm', monetary::text
from {{ ref('mart_customer_rfm') }} group by monetary having count(distinct m_score) > 1
