-- Customer RFM segmentation mart.
--
-- Scoring: each of Recency/Frequency/Monetary is scored 1-5 from its
-- percentile rank across customers: score = 1 + floor(5 * percent_rank),
-- capped at 5. percent_rank gives tied values the same rank, so identical
-- customers always get identical scores and the result is deterministic
-- (ntile() split ties arbitrarily across buckets). Because of ties, buckets
-- are not exactly 20% each (e.g. every one-order customer shares F = 1).
-- Recency is inverted (fewer days since last order = higher score) so that in
-- all three scores, 5 = best/most valuable.
--
-- Only customers with >= 1 sale order are scored; customers with nothing but
-- credit notes in the data window have no purchase to score.
--
-- Segment logic (classic RFM heuristic, thresholds chosen to be
-- interpretable and defensible for a portfolio project):
--   Champions   : R>=4 and F>=4 and M>=4   (recent, frequent, big spenders)
--   Loyal       : F>=4 and M>=3 and R>=2   (frequent/high spend, reasonably recent)
--   Potential Loyalist: R>=4 and F between 2-3 (recent, building frequency)
--   At Risk     : R<=2 and (F>=3 or M>=3)  (used to be valuable, going quiet)
--   Lost        : R<=2 and F<=2 and M<=2   (low on all three - largely inactive)
--   Others      : everything not matched above (mid-tier / new customers)

with rfm as (
    select * from {{ ref('int_customer_rfm_base') }}
    where frequency > 0
),

ranked as (
    select
        *,
        -- order by recency_days DESC: the longest-silent customers rank 0 (score 1)
        percent_rank() over (order by recency_days desc) as r_pct,
        percent_rank() over (order by frequency asc)     as f_pct,
        percent_rank() over (order by monetary asc)      as m_pct
    from rfm
),

scored as (
    select
        *,
        least(5, 1 + floor(5 * r_pct))::int as r_score,
        least(5, 1 + floor(5 * f_pct))::int as f_score,
        least(5, 1 + floor(5 * m_pct))::int as m_score
    from ranked
),

final as (
    select
        customer_id,
        country,
        first_order_date,
        last_order_date,
        recency_days,
        frequency,
        monetary,
        total_items_purchased,
        total_items_returned,
        return_rate,
        r_score,
        f_score,
        m_score,
        (r_score + f_score + m_score) as rfm_total_score,
        case
            when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'Champions'
            when f_score >= 4 and m_score >= 3 and r_score >= 2 then 'Loyal'
            when r_score >= 4 and f_score between 2 and 3 then 'Potential Loyalist'
            when r_score <= 2 and (f_score >= 3 or m_score >= 3) then 'At Risk'
            when r_score <= 2 and f_score <= 2 and m_score <= 2 then 'Lost'
            else 'Others'
        end as customer_segment
    from scored
)

select * from final
