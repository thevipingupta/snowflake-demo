-- =============================================================================
-- CC_DEMO.GOLD.v_q3_disposition_repeat
-- Business question Q3: which dispositions lead to repeat calls from the same
-- customer_key within 7 days?
--
-- Grain   : one row per disposition path (the disposition of the EARLIER call).
-- Sources : GOLD.fact_call_daily (all calls by disposition), GOLD.fact_repeat_call
--           (repeat pairs, with prior_disposition_key), GOLD.dim_disposition.
-- Repeat call (VEC must confirm): a call from the same customer_key (SALESFORCE_ID,
--           or ANI when SALESFORCE_ID is empty) within 7 calendar days of an earlier call.
-- Metric  : repeat_rate_pct = calls with a repeat within 7 days / all calls with that
--           disposition. The rate is for the earlier call's disposition.
--
-- Limitation: calls in the last 7 days of loaded data have not had their 7-day
--           window complete, so their rates are understated.
--
-- Sample query (expected result, in plain English):
--   SELECT disposition_path, calls_with_disposition, repeat_rate_pct, repeat_rank
--     FROM CC_DEMO.GOLD.v_q3_disposition_repeat
--    ORDER BY repeat_rank LIMIT 5;
--   Expected: repeat rates sit close together, at roughly 14-16% for every disposition.
--   No disposition stands out as a driver of repeat calls. Differences of about 1 point
--   are within noise. The generator does not tie repeats to disposition, so a flat
--   result is correct behavior for this synthetic data.
-- =============================================================================
CREATE OR REPLACE VIEW CC_DEMO.GOLD.v_q3_disposition_repeat AS
WITH calls AS (
    SELECT disposition_key, SUM(calls_offered) AS calls_with_disposition
      FROM CC_DEMO.GOLD.fact_call_daily
     GROUP BY disposition_key
),
followed AS (
    SELECT prior_disposition_key AS disposition_key,
           COUNT(DISTINCT prior_contact_id) AS calls_followed_by_repeat,
           COUNT(*)                         AS repeat_calls,
           ROUND(AVG(days_since_prior_call), 2) AS avg_days_to_repeat
      FROM CC_DEMO.GOLD.fact_repeat_call
     WHERE prior_disposition_key IS NOT NULL
     GROUP BY prior_disposition_key
),
joined AS (
    SELECT dd.disposition_group_a, dd.disposition_group_b, dd.disposition_group_c,
           dd.disposition_path,
           c.calls_with_disposition,
           COALESCE(f.calls_followed_by_repeat, 0) AS calls_followed_by_repeat,
           COALESCE(f.repeat_calls, 0)             AS repeat_calls,
           f.avg_days_to_repeat
      FROM CC_DEMO.GOLD.dim_disposition dd
      JOIN calls c ON c.disposition_key = dd.disposition_key
      LEFT JOIN followed f ON f.disposition_key = dd.disposition_key
)
SELECT disposition_group_a,
       disposition_group_b,
       disposition_group_c,
       disposition_path,
       calls_with_disposition,
       calls_followed_by_repeat,
       repeat_calls,
       ROUND(calls_followed_by_repeat / NULLIF(calls_with_disposition, 0) * 100, 1) AS repeat_rate_pct,
       avg_days_to_repeat,
       RANK() OVER (ORDER BY calls_followed_by_repeat / NULLIF(calls_with_disposition, 0) DESC) AS repeat_rank
  FROM joined;
