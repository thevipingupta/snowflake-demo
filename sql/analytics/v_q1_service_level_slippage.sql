-- =============================================================================
-- CC_DEMO.GOLD.v_q1_service_level_slippage
-- Business question Q1: where are service levels slipping, by skill and hour of day?
--
-- Grain   : one row per week, skill, and hour of day (interval start, America/New_York).
-- Source  : GOLD.fact_service_level_interval, GOLD.dim_skill, GOLD.dim_date.
-- Metric  : service_level_pct = SERVICE LEVEL from the source, weighted by calls_offered
--           across the intervals in the group. Each interval's SERVICE LEVEL already
--           uses that skill's SOA threshold (soa_threshold_sec). The threshold is not
--           restated here.
--           baseline = average of the same skill-hour over the previous 4 weeks.
--           slipping_flag = service level is at least 10 points below baseline.
--
-- Sample query (expected result, in plain English):
--   SELECT skill_name, hour_of_day, week_start, service_level_pct,
--          baseline_service_level_pct, sl_change_pts
--     FROM CC_DEMO.GOLD.v_q1_service_level_slippage
--    WHERE slipping_flag
--    ORDER BY sl_change_pts LIMIT 10;
--   Expected: the worst rows are CLAIMANT_PHONE in the week of 2026-04-13 (the
--   policy-deadline surge) and in the week of 2026-05-04 (the phone staffing
--   shortfall). In the surge week, service level is low across most hours of the day.
--   EMPLOYER_PHONE also slips in the surge week. GENERAL_PHONE shows little slippage.
-- =============================================================================
CREATE OR REPLACE VIEW CC_DEMO.GOLD.v_q1_service_level_slippage AS
WITH base AS (
    SELECT sk.skill_name,
           DATE_TRUNC('week', d.calendar_date)            AS week_start,
           HOUR(f.interval_start_et)                      AS hour_of_day,
           f.calls_offered,
           f.calls_abandoned,
           f.service_level_pct,
           f.soa_threshold_sec
      FROM CC_DEMO.GOLD.fact_service_level_interval f
      JOIN CC_DEMO.GOLD.dim_skill sk ON sk.skill_key = f.skill_key
      JOIN CC_DEMO.GOLD.dim_date d   ON d.date_key = f.date_key
),
weekly AS (
    SELECT skill_name, week_start, hour_of_day,
           SUM(calls_offered)                                                     AS calls_offered,
           SUM(calls_abandoned)                                                   AS calls_abandoned,
           SUM(service_level_pct * calls_offered)
             / NULLIF(SUM(CASE WHEN service_level_pct IS NOT NULL THEN calls_offered END), 0) AS service_level_pct,
           MAX(soa_threshold_sec)                                                 AS soa_threshold_sec
      FROM base
     GROUP BY skill_name, week_start, hour_of_day
),
with_baseline AS (
    SELECT w.*,
           AVG(service_level_pct) OVER (PARTITION BY skill_name, hour_of_day ORDER BY week_start
                                        ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) AS baseline_service_level_pct
      FROM weekly w
)
SELECT skill_name,
       week_start,
       hour_of_day,
       calls_offered,
       ROUND(service_level_pct, 1)                                        AS service_level_pct,
       soa_threshold_sec,
       ROUND(calls_abandoned / NULLIF(calls_offered, 0) * 100, 1)         AS abandonment_rate_pct,
       ROUND(baseline_service_level_pct, 1)                               AS baseline_service_level_pct,
       ROUND(service_level_pct - baseline_service_level_pct, 1)           AS sl_change_pts,
       IFF(service_level_pct - baseline_service_level_pct <= -10
           AND calls_offered >= 10, TRUE, FALSE)                          AS slipping_flag
  FROM with_baseline;
