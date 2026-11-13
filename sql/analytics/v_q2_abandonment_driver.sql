-- =============================================================================
-- CC_DEMO.GOLD.v_q2_abandonment_driver
-- Business question Q2: why did abandonment spike on a given day? Surge or staffing?
--
-- DEPENDS ON THE WFM ASSUMPTION. The staffing side (logged_in_minutes) comes from
-- the WFM extract, which is an assumption, not a Five9 source.
--
-- Grain   : one row per calendar date and skill.
-- Sources : GOLD.fact_service_level_interval (calls, abandonment),
--           GOLD.fact_agent_daily (WFM staffing), GOLD.dim_agent, GOLD.ref_team_skill
--           (agent group to skill; ASSUMPTION), GOLD.dim_skill, GOLD.dim_date.
-- Baseline: the same weekday over the previous 4 weeks (weekend days compare with
--           weekend days, so weekends do not look like surges).
-- Rules   : spike         = abandonment_rate_pct is at least 5 points above baseline.
--           volume_ratio  = calls_offered / baseline calls_offered.
--           staffing_ratio = logged_in_minutes / baseline logged_in_minutes (WFM).
--           VOLUME_SURGE          volume_ratio >= 1.5 and staffing_ratio > 0.85
--           STAFFING_SHORTFALL    staffing_ratio <= 0.85 and volume_ratio < 1.5
--           SURGE_AND_SHORTFALL   both conditions
--           UNEXPLAINED           a spike with neither condition
--           NO_SPIKE / NO_BASELINE otherwise.
--
-- Sample query (expected result, in plain English):
--   SELECT calendar_date, skill_name, abandonment_rate_pct, base_abandonment_rate_pct,
--          volume_ratio, staffing_ratio, driver
--     FROM CC_DEMO.GOLD.v_q2_abandonment_driver
--    WHERE driver <> 'NO_SPIKE' AND driver <> 'NO_BASELINE'
--    ORDER BY calendar_date;
--   Expected: CLAIMANT_PHONE surge days from 2026-04-08 to 2026-04-17 come out as
--   VOLUME_SURGE, with 2026-04-15 as SURGE_AND_SHORTFALL. Shortfall days in the week of
--   2026-05-04 come out as STAFFING_SHORTFALL. Several weekend days show 100% abandonment
--   with zero logged minutes, meaning no agents were rostered for that skill that day.
--   These are STAFFING_SHORTFALL rows, and they are a feature of the synthetic crew schedule.
-- =============================================================================
CREATE OR REPLACE VIEW CC_DEMO.GOLD.v_q2_abandonment_driver AS
WITH daily AS (
    SELECT d.calendar_date, d.day_of_week_num, sk.skill_name,
           SUM(f.calls_offered)   AS calls_offered,
           SUM(f.calls_abandoned) AS calls_abandoned
      FROM CC_DEMO.GOLD.fact_service_level_interval f
      JOIN CC_DEMO.GOLD.dim_skill sk ON sk.skill_key = f.skill_key
      JOIN CC_DEMO.GOLD.dim_date d   ON d.date_key = f.date_key
     GROUP BY d.calendar_date, d.day_of_week_num, sk.skill_name
),
staff AS (
    SELECT r.skill_name, fa.date_key,
           SUM(fa.logged_in_minutes) AS logged_in_minutes,
           COUNT(DISTINCT fa.agent_id) AS agents_logged
      FROM CC_DEMO.GOLD.fact_agent_daily fa
      JOIN CC_DEMO.GOLD.dim_agent a       ON a.agent_id = fa.agent_id
      JOIN CC_DEMO.GOLD.ref_team_skill r  ON r.agent_group = a.agent_group
     GROUP BY r.skill_name, fa.date_key
),
joined AS (
    SELECT dl.calendar_date, dl.day_of_week_num, dl.skill_name,
           dl.calls_offered, dl.calls_abandoned,
           dl.calls_abandoned / NULLIF(dl.calls_offered, 0) * 100 AS abandonment_rate_pct,
           COALESCE(st.logged_in_minutes, 0) AS logged_in_minutes,
           COALESCE(st.agents_logged, 0)     AS agents_logged
      FROM daily dl
      LEFT JOIN staff st
        ON st.skill_name = dl.skill_name
       AND st.date_key = TO_NUMBER(TO_CHAR(dl.calendar_date, 'YYYYMMDD'))
),
base AS (
    SELECT j.*,
           AVG(calls_offered) OVER (PARTITION BY skill_name, day_of_week_num ORDER BY calendar_date
                                    ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING)           AS base_calls_offered,
           AVG(logged_in_minutes) OVER (PARTITION BY skill_name, day_of_week_num ORDER BY calendar_date
                                    ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING)           AS base_logged_minutes,
           AVG(abandonment_rate_pct) OVER (PARTITION BY skill_name, day_of_week_num ORDER BY calendar_date
                                    ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING)           AS base_abandonment_rate_pct
      FROM joined j
),
scored AS (
    SELECT b.*,
           b.calls_offered / NULLIF(b.base_calls_offered, 0)                AS volume_ratio,
           b.logged_in_minutes / NULLIF(b.base_logged_minutes, 0)           AS staffing_ratio
      FROM base b
)
SELECT calendar_date,
       skill_name,
       calls_offered,
       calls_abandoned,
       ROUND(abandonment_rate_pct, 1)       AS abandonment_rate_pct,
       ROUND(base_abandonment_rate_pct, 1)  AS base_abandonment_rate_pct,
       logged_in_minutes,
       ROUND(base_logged_minutes, 0)        AS base_logged_minutes,
       agents_logged,
       ROUND(volume_ratio, 2)               AS volume_ratio,
       ROUND(staffing_ratio, 2)             AS staffing_ratio,
       CASE
           WHEN base_abandonment_rate_pct IS NULL                                  THEN 'NO_BASELINE'
           WHEN abandonment_rate_pct - base_abandonment_rate_pct < 5                THEN 'NO_SPIKE'
           WHEN volume_ratio >= 1.5 AND staffing_ratio > 0.85                       THEN 'VOLUME_SURGE'
           WHEN staffing_ratio <= 0.85 AND volume_ratio < 1.5                       THEN 'STAFFING_SHORTFALL'
           WHEN volume_ratio >= 1.5 AND staffing_ratio <= 0.85                      THEN 'SURGE_AND_SHORTFALL'
           ELSE 'UNEXPLAINED'
       END                                  AS driver
  FROM scored;
