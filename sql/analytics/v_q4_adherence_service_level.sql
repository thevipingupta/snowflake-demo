-- =============================================================================
-- CC_DEMO.GOLD.v_q4_adherence_service_level
-- Business question Q4: how does agent adherence relate to service level on the same
-- day and skill?
--
-- DEPENDS ON THE WFM ASSUMPTION. Adherence and logged minutes come from the WFM
-- extract, which is an assumption, not a Five9 source.
--
-- Grain   : one row per calendar date and skill.
-- Metrics : adherence_ratio = SUM(logged_in_minutes) / SUM(scheduled_minutes), from WFM.
--           source_adherence_pct = average of the WFM adherence_pct column, as delivered.
--           service_level_pct    = SERVICE LEVEL from the source, weighted by calls_offered.
--           logged_in_minutes    = the agent hours on the schedule that day.
--
-- Read with care: adherence_ratio and logged_in_minutes answer different questions.
--           A low adherence_ratio is a schedule-adherence problem. Low logged minutes
--           are a headcount problem.
--
-- Sample query (expected result, in plain English):
--   SELECT skill_name,
--          CORR(adherence_ratio, service_level_pct)    AS corr_adherence_sl,
--          CORR(logged_in_minutes, service_level_pct)  AS corr_logged_sl
--     FROM CC_DEMO.GOLD.v_q4_adherence_service_level
--    WHERE service_level_pct IS NOT NULL
--    GROUP BY skill_name;
--   Expected: the adherence correlation is near zero for every skill (about -0.15 to 0.1).
--   The logged-minutes correlation is clearly positive (about 0.57 pooled). Staffed
--   hours track service level. Adherence ratio does not. The week 10 shortfall is a
--   headcount effect, and the adherence ratio does not show it.
-- =============================================================================
CREATE OR REPLACE VIEW CC_DEMO.GOLD.v_q4_adherence_service_level AS
WITH staff AS (
    SELECT r.skill_name, fa.date_key,
           SUM(fa.logged_in_minutes)  AS logged_in_minutes,
           SUM(fa.scheduled_minutes)  AS scheduled_minutes,
           AVG(fa.adherence_pct)      AS source_adherence_pct,
           COUNT(DISTINCT fa.agent_id) AS agents_scheduled
      FROM CC_DEMO.GOLD.fact_agent_daily fa
      JOIN CC_DEMO.GOLD.dim_agent a       ON a.agent_id = fa.agent_id
      JOIN CC_DEMO.GOLD.ref_team_skill r  ON r.agent_group = a.agent_group
     WHERE fa.scheduled_minutes > 0
     GROUP BY r.skill_name, fa.date_key
),
sl AS (
    SELECT sk.skill_name, f.date_key,
           SUM(f.service_level_pct * f.calls_offered)
             / NULLIF(SUM(CASE WHEN f.service_level_pct IS NOT NULL THEN f.calls_offered END), 0) AS service_level_pct,
           SUM(f.calls_offered) AS calls_offered
      FROM CC_DEMO.GOLD.fact_service_level_interval f
      JOIN CC_DEMO.GOLD.dim_skill sk ON sk.skill_key = f.skill_key
     GROUP BY sk.skill_name, f.date_key
)
SELECT d.calendar_date,
       s.skill_name,
       ROUND(st.logged_in_minutes / st.scheduled_minutes, 4) AS adherence_ratio,
       ROUND(st.source_adherence_pct, 1)                     AS source_adherence_pct,
       st.logged_in_minutes,
       st.scheduled_minutes,
       st.agents_scheduled,
       ROUND(s.service_level_pct, 1)                         AS service_level_pct,
       s.calls_offered
  FROM sl s
  JOIN staff st ON st.skill_name = s.skill_name AND st.date_key = s.date_key
  JOIN CC_DEMO.GOLD.dim_date d ON d.date_key = s.date_key;
