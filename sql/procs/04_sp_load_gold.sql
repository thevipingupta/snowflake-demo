-- =============================================================================
-- 04_sp_load_gold.sql  (Snowflake Scripting)
-- =============================================================================
-- CC_DEMO.CONTROL.sp_load_gold(p_batch_id) RETURNS VARIANT
--
-- Refreshes the GOLD star schema for the dates touched by the batch.
--
-- Date range: from the silver rows of this batch (call date, ACD interval
-- date, WFM work date). Gold tables are rebuilt for that range only.
--
-- Method: every table is written with MERGE on its grain columns, so a re-run
-- updates rows instead of adding them. Rows inside the date range that no
-- longer appear in silver are removed with a DELETE. Snowflake MERGE has no
-- "not matched by source" clause, so the DELETE is a separate statement.
-- No plain INSERT is used for gold tables.
--
-- Order:
--   dim_date, dim_skill, dim_media_type, dim_disposition   (MERGE on natural key)
--   fact_repeat_call      (MERGE on repeat_contact_id; repeats from stg_call history)
--   fact_call_daily       (MERGE on date, skill, media type, disposition)
--   fact_service_level_interval (MERGE on skill, interval_start_et)
--   fact_agent_daily      (MERGE on date, agent_id; agent_key = version in effect)
--   dim_agent is a view on dim_agent_scd2 (nothing to load)
--
-- Returns: {batch_id, source_table, status, rows_in, rows_loaded, rows_quarantined,
--           dq_errors, dq_warnings, message}. rows_loaded = gold rows merged, all facts.
-- =============================================================================

CREATE OR REPLACE PROCEDURE CC_DEMO.CONTROL.sp_load_gold(p_batch_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_from     DATE;
    v_to       DATE;
    v_from_key NUMBER(8,0);
    v_to_key   NUMBER(8,0);
    v_merged   NUMBER DEFAULT 0;
    v_n        NUMBER;
    v_started  TIMESTAMP_NTZ;
BEGIN
    v_started := CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ;

    -- Date range of this batch, from silver.
    SELECT MIN(d), MAX(d) INTO :v_from, :v_to
      FROM (SELECT call_date_et AS d FROM CC_DEMO.SILVER.stg_call WHERE _batch_id = :p_batch_id
            UNION ALL SELECT interval_date_et FROM CC_DEMO.SILVER.stg_acd_interval WHERE _batch_id = :p_batch_id
            UNION ALL SELECT work_date FROM CC_DEMO.SILVER.stg_wfm_daily WHERE _batch_id = :p_batch_id);
    IF (v_from IS NULL) THEN
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'GOLD', 'status', 'SUCCEEDED',
            'rows_in', 0, 'rows_loaded', 0, 'rows_quarantined', 0, 'dq_errors', 0, 'dq_warnings', 0,
            'message', 'no silver rows for this batch; gold unchanged');
    END IF;
    v_from_key := TO_NUMBER(TO_CHAR(v_from, 'YYYYMMDD'));
    v_to_key   := TO_NUMBER(TO_CHAR(v_to, 'YYYYMMDD'));

    BEGIN TRANSACTION;

    -- dim_date: every date in the range, from the silver dates.
    MERGE INTO CC_DEMO.GOLD.dim_date t
    USING (SELECT DISTINCT TO_NUMBER(TO_CHAR(d, 'YYYYMMDD')) AS date_key, d AS calendar_date,
                  YEAR(d) AS year_num, QUARTER(d) AS quarter_num, MONTH(d) AS month_num,
                  MONTHNAME(d) AS month_name, DAY(d) AS day_of_month,
                  DAYOFWEEKISO(d) AS day_of_week_num, DAYNAME(d) AS day_name,
                  WEEKOFYEAR(d) AS week_of_year, DAYOFWEEKISO(d) >= 6 AS is_weekend
             FROM (SELECT call_date_et AS d FROM CC_DEMO.SILVER.stg_call
                   UNION SELECT interval_date_et FROM CC_DEMO.SILVER.stg_acd_interval
                   UNION SELECT work_date FROM CC_DEMO.SILVER.stg_wfm_daily)
            WHERE d BETWEEN :v_from AND :v_to) s
       ON t.date_key = s.date_key
     WHEN MATCHED THEN UPDATE SET calendar_date = s.calendar_date, year_num = s.year_num,
          quarter_num = s.quarter_num, month_num = s.month_num, month_name = s.month_name,
          day_of_month = s.day_of_month, day_of_week_num = s.day_of_week_num, day_name = s.day_name,
          week_of_year = s.week_of_year, is_weekend = s.is_weekend
     WHEN NOT MATCHED THEN INSERT (date_key, calendar_date, year_num, quarter_num, month_num, month_name,
          day_of_month, day_of_week_num, day_name, week_of_year, is_weekend)
          VALUES (s.date_key, s.calendar_date, s.year_num, s.quarter_num, s.month_num, s.month_name,
          s.day_of_month, s.day_of_week_num, s.day_name, s.week_of_year, s.is_weekend);

    -- Dimensions on natural keys. Keys are AUTOINCREMENT.
    MERGE INTO CC_DEMO.GOLD.dim_skill t
    USING (SELECT skill AS skill_name, MAX(campaign) AS campaign_name, MAX(soa_threshold_sec) AS soa_threshold_sec
             FROM CC_DEMO.SILVER.stg_acd_interval
            WHERE interval_date_et BETWEEN :v_from AND :v_to AND skill IS NOT NULL
            GROUP BY skill) s
       ON t.skill_name = s.skill_name
     WHEN MATCHED THEN UPDATE SET campaign_name = s.campaign_name, soa_threshold_sec = s.soa_threshold_sec
     WHEN NOT MATCHED THEN INSERT (skill_name, campaign_name, soa_threshold_sec)
          VALUES (s.skill_name, s.campaign_name, s.soa_threshold_sec);

    MERGE INTO CC_DEMO.GOLD.dim_media_type t
    USING (SELECT DISTINCT channel AS media_type FROM CC_DEMO.SILVER.stg_call
            WHERE call_date_et BETWEEN :v_from AND :v_to AND channel IS NOT NULL) s
       ON t.media_type = s.media_type
     WHEN NOT MATCHED THEN INSERT (media_type) VALUES (s.media_type);

    MERGE INTO CC_DEMO.GOLD.dim_disposition t
    USING (SELECT DISTINCT disposition, disposition_group_a, disposition_group_b, disposition_group_c, disposition_path
             FROM CC_DEMO.SILVER.stg_call
            WHERE call_date_et BETWEEN :v_from AND :v_to AND disposition_path IS NOT NULL) s
       ON t.disposition_path = s.disposition_path
     WHEN MATCHED THEN UPDATE SET disposition = s.disposition, disposition_group_a = s.disposition_group_a,
          disposition_group_b = s.disposition_group_b, disposition_group_c = s.disposition_group_c
     WHEN NOT MATCHED THEN INSERT (disposition, disposition_group_a, disposition_group_b, disposition_group_c, disposition_path)
          VALUES (s.disposition, s.disposition_group_a, s.disposition_group_b, s.disposition_group_c, s.disposition_path);

    -- fact_repeat_call: a call is a repeat when an earlier call (stg_call, all history)
    -- has the same customer_key and was 0-7 calendar days before it. The most recent
    -- such earlier call is kept.
    CREATE OR REPLACE TEMPORARY TABLE tmp_repeat AS
    SELECT cur.contact_id                                   AS repeat_contact_id,
           prior.contact_id                                 AS prior_contact_id,
           cur.customer_key                                 AS customer_key,
           TO_NUMBER(TO_CHAR(cur.call_date_et, 'YYYYMMDD')) AS call_date_key,
           TO_NUMBER(TO_CHAR(prior.call_date_et, 'YYYYMMDD')) AS prior_call_date_key,
           DATEDIFF('day', prior.call_date_et, cur.call_date_et) AS days_since_prior_call,
           sk.skill_key                                     AS skill_key,
           mt.media_type_key                                AS media_type_key
      FROM CC_DEMO.SILVER.stg_call cur
      JOIN CC_DEMO.SILVER.stg_call prior
        ON prior.customer_key = cur.customer_key
       AND prior.contact_id <> cur.contact_id
       AND prior.call_start_et < cur.call_start_et
       AND DATEDIFF('day', prior.call_date_et, cur.call_date_et) BETWEEN 0 AND 7
      JOIN CC_DEMO.GOLD.dim_skill sk ON sk.skill_name = cur.skill
      JOIN CC_DEMO.GOLD.dim_media_type mt ON mt.media_type = cur.channel
     WHERE cur.call_date_et BETWEEN :v_from AND :v_to
       AND cur.customer_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cur.contact_id ORDER BY prior.call_start_et DESC) = 1;

    MERGE INTO CC_DEMO.GOLD.fact_repeat_call t
    USING tmp_repeat s ON t.repeat_contact_id = s.repeat_contact_id
     WHEN MATCHED THEN UPDATE ALL BY NAME
     WHEN NOT MATCHED THEN INSERT ALL BY NAME;
    DELETE FROM CC_DEMO.GOLD.fact_repeat_call
     WHERE call_date_key BETWEEN :v_from_key AND :v_to_key
       AND repeat_contact_id NOT IN (SELECT repeat_contact_id FROM tmp_repeat);

    -- fact_call_daily: grain date, skill, media type, disposition.
    CREATE OR REPLACE TEMPORARY TABLE tmp_call_daily AS
    SELECT TO_NUMBER(TO_CHAR(c.call_date_et, 'YYYYMMDD')) AS date_key,
           sk.skill_key, mt.media_type_key, dd.disposition_key,
           COUNT(*)                                    AS calls_offered,
           SUM(COALESCE(c.contacted_flag, 0))          AS calls_answered,
           SUM(COALESCE(c.abandoned_flag, 0))          AS calls_abandoned,
           SUM(COALESCE(c.handle_seconds, 0))          AS handle_seconds_total,
           COUNT(r.repeat_contact_id)                  AS repeat_calls
      FROM CC_DEMO.SILVER.stg_call c
      JOIN CC_DEMO.GOLD.dim_skill sk        ON sk.skill_name = c.skill
      JOIN CC_DEMO.GOLD.dim_media_type mt   ON mt.media_type = c.channel
      JOIN CC_DEMO.GOLD.dim_disposition dd  ON dd.disposition_path = c.disposition_path
      LEFT JOIN CC_DEMO.GOLD.fact_repeat_call r ON r.repeat_contact_id = c.contact_id
     WHERE c.call_date_et BETWEEN :v_from AND :v_to
     GROUP BY 1, 2, 3, 4;

    MERGE INTO CC_DEMO.GOLD.fact_call_daily t
    USING tmp_call_daily s
       ON t.date_key = s.date_key AND t.skill_key = s.skill_key
      AND t.media_type_key = s.media_type_key AND t.disposition_key = s.disposition_key
     WHEN MATCHED THEN UPDATE ALL BY NAME
     WHEN NOT MATCHED THEN INSERT ALL BY NAME;
    DELETE FROM CC_DEMO.GOLD.fact_call_daily f
     WHERE f.date_key BETWEEN :v_from_key AND :v_to_key
       AND NOT EXISTS (SELECT 1 FROM tmp_call_daily s
                        WHERE s.date_key = f.date_key AND s.skill_key = f.skill_key
                          AND s.media_type_key = f.media_type_key AND s.disposition_key = f.disposition_key);

    -- fact_service_level_interval: grain skill and interval. SERVICE LEVEL is the source value.
    MERGE INTO CC_DEMO.GOLD.fact_service_level_interval t
    USING (SELECT TO_NUMBER(TO_CHAR(a.interval_date_et, 'YYYYMMDD')) AS date_key,
                  a.interval_start_et, sk.skill_key, a.campaign AS campaign_name,
                  a.calls_offered, a.calls_answered, a.calls_abandoned, a.service_level_pct,
                  a.soa_threshold_sec, a.queue_wait_seconds, a.handle_seconds, a.dq_status
             FROM CC_DEMO.SILVER.stg_acd_interval a
             JOIN CC_DEMO.GOLD.dim_skill sk ON sk.skill_name = a.skill
            WHERE a.interval_date_et BETWEEN :v_from AND :v_to) s
       ON t.skill_key = s.skill_key AND t.interval_start_et = s.interval_start_et
     WHEN MATCHED THEN UPDATE ALL BY NAME
     WHEN NOT MATCHED THEN INSERT ALL BY NAME;
    DELETE FROM CC_DEMO.GOLD.fact_service_level_interval f
     WHERE f.date_key BETWEEN :v_from_key AND :v_to_key
       AND NOT EXISTS (SELECT 1 FROM CC_DEMO.SILVER.stg_acd_interval a
                        JOIN CC_DEMO.GOLD.dim_skill sk ON sk.skill_name = a.skill
                        WHERE sk.skill_key = f.skill_key AND a.interval_start_et = f.interval_start_et);

    -- fact_agent_daily (ASSUMPTION: depends on the WFM extract). agent_key is the
    -- dim_agent_scd2 version in effect on the work date. handled_calls counts calls
    -- in stg_call for that agent and date.
    MERGE INTO CC_DEMO.GOLD.fact_agent_daily t
    USING (SELECT TO_NUMBER(TO_CHAR(w.work_date, 'YYYYMMDD')) AS date_key,
                  d.agent_key, w.agent_id, w.scheduled_minutes, w.logged_in_minutes, w.adherence_pct,
                  COUNT(c.contact_id) AS handled_calls
             FROM CC_DEMO.SILVER.stg_wfm_daily w
             JOIN CC_DEMO.SILVER.dim_agent_scd2 d
               ON d.agent_id = w.agent_id AND w.work_date BETWEEN d.effective_start_date AND d.effective_end_date
             LEFT JOIN CC_DEMO.SILVER.stg_call c
               ON c.agent_id = w.agent_id AND c.call_date_et = w.work_date
            WHERE w.work_date BETWEEN :v_from AND :v_to
            GROUP BY 1, 2, 3, 4, 5, 6) s
       ON t.date_key = s.date_key AND t.agent_id = s.agent_id
     WHEN MATCHED THEN UPDATE ALL BY NAME
     WHEN NOT MATCHED THEN INSERT ALL BY NAME;
    DELETE FROM CC_DEMO.GOLD.fact_agent_daily f
     WHERE f.date_key BETWEEN :v_from_key AND :v_to_key
       AND NOT EXISTS (SELECT 1 FROM CC_DEMO.SILVER.stg_wfm_daily w
                        WHERE TO_NUMBER(TO_CHAR(w.work_date, 'YYYYMMDD')) = f.date_key AND w.agent_id = f.agent_id);

    SELECT (SELECT COUNT(*) FROM tmp_call_daily) + (SELECT COUNT(*) FROM tmp_repeat)
         + (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_acd_interval WHERE interval_date_et BETWEEN :v_from AND :v_to)
         + (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_wfm_daily WHERE work_date BETWEEN :v_from AND :v_to)
      INTO :v_merged;

    COMMIT;

    RETURN OBJECT_CONSTRUCT(
        'batch_id', p_batch_id,
        'source_table', 'GOLD',
        'status', 'SUCCEEDED',
        'rows_in', v_merged,
        'rows_loaded', v_merged,
        'rows_quarantined', 0,
        'dq_errors', 0,
        'dq_warnings', 0,
        'message', 'gold refreshed for ' || v_from || ' to ' || v_to);

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        INSERT INTO CC_DEMO.CONTROL.load_log (batch_id, source_table, source_file, rows_read, rows_loaded,
                                              rows_skipped_duplicate_batch, status, error_message, started_ts, ended_ts)
        SELECT :p_batch_id, 'GOLD', NULL, 0, 0, 0, 'FAILED', SQLERRM, :v_started,
               CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ;
        RAISE;
END;
$$;
