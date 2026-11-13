-- =============================================================================
-- 05_sp_reconcile.sql  (Snowflake Scripting)
-- =============================================================================
-- CC_DEMO.CONTROL.sp_reconcile(p_batch_id) RETURNS VARIANT
--
-- Writes counts and sums for each metric to CONTROL.reconciliation, for the
-- bronze, silver, and gold layers. Each row is one comparison.
--
-- Tolerance:
--   counts: zero (values must be equal)
--   sums:   0.01% of the left-hand value
--
-- Column use (CONTROL.reconciliation):
--   source_value = file side (file rows read by the bronze COPY)  or  the left-hand value
--   bronze_value = bronze side
--   silver_value = silver side, or the left-hand value for gold-internal checks
--   gold_value   = gold side,   or the right-hand value for gold-internal checks
--
-- Layer checks:
--   B1 file_rows_vs_bronze          rows the bronze COPY read == rows in the raw table
--   B2 bronze_vs_silver_accounted   raw rows == silver rows + quarantined rows
--                                   (SERVICE_LEVEL_OUT_OF_RANGE rows are in silver too,
--                                   so they are not counted twice)
--   G1 calls_vs_gold                silver calls in the date range == sum of fact_call_daily calls_offered
--   G2 handle_seconds_vs_gold       silver handle seconds == sum of fact_call_daily handle_seconds_total (sum tolerance)
--   G3 acd_intervals_vs_gold        silver ACD intervals == fact_service_level_interval rows
--   G4 wfm_rows_vs_gold             silver WFM rows == fact_agent_daily rows
--   G5 repeat_calls_vs_fact         sum of fact_call_daily repeat_calls == fact_repeat_call rows
--
-- Date range for G1-G5: the dates of this batch in silver.
--
-- Returns: {batch_id, source_table, status (PASS | FAIL), rows_in, rows_loaded,
--           rows_quarantined, dq_errors, dq_warnings, failed_checks, message}
-- A FAIL here stops sp_run_pipeline.
-- =============================================================================

CREATE OR REPLACE PROCEDURE CC_DEMO.CONTROL.sp_reconcile(p_batch_id VARCHAR)
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
    v_checks   NUMBER;
    v_failed   NUMBER;
    v_started  TIMESTAMP_NTZ;
BEGIN
    v_started := CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ;

    SELECT MIN(d), MAX(d) INTO :v_from, :v_to
      FROM (SELECT call_date_et AS d FROM CC_DEMO.SILVER.stg_call WHERE _batch_id = :p_batch_id
            UNION ALL SELECT interval_date_et FROM CC_DEMO.SILVER.stg_acd_interval WHERE _batch_id = :p_batch_id
            UNION ALL SELECT work_date FROM CC_DEMO.SILVER.stg_wfm_daily WHERE _batch_id = :p_batch_id);
    v_from_key := TO_NUMBER(TO_CHAR(v_from, 'YYYYMMDD'));
    v_to_key   := TO_NUMBER(TO_CHAR(v_to, 'YYYYMMDD'));

    CREATE OR REPLACE TEMPORARY TABLE tmp_rec AS
    WITH counts AS (
        SELECT 'API_CALLLOG' AS source_table,
               (SELECT rows_read FROM CC_DEMO.CONTROL.load_log l WHERE l.batch_id = :p_batch_id
                 AND l.source_table = 'API_CALLLOG' AND l.status = 'SUCCEEDED' ORDER BY l.ended_ts DESC LIMIT 1) AS file_rows,
               (SELECT COUNT(*) FROM CC_DEMO.BRONZE.raw_api_calllog WHERE _batch_id = :p_batch_id) AS bronze_rows,
               (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_call WHERE _batch_id = :p_batch_id)
             + (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_quarantine_record q WHERE q.batch_id = :p_batch_id
                 AND q.source_table = 'raw_api_calllog' AND q.reason_code <> 'SERVICE_LEVEL_OUT_OF_RANGE') AS silver_accounted
        UNION ALL
        SELECT 'API_CALL_SEGMENTREPORTALL',
               (SELECT rows_read FROM CC_DEMO.CONTROL.load_log l WHERE l.batch_id = :p_batch_id
                 AND l.source_table = 'API_CALL_SEGMENTREPORTALL' AND l.status = 'SUCCEEDED' ORDER BY l.ended_ts DESC LIMIT 1),
               (SELECT COUNT(*) FROM CC_DEMO.BRONZE.raw_api_call_segment WHERE _batch_id = :p_batch_id),
               (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_call_segment WHERE _batch_id = :p_batch_id)
             + (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_quarantine_record q WHERE q.batch_id = :p_batch_id
                 AND q.source_table = 'raw_api_call_segment' AND q.reason_code <> 'SERVICE_LEVEL_OUT_OF_RANGE')
        UNION ALL
        SELECT 'API_VOICEMAIL_LIFECYCLE_LOG',
               (SELECT rows_read FROM CC_DEMO.CONTROL.load_log l WHERE l.batch_id = :p_batch_id
                 AND l.source_table = 'API_VOICEMAIL_LIFECYCLE_LOG' AND l.status = 'SUCCEEDED' ORDER BY l.ended_ts DESC LIMIT 1),
               (SELECT COUNT(*) FROM CC_DEMO.BRONZE.raw_api_voicemail_lifecycle WHERE _batch_id = :p_batch_id),
               (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_voicemail WHERE _batch_id = :p_batch_id)
             + (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_quarantine_record q WHERE q.batch_id = :p_batch_id
                 AND q.source_table = 'raw_api_voicemail_lifecycle' AND q.reason_code <> 'SERVICE_LEVEL_OUT_OF_RANGE')
        UNION ALL
        SELECT 'API_ACD_ALL',
               (SELECT rows_read FROM CC_DEMO.CONTROL.load_log l WHERE l.batch_id = :p_batch_id
                 AND l.source_table = 'API_ACD_ALL' AND l.status = 'SUCCEEDED' ORDER BY l.ended_ts DESC LIMIT 1),
               (SELECT COUNT(*) FROM CC_DEMO.BRONZE.raw_api_acd_all WHERE _batch_id = :p_batch_id),
               (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_acd_interval WHERE _batch_id = :p_batch_id)
             + (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_quarantine_record q WHERE q.batch_id = :p_batch_id
                 AND q.source_table = 'raw_api_acd_all' AND q.reason_code <> 'SERVICE_LEVEL_OUT_OF_RANGE')
        UNION ALL
        SELECT 'WFM_DAILY',
               (SELECT rows_read FROM CC_DEMO.CONTROL.load_log l WHERE l.batch_id = :p_batch_id
                 AND l.source_table = 'WFM_DAILY' AND l.status = 'SUCCEEDED' ORDER BY l.ended_ts DESC LIMIT 1),
               (SELECT COUNT(*) FROM CC_DEMO.BRONZE.raw_wfm_daily WHERE _batch_id = :p_batch_id),
               (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_wfm_daily WHERE _batch_id = :p_batch_id)
             + (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_quarantine_record q WHERE q.batch_id = :p_batch_id
                 AND q.source_table = 'raw_wfm_daily' AND q.reason_code <> 'SERVICE_LEVEL_OUT_OF_RANGE')
    )
    SELECT source_table || '.file_rows_vs_bronze' AS metric, file_rows AS source_value, bronze_rows AS bronze_value,
           NULL::NUMBER AS silver_value, NULL::NUMBER AS gold_value,
           file_rows AS lhs, bronze_rows AS rhs, 'COUNT' AS tol
      FROM counts
    UNION ALL
    SELECT source_table || '.bronze_vs_silver_accounted', NULL, bronze_rows, silver_accounted, NULL,
           bronze_rows, silver_accounted, 'COUNT'
      FROM counts
    UNION ALL
    SELECT 'fact_call_daily.calls_vs_silver', NULL, NULL,
           (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_call WHERE call_date_et BETWEEN :v_from AND :v_to),
           (SELECT COALESCE(SUM(calls_offered), 0) FROM CC_DEMO.GOLD.fact_call_daily WHERE date_key BETWEEN :v_from_key AND :v_to_key),
           NULL, NULL, 'COUNT'
    UNION ALL
    SELECT 'fact_call_daily.handle_seconds_vs_silver', NULL, NULL,
           (SELECT COALESCE(SUM(handle_seconds), 0) FROM CC_DEMO.SILVER.stg_call WHERE call_date_et BETWEEN :v_from AND :v_to),
           (SELECT COALESCE(SUM(handle_seconds_total), 0) FROM CC_DEMO.GOLD.fact_call_daily WHERE date_key BETWEEN :v_from_key AND :v_to_key),
           NULL, NULL, 'SUM'
    UNION ALL
    SELECT 'fact_service_level_interval.rows_vs_silver', NULL, NULL,
           (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_acd_interval WHERE interval_date_et BETWEEN :v_from AND :v_to),
           (SELECT COUNT(*) FROM CC_DEMO.GOLD.fact_service_level_interval WHERE date_key BETWEEN :v_from_key AND :v_to_key),
           NULL, NULL, 'COUNT'
    UNION ALL
    SELECT 'fact_agent_daily.rows_vs_silver', NULL, NULL,
           (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_wfm_daily WHERE work_date BETWEEN :v_from AND :v_to),
           (SELECT COUNT(*) FROM CC_DEMO.GOLD.fact_agent_daily WHERE date_key BETWEEN :v_from_key AND :v_to_key),
           NULL, NULL, 'COUNT'
    UNION ALL
    SELECT 'fact_repeat_call.rows_vs_fact_call_daily', NULL, NULL,
           (SELECT COALESCE(SUM(repeat_calls), 0) FROM CC_DEMO.GOLD.fact_call_daily WHERE date_key BETWEEN :v_from_key AND :v_to_key),
           (SELECT COUNT(*) FROM CC_DEMO.GOLD.fact_repeat_call WHERE call_date_key BETWEEN :v_from_key AND :v_to_key),
           NULL, NULL, 'COUNT';

    -- Pass/fail per row. A SUM passes when the difference is within 0.01% of the left side.
    CREATE OR REPLACE TEMPORARY TABLE tmp_rec_status AS
    SELECT r.*,
           IFF(r.tol = 'SUM',
               ABS(COALESCE(r.lhs, -1) - COALESCE(r.rhs, -2)) <= 0.0001 * ABS(COALESCE(r.lhs, 0)),
               COALESCE(r.lhs, -1) = COALESCE(r.rhs, -2))                          AS passed
      FROM tmp_rec r;

    SELECT COUNT(*), COUNT_IF(NOT passed) INTO :v_checks, :v_failed FROM tmp_rec_status;

    BEGIN TRANSACTION;

    DELETE FROM CC_DEMO.CONTROL.reconciliation WHERE batch_id = :p_batch_id;

    INSERT INTO CC_DEMO.CONTROL.reconciliation
        (batch_id, source_table, metric, source_value, bronze_value, silver_value, gold_value, status, checked_ts)
    SELECT :p_batch_id,
           IFF(CONTAINS(metric, '.'), SPLIT_PART(metric, '.', 1), 'GOLD'),
           metric,
           source_value, bronze_value, silver_value, gold_value,
           IFF(passed, 'PASS', 'FAIL'),
           :v_started
      FROM tmp_rec_status;

    COMMIT;

    RETURN OBJECT_CONSTRUCT(
        'batch_id', p_batch_id,
        'source_table', 'RECONCILIATION',
        'status', IFF(v_failed = 0, 'PASS', 'FAIL'),
        'rows_in', v_checks,
        'rows_loaded', v_checks - v_failed,
        'rows_quarantined', 0,
        'dq_errors', v_failed,
        'dq_warnings', 0,
        'failed_checks', v_failed,
        'message', IFF(v_failed = 0, 'all ' || v_checks || ' checks passed',
                       v_failed || ' of ' || v_checks || ' checks failed; see CONTROL.reconciliation'));

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        INSERT INTO CC_DEMO.CONTROL.load_log (batch_id, source_table, source_file, rows_read, rows_loaded,
                                              rows_skipped_duplicate_batch, status, error_message, started_ts, ended_ts)
        SELECT :p_batch_id, 'RECONCILIATION', NULL, 0, 0, 0, 'FAILED', SQLERRM, :v_started,
               CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ;
        RAISE;
END;
$$;
