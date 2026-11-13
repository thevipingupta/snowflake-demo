-- =============================================================================
-- 06_sp_run_pipeline.sql  (Snowflake Scripting)
-- =============================================================================
-- CC_DEMO.CONTROL.sp_run_pipeline(p_batch_id, p_max_quarantine_rate) RETURNS VARIANT
--
-- Runs one batch through every layer, in dependency order:
--   bronze: WFM_DAILY, API_ACD_ALL, API_CALLLOG, API_CALL_SEGMENTREPORTALL,
--           API_VOICEMAIL_LIFECYCLE_LOG
--   silver: the same five sources, in the same order (WFM first, because DQ006
--           checks agents against it; calls before segments and voicemail, because
--           those check against stg_call)
--   agent SCD2, gold, reconciliation
--
-- Stops and returns status FAILED with the failing step when:
--   * a step raises an error (the step has already written its FAILED row to load_log),
--   * a step returns FAILED,
--   * reconciliation returns FAIL,
--   * a silver step rejects more than p_max_quarantine_rate of its rows (default 5%).
--
-- Quarantined rows alone do not stop the run (the brief's error-severity rules
-- quarantine rows). Such a run returns SUCCEEDED_WITH_QUARANTINE.
--
-- Each CALL is followed by RESULT_SCAN(LAST_QUERY_ID()) to read the JSON it returned.
--
-- Re-running a batch: bronze is skipped (already loaded), silver MERGEs, gold MERGEs,
-- and reconciliation is rewritten. Row counts do not change.
--
-- Returns: {batch_id, source_table = 'PIPELINE', status, rows_in, rows_loaded,
--           rows_quarantined, dq_errors, dq_warnings, failed_step, message, steps}
-- =============================================================================

CREATE OR REPLACE PROCEDURE CC_DEMO.CONTROL.sp_run_pipeline(p_batch_id VARCHAR, p_max_quarantine_rate FLOAT)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_step      VARCHAR DEFAULT 'start';
    v_res       VARIANT;
    v_steps     ARRAY DEFAULT ARRAY_CONSTRUCT();
    v_rows_in   NUMBER DEFAULT 0;
    v_rows_load NUMBER DEFAULT 0;
    v_quar      NUMBER DEFAULT 0;
    v_dq_err    NUMBER DEFAULT 0;
    v_dq_warn   NUMBER DEFAULT 0;
    v_status    VARCHAR;
    v_rate      FLOAT;
BEGIN
    -- ---- bronze -----------------------------------------------------------
    v_step := 'bronze WFM_DAILY';
    CALL CC_DEMO.CONTROL.sp_load_bronze('WFM_DAILY', :p_batch_id);
    SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('step', v_step, 'status', v_res:status::VARCHAR));
    IF (v_res:status::VARCHAR = 'FAILED') THEN
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'PIPELINE', 'status', 'FAILED',
            'failed_step', v_step, 'message', 'bronze step failed; see CONTROL.load_log', 'steps', v_steps);
    END IF;

    v_step := 'bronze API_ACD_ALL';
    CALL CC_DEMO.CONTROL.sp_load_bronze('API_ACD_ALL', :p_batch_id);
    SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('step', v_step, 'status', v_res:status::VARCHAR));
    IF (v_res:status::VARCHAR = 'FAILED') THEN
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'PIPELINE', 'status', 'FAILED',
            'failed_step', v_step, 'message', 'bronze step failed; see CONTROL.load_log', 'steps', v_steps);
    END IF;

    v_step := 'bronze API_CALLLOG';
    CALL CC_DEMO.CONTROL.sp_load_bronze('API_CALLLOG', :p_batch_id);
    SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('step', v_step, 'status', v_res:status::VARCHAR));
    IF (v_res:status::VARCHAR = 'FAILED') THEN
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'PIPELINE', 'status', 'FAILED',
            'failed_step', v_step, 'message', 'bronze step failed; see CONTROL.load_log', 'steps', v_steps);
    END IF;

    v_step := 'bronze API_CALL_SEGMENTREPORTALL';
    CALL CC_DEMO.CONTROL.sp_load_bronze('API_CALL_SEGMENTREPORTALL', :p_batch_id);
    SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('step', v_step, 'status', v_res:status::VARCHAR));
    IF (v_res:status::VARCHAR = 'FAILED') THEN
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'PIPELINE', 'status', 'FAILED',
            'failed_step', v_step, 'message', 'bronze step failed; see CONTROL.load_log', 'steps', v_steps);
    END IF;

    v_step := 'bronze API_VOICEMAIL_LIFECYCLE_LOG';
    CALL CC_DEMO.CONTROL.sp_load_bronze('API_VOICEMAIL_LIFECYCLE_LOG', :p_batch_id);
    SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('step', v_step, 'status', v_res:status::VARCHAR));
    IF (v_res:status::VARCHAR = 'FAILED') THEN
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'PIPELINE', 'status', 'FAILED',
            'failed_step', v_step, 'message', 'bronze step failed; see CONTROL.load_log', 'steps', v_steps);
    END IF;

    -- ---- silver -----------------------------------------------------------
    v_step := 'silver WFM_DAILY';
    CALL CC_DEMO.CONTROL.sp_load_silver('WFM_DAILY', :p_batch_id);
    SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('step', v_step, 'status', v_res:status::VARCHAR, 'rows_quarantined', v_res:rows_quarantined));
    v_rows_in := v_rows_in + COALESCE(v_res:rows_in::NUMBER, 0);
    v_rows_load := v_rows_load + COALESCE(v_res:rows_loaded::NUMBER, 0);
    v_quar := v_quar + COALESCE(v_res:rows_quarantined::NUMBER, 0);
    v_dq_err := v_dq_err + COALESCE(v_res:dq_errors::NUMBER, 0);
    v_dq_warn := v_dq_warn + COALESCE(v_res:dq_warnings::NUMBER, 0);
    v_rate := COALESCE(v_res:rows_quarantined::FLOAT / NULLIF(v_res:rows_in::FLOAT, 0), 0);
    IF (v_res:status::VARCHAR = 'FAILED' OR v_rate > p_max_quarantine_rate) THEN
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'PIPELINE', 'status', 'FAILED',
            'failed_step', v_step, 'message', 'quarantine rate ' || ROUND(v_rate * 100, 2) || '% exceeds limit or step failed',
            'steps', v_steps);
    END IF;

    v_step := 'silver API_ACD_ALL';
    CALL CC_DEMO.CONTROL.sp_load_silver('API_ACD_ALL', :p_batch_id);
    SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('step', v_step, 'status', v_res:status::VARCHAR, 'rows_quarantined', v_res:rows_quarantined));
    v_rows_in := v_rows_in + COALESCE(v_res:rows_in::NUMBER, 0);
    v_rows_load := v_rows_load + COALESCE(v_res:rows_loaded::NUMBER, 0);
    v_quar := v_quar + COALESCE(v_res:rows_quarantined::NUMBER, 0);
    v_dq_err := v_dq_err + COALESCE(v_res:dq_errors::NUMBER, 0);
    v_dq_warn := v_dq_warn + COALESCE(v_res:dq_warnings::NUMBER, 0);
    v_rate := COALESCE(v_res:rows_quarantined::FLOAT / NULLIF(v_res:rows_in::FLOAT, 0), 0);
    IF (v_res:status::VARCHAR = 'FAILED' OR v_rate > p_max_quarantine_rate) THEN
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'PIPELINE', 'status', 'FAILED',
            'failed_step', v_step, 'message', 'quarantine rate ' || ROUND(v_rate * 100, 2) || '% exceeds limit or step failed',
            'steps', v_steps);
    END IF;

    v_step := 'silver API_CALLLOG';
    CALL CC_DEMO.CONTROL.sp_load_silver('API_CALLLOG', :p_batch_id);
    SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('step', v_step, 'status', v_res:status::VARCHAR, 'rows_quarantined', v_res:rows_quarantined));
    v_rows_in := v_rows_in + COALESCE(v_res:rows_in::NUMBER, 0);
    v_rows_load := v_rows_load + COALESCE(v_res:rows_loaded::NUMBER, 0);
    v_quar := v_quar + COALESCE(v_res:rows_quarantined::NUMBER, 0);
    v_dq_err := v_dq_err + COALESCE(v_res:dq_errors::NUMBER, 0);
    v_dq_warn := v_dq_warn + COALESCE(v_res:dq_warnings::NUMBER, 0);
    v_rate := COALESCE(v_res:rows_quarantined::FLOAT / NULLIF(v_res:rows_in::FLOAT, 0), 0);
    IF (v_res:status::VARCHAR = 'FAILED' OR v_rate > p_max_quarantine_rate) THEN
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'PIPELINE', 'status', 'FAILED',
            'failed_step', v_step, 'message', 'quarantine rate ' || ROUND(v_rate * 100, 2) || '% exceeds limit or step failed',
            'steps', v_steps);
    END IF;

    v_step := 'silver API_CALL_SEGMENTREPORTALL';
    CALL CC_DEMO.CONTROL.sp_load_silver('API_CALL_SEGMENTREPORTALL', :p_batch_id);
    SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('step', v_step, 'status', v_res:status::VARCHAR, 'rows_quarantined', v_res:rows_quarantined));
    v_rows_in := v_rows_in + COALESCE(v_res:rows_in::NUMBER, 0);
    v_rows_load := v_rows_load + COALESCE(v_res:rows_loaded::NUMBER, 0);
    v_quar := v_quar + COALESCE(v_res:rows_quarantined::NUMBER, 0);
    v_dq_err := v_dq_err + COALESCE(v_res:dq_errors::NUMBER, 0);
    v_dq_warn := v_dq_warn + COALESCE(v_res:dq_warnings::NUMBER, 0);
    v_rate := COALESCE(v_res:rows_quarantined::FLOAT / NULLIF(v_res:rows_in::FLOAT, 0), 0);
    IF (v_res:status::VARCHAR = 'FAILED' OR v_rate > p_max_quarantine_rate) THEN
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'PIPELINE', 'status', 'FAILED',
            'failed_step', v_step, 'message', 'quarantine rate ' || ROUND(v_rate * 100, 2) || '% exceeds limit or step failed',
            'steps', v_steps);
    END IF;

    v_step := 'silver API_VOICEMAIL_LIFECYCLE_LOG';
    CALL CC_DEMO.CONTROL.sp_load_silver('API_VOICEMAIL_LIFECYCLE_LOG', :p_batch_id);
    SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('step', v_step, 'status', v_res:status::VARCHAR, 'rows_quarantined', v_res:rows_quarantined));
    v_rows_in := v_rows_in + COALESCE(v_res:rows_in::NUMBER, 0);
    v_rows_load := v_rows_load + COALESCE(v_res:rows_loaded::NUMBER, 0);
    v_quar := v_quar + COALESCE(v_res:rows_quarantined::NUMBER, 0);
    v_dq_err := v_dq_err + COALESCE(v_res:dq_errors::NUMBER, 0);
    v_dq_warn := v_dq_warn + COALESCE(v_res:dq_warnings::NUMBER, 0);
    v_rate := COALESCE(v_res:rows_quarantined::FLOAT / NULLIF(v_res:rows_in::FLOAT, 0), 0);
    IF (v_res:status::VARCHAR = 'FAILED' OR v_rate > p_max_quarantine_rate) THEN
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'PIPELINE', 'status', 'FAILED',
            'failed_step', v_step, 'message', 'quarantine rate ' || ROUND(v_rate * 100, 2) || '% exceeds limit or step failed',
            'steps', v_steps);
    END IF;

    -- ---- agent SCD2, gold, reconciliation ---------------------------------
    v_step := 'dim_agent_scd2';
    CALL CC_DEMO.CONTROL.sp_load_agent_scd2(:p_batch_id);
    SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('step', v_step, 'status', v_res:status::VARCHAR));
    IF (v_res:status::VARCHAR = 'FAILED') THEN
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'PIPELINE', 'status', 'FAILED',
            'failed_step', v_step, 'message', v_res:message::VARCHAR, 'steps', v_steps);
    END IF;

    v_step := 'gold';
    CALL CC_DEMO.CONTROL.sp_load_gold(:p_batch_id);
    SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('step', v_step, 'status', v_res:status::VARCHAR));
    IF (v_res:status::VARCHAR = 'FAILED') THEN
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'PIPELINE', 'status', 'FAILED',
            'failed_step', v_step, 'message', 'gold step failed; see CONTROL.load_log', 'steps', v_steps);
    END IF;

    v_step := 'reconcile';
    CALL CC_DEMO.CONTROL.sp_reconcile(:p_batch_id);
    SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('step', v_step, 'status', v_res:status::VARCHAR,
                                                     'failed_checks', v_res:failed_checks));
    IF (v_res:status::VARCHAR <> 'PASS') THEN
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'PIPELINE', 'status', 'FAILED',
            'failed_step', v_step, 'message', v_res:message::VARCHAR, 'steps', v_steps);
    END IF;

    v_status := IFF(v_quar > 0, 'SUCCEEDED_WITH_QUARANTINE', 'SUCCEEDED');
    RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'PIPELINE', 'status', v_status,
        'rows_in', v_rows_in, 'rows_loaded', v_rows_load, 'rows_quarantined', v_quar,
        'dq_errors', v_dq_err, 'dq_warnings', v_dq_warn, 'failed_step', NULL,
        'message', 'pipeline complete', 'steps', v_steps);

EXCEPTION
    WHEN OTHER THEN
        -- The failing procedure has already written its FAILED row to load_log.
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'PIPELINE', 'status', 'FAILED',
            'failed_step', v_step, 'message', 'step raised an error: ' || SQLERRM, 'steps', v_steps);
END;
$$;
