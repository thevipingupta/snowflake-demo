-- =============================================================================
-- 01_sp_load_bronze.sql  (Snowflake Scripting)
-- =============================================================================
-- CC_DEMO.CONTROL.sp_load_bronze(p_source_table, p_batch_id) RETURNS VARIANT
--
-- Loads one batch (one or more daily files that match the batch id) from the
-- stage into the matching BRONZE raw table.
--
-- Steps:
--   1. Skip if this source and batch already has a SUCCEEDED row in load_log.
--   2. Read the header line of every matching file. All files in a batch must
--      share one header; otherwise the load fails.
--   3. Land the rows positionally in a temporary table (c_1 .. c_n), so
--      duplicate or unusual header names cannot break the read.
--   4. Insert into the raw table by name, with metadata:
--        _batch_id, _source_file (file name), _load_ts (UTC),
--        _record_hash (SHA-256 of the values as received).
--      Columns in the file but not in the table go to _extra_payload.
--      Columns in the table but not in the file load as NULL.
--   5. Drift: log new or missing columns to CONTROL.schema_drift_log, then
--      save this batch's header to CONTROL.batch_header for the next batch.
--
-- Transaction: DDL (temporary tables) auto-commits in Snowflake, so only the
-- data writes and the control writes run in the explicit transaction.
-- On error: roll back, write a FAILED row to load_log, and re-raise.
--
-- Returns: {batch_id, source_table, status, rows_in, rows_loaded,
--           rows_quarantined, dq_errors, dq_warnings, message}
-- Note: bronze has no quarantine; rows_quarantined and dq_errors are 0 here.
-- Parameters are p_source_table and p_batch_id (the brief's names, with a p_ prefix
-- so they do not clash with column names in the control tables).
-- =============================================================================

CREATE OR REPLACE PROCEDURE CC_DEMO.CONTROL.sp_load_bronze(p_source_table VARCHAR, p_batch_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    e_bad_batch_id EXCEPTION (-20101, 'batch_id must look like B001');
    e_no_files     EXCEPTION (-20102, 'no file in the stage matches this source and batch');
    e_header       EXCEPTION (-20103, 'files in this batch do not share one header');
    v_raw          VARCHAR;
    v_folder       VARCHAR;
    v_prefix       VARCHAR;
    v_ff           VARCHAR;
    v_hdr_row      NUMBER;
    v_pattern      VARCHAR;
    v_sql          VARCHAR;
    v_nfiles       NUMBER;
    v_nhdr         NUMBER;
    v_hdr          VARCHAR;
    v_sel          VARCHAR;
    v_cols         VARCHAR;
    v_vals         VARCHAR;
    v_all          VARCHAR;
    v_extra        VARCHAR;
    v_read         NUMBER;
    v_loaded       NUMBER;
    v_done         NUMBER;
    v_drift        NUMBER;
    v_cur_names    VARIANT;
    v_prev_names   VARIANT;
    v_started      TIMESTAMP_NTZ;
BEGIN
    v_started := CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ;

    IF (NOT REGEXP_LIKE(p_batch_id, '^B[0-9]{3}$')) THEN
        RAISE e_bad_batch_id;
    END IF;

    SELECT raw_table, folder, file_prefix, file_format, header_row
      INTO :v_raw, :v_folder, :v_prefix, :v_ff, :v_hdr_row
      FROM CC_DEMO.CONTROL.source_config
     WHERE source_table = :p_source_table;

    -- 1. Already loaded?
    SELECT COUNT(*) INTO :v_done FROM CC_DEMO.CONTROL.load_log
     WHERE batch_id = :p_batch_id AND source_table = :p_source_table AND status = 'SUCCEEDED';
    IF (v_done > 0) THEN
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', p_source_table,
            'status', 'SKIPPED_ALREADY_LOADED', 'rows_in', 0, 'rows_loaded', 0,
            'rows_quarantined', 0, 'dq_errors', 0, 'dq_warnings', 0,
            'message', 'batch already loaded; not reloaded');
    END IF;

    v_pattern := '.*' || v_folder || v_prefix || '_[0-9]{8}_' || p_batch_id || '[.]csv';

    -- 2. Header of every matching file (one row per file, line number = header row)
    v_sql := 'CREATE OR REPLACE TEMPORARY TABLE tmp_hdr AS '
          || 'SELECT METADATA$FILENAME AS src_file, $1::VARCHAR AS hdr '
          || 'FROM @CC_DEMO.BRONZE.src_stage (FILE_FORMAT => ''CC_DEMO.BRONZE.ff_line'', PATTERN => ''' || v_pattern || ''') '
          || 'WHERE METADATA$FILE_ROW_NUMBER = ' || v_hdr_row;
    EXECUTE IMMEDIATE :v_sql;

    SELECT COUNT(DISTINCT src_file), COUNT(DISTINCT hdr), MAX(hdr)
      INTO :v_nfiles, :v_nhdr, :v_hdr
      FROM tmp_hdr;
    IF (v_nfiles = 0) THEN
        RAISE e_no_files;
    END IF;
    IF (v_nhdr <> 1) THEN
        RAISE e_header;
    END IF;

    -- Header positions, and bronze names. Occurrence 1 keeps the name; occurrence N > 1
    -- becomes name__N. The map has no duplicates today, so this is a safeguard.
    CREATE OR REPLACE TEMPORARY TABLE tmp_cols AS
    -- CR is removed: the generator writes CRLF line endings, so the last name would end in CR.
    SELECT f.index + 1 AS pos, TRIM(REPLACE(f.value::VARCHAR, CHR(13), '')) AS src_name
      FROM TABLE(FLATTEN(INPUT => SPLIT(:v_hdr, ','))) f;

    CREATE OR REPLACE TEMPORARY TABLE tmp_cols2 AS
    SELECT pos, src_name,
           CASE WHEN ROW_NUMBER() OVER (PARTITION BY src_name ORDER BY pos) = 1 THEN src_name
                ELSE src_name || '__' || ROW_NUMBER() OVER (PARTITION BY src_name ORDER BY pos)
           END AS bronze_name
      FROM tmp_cols;

    -- 3. Positional landing table: c_1 .. c_n holds the file's columns in order.
    SELECT 'CREATE OR REPLACE TEMPORARY TABLE tmp_landing ('
           || LISTAGG('c_' || pos || ' VARCHAR', ', ') WITHIN GROUP (ORDER BY pos)
           || ', src_file VARCHAR)',
           LISTAGG('$' || pos, ', ') WITHIN GROUP (ORDER BY pos)
      INTO :v_sql, :v_sel
      FROM tmp_cols2;
    EXECUTE IMMEDIATE :v_sql;

    v_sql := 'COPY INTO tmp_landing FROM (SELECT ' || v_sel || ', METADATA$FILENAME '
          || 'FROM @CC_DEMO.BRONZE.src_stage (FILE_FORMAT => ''CC_DEMO.BRONZE.' || v_ff
          || ''', PATTERN => ''' || v_pattern || ''')) FORCE = TRUE ON_ERROR = ABORT_STATEMENT';
    EXECUTE IMMEDIATE :v_sql;

    SELECT COUNT(*) INTO :v_read FROM tmp_landing;

    -- Map raw table columns (from the catalog) to landing positions.
    CREATE OR REPLACE TEMPORARY TABLE tmp_map AS
    SELECT c.column_name AS raw_col, t.pos
      FROM CC_DEMO.INFORMATION_SCHEMA.COLUMNS c
      LEFT JOIN tmp_cols2 t ON t.bronze_name = c.column_name
     WHERE c.table_schema = 'BRONZE'
       AND c.table_name = UPPER(:v_raw)
       AND UPPER(c.column_name) NOT IN ('_BATCH_ID', '_SOURCE_FILE', '_LOAD_TS', '_RECORD_HASH', '_EXTRA_PAYLOAD');

    -- Columns in the file that the table does not have: these go to _extra_payload.
    CREATE OR REPLACE TEMPORARY TABLE tmp_extra AS
    SELECT t.bronze_name, t.pos
      FROM tmp_cols2 t
     WHERE t.bronze_name NOT IN (SELECT raw_col FROM tmp_map);

    SELECT LISTAGG('"' || raw_col || '"', ', ') WITHIN GROUP (ORDER BY raw_col),
           LISTAGG(IFF(pos IS NULL, 'NULL', 'c_' || pos), ', ') WITHIN GROUP (ORDER BY raw_col)
      INTO :v_cols, :v_vals
      FROM tmp_map;

    SELECT LISTAGG('c_' || pos, ', ') WITHIN GROUP (ORDER BY pos) INTO :v_all FROM tmp_cols2;

    SELECT COALESCE(LISTAGG('''' || bronze_name || ''', c_' || pos, ', ') WITHIN GROUP (ORDER BY pos), '')
      INTO :v_extra
      FROM tmp_extra;

    SELECT ARRAY_AGG(bronze_name) WITHIN GROUP (ORDER BY pos) INTO :v_cur_names FROM tmp_cols2;

    -- 4-5. Data writes and control writes, in one transaction.
    BEGIN TRANSACTION;

    v_sql := 'INSERT INTO CC_DEMO.BRONZE.' || v_raw || ' (' || v_cols
          || ', _batch_id, _source_file, _load_ts, _record_hash, _extra_payload) '
          || 'SELECT ' || v_vals || ', ''' || p_batch_id || ''', src_file, '
          || 'CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ, '
          || 'SHA2(CONCAT_WS(''|'', ' || v_all || '), 256), '
          || 'OBJECT_CONSTRUCT(' || v_extra || ') FROM tmp_landing';
    EXECUTE IMMEDIATE :v_sql;

    SELECT COUNT(*) INTO :v_loaded
      FROM IDENTIFIER('CC_DEMO.BRONZE.' || v_raw)
     WHERE _batch_id = :p_batch_id;

    -- Drift, table level: extra columns (not in table) and missing columns (not in file).
    INSERT INTO CC_DEMO.CONTROL.schema_drift_log (batch_id, source_table, column_name, first_seen_ts, detail)
    SELECT :p_batch_id, :p_source_table, e.bronze_name, :v_started, 'ADDED_TO_PAYLOAD'
      FROM tmp_extra e
     WHERE NOT EXISTS (SELECT 1 FROM CC_DEMO.CONTROL.schema_drift_log d
                        WHERE d.source_table = :p_source_table AND d.column_name = e.bronze_name
                          AND d.detail = 'ADDED_TO_PAYLOAD');

    INSERT INTO CC_DEMO.CONTROL.schema_drift_log (batch_id, source_table, column_name, first_seen_ts, detail)
    SELECT :p_batch_id, :p_source_table, m.raw_col, :v_started, 'MISSING_IN_FILE'
      FROM tmp_map m
     WHERE m.pos IS NULL
       AND NOT EXISTS (SELECT 1 FROM CC_DEMO.CONTROL.schema_drift_log d
                        WHERE d.source_table = :p_source_table AND d.column_name = m.raw_col
                          AND d.detail = 'MISSING_IN_FILE');

    -- Drift, batch level: compare with the previous batch's header.
    v_prev_names := (SELECT header_names FROM CC_DEMO.CONTROL.batch_header
                      WHERE source_table = :p_source_table AND batch_id < :p_batch_id
                      ORDER BY batch_id DESC LIMIT 1);
    IF (v_prev_names IS NOT NULL) THEN
        INSERT INTO CC_DEMO.CONTROL.schema_drift_log (batch_id, source_table, column_name, first_seen_ts, detail)
        SELECT :p_batch_id, :p_source_table, c.value::VARCHAR, :v_started, 'NEW_VS_PRIOR_BATCH'
          FROM TABLE(FLATTEN(INPUT => :v_cur_names)) c
         WHERE c.value::VARCHAR NOT IN (SELECT p.value::VARCHAR FROM TABLE(FLATTEN(INPUT => :v_prev_names)) p)
           AND NOT EXISTS (SELECT 1 FROM CC_DEMO.CONTROL.schema_drift_log d
                            WHERE d.source_table = :p_source_table AND d.column_name = c.value::VARCHAR
                              AND d.detail = 'NEW_VS_PRIOR_BATCH');

        INSERT INTO CC_DEMO.CONTROL.schema_drift_log (batch_id, source_table, column_name, first_seen_ts, detail)
        SELECT :p_batch_id, :p_source_table, p.value::VARCHAR, :v_started, 'MISSING_VS_PRIOR_BATCH'
          FROM TABLE(FLATTEN(INPUT => :v_prev_names)) p
         WHERE p.value::VARCHAR NOT IN (SELECT c.value::VARCHAR FROM TABLE(FLATTEN(INPUT => :v_cur_names)) c)
           AND NOT EXISTS (SELECT 1 FROM CC_DEMO.CONTROL.schema_drift_log d
                            WHERE d.source_table = :p_source_table AND d.column_name = p.value::VARCHAR
                              AND d.detail = 'MISSING_VS_PRIOR_BATCH');
    END IF;

    MERGE INTO CC_DEMO.CONTROL.batch_header t
    USING (SELECT :p_batch_id AS batch_id, :p_source_table AS source_table,
                  :v_cur_names AS header_names, :v_started AS loaded_ts) s
       ON t.batch_id = s.batch_id AND t.source_table = s.source_table
     WHEN MATCHED THEN UPDATE SET header_names = s.header_names, loaded_ts = s.loaded_ts
     WHEN NOT MATCHED THEN INSERT (batch_id, source_table, header_names, loaded_ts)
          VALUES (s.batch_id, s.source_table, s.header_names, s.loaded_ts);

    SELECT COUNT(*) INTO :v_drift FROM CC_DEMO.CONTROL.schema_drift_log
     WHERE batch_id = :p_batch_id AND source_table = :p_source_table AND first_seen_ts = :v_started;

    INSERT INTO CC_DEMO.CONTROL.load_log (batch_id, source_table, source_file, rows_read, rows_loaded,
                                          rows_skipped_duplicate_batch, status, error_message, started_ts, ended_ts)
    SELECT :p_batch_id, :p_source_table, (SELECT LISTAGG(DISTINCT src_file, ',') FROM tmp_landing),
           :v_read, :v_loaded, 0, 'SUCCEEDED', NULL, :v_started,
           CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ;

    COMMIT;

    RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', p_source_table,
        'status', 'SUCCEEDED', 'rows_in', v_read, 'rows_loaded', v_loaded,
        'rows_quarantined', 0, 'dq_errors', 0, 'dq_warnings', v_drift,
        'message', 'loaded; dq_warnings counts new drift entries for this batch');

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        INSERT INTO CC_DEMO.CONTROL.load_log (batch_id, source_table, source_file, rows_read, rows_loaded,
                                              rows_skipped_duplicate_batch, status, error_message, started_ts, ended_ts)
        SELECT :p_batch_id, :p_source_table, NULL, 0, 0, 0, 'FAILED', SQLERRM, :v_started,
               CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ;
        RAISE;
END;
$$;
