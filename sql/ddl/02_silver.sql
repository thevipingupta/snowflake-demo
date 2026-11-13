-- =============================================================================
-- 02_silver.sql  (Snowflake)  -- SILVER layer: typed, standardized, masked
-- =============================================================================
-- Purpose : typed and standardized tables built from CC_DEMO.BRONZE, with
--           snake_case names from docs/source_column_map.csv. Quarantined
--           records go to stg_quarantine_record, with a reason code.
-- Source  : CC_DEMO.BRONZE.raw_* (see 01_bronze.sql).
--
-- Conventions (CLAUDE.md):
--   * Timestamps are TIMESTAMP_NTZ. Columns ending _et hold America/New_York
--     wall-clock time; _utc holds UTC. Bronze timestamps are UTC (assumed).
--   * Durations are parsed to whole seconds (NUMBER). Source values are either
--     seconds ("123") or "HH:MM:SS" (TIME-typed sources). See parse_duration_seconds.
--   * PII (CLAUDE.md list) is masked to the literal '***' in silver. The
--     customer_key is computed from raw ANI / SALESFORCE_ID BEFORE masking, so
--     it still matches across calls. The HMAC secret is not in this file.
--   * Quarantined rows: a row that fails a silver rule is written to
--     stg_quarantine_record with reason_code and its original payload. The
--     silver table then keeps only rows that pass, except where a row is kept
--     with a NULL value and a dq_status (see stg_acd_interval).
--   * Snowflake PRIMARY KEY and FOREIGN KEY constraints are informational only
--     (not enforced). They document the grain and the join keys.
--   * Name note: the column map gives "timestamp" for the TIMESTAMP column.
--     That is a reserved-style name in Snowflake, so silver uses event_ts_utc
--     and event_ts_et instead.
--
-- Table list (silver names follow the brief; the quarantine table is prefixed
-- stg_ to follow the CLAUDE.md naming rule, see the assumptions list):
--   stg_acd_interval          one row per skill, campaign, 30-minute interval
--   stg_call                  one row per CONTACT ID (deduplicated)
--   stg_call_segment          one row per CALL SEGMENT ID
--   stg_voicemail             one row per voicemail event
--   stg_wfm_daily             one row per work_date and agent_id   (ASSUMPTION source)
--   dim_agent_scd2            type 2 agent dimension                (ASSUMPTION dates)
--   stg_quarantine_record     rejected rows with reason code
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Duration parser. Accepts "123", "-37", or "HH:MM:SS". Returns NULL for blank
-- or unparseable input. Used by every silver load for duration columns.
-- Defined as a SQL function (not a procedure), so it uses CREATE OR REPLACE.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION CC_DEMO.SILVER.parse_duration_seconds(v VARCHAR)
RETURNS NUMBER(12,0)
LANGUAGE SQL
AS
$$
    CASE
        WHEN v IS NULL OR TRIM(v) = '' THEN NULL
        WHEN CONTAINS(v, ':') THEN
            TRY_TO_NUMBER(SPLIT_PART(v, ':', 1)) * 3600
          + TRY_TO_NUMBER(SPLIT_PART(v, ':', 2)) * 60
          + TRY_TO_NUMBER(SPLIT_PART(v, ':', 3))
        ELSE TRY_TO_NUMBER(v)
    END
$$;

-- -----------------------------------------------------------------------------
-- CC_DEMO.SILVER.stg_acd_interval
--   Grain   : one row per skill, campaign, and 30-minute interval (America/New_York).
--   Source  : BRONZE.raw_api_acd_all. DATE + TIME INTERVAL give the interval start,
--             interpreted as America/New_York local time.
--   Notes   : service_level_pct is the Five9 SERVICE LEVEL value, as received.
--             It is NOT recalculated. soa_threshold_sec is SOA MIN/THRESHOLD (SEC),
--             which varies by skill.
--             A SERVICE LEVEL outside 0-100 is kept with service_level_pct = NULL,
--             dq_status = 'FAIL', and a matching stg_quarantine_record row, so
--             row counts still reconcile to bronze.
--             first_call_resolution_flag and queue_overflow_flag are NULL for
--             files that predate their addition (2026-04-29).
-- Clustering: interval_date_et. Queries about a window of days filter on the
--             date, so micro-partitions prune by date. Rows arrive by batch date,
--             which keeps the clustering stable.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.SILVER.stg_acd_interval (
    skill VARCHAR(64) NOT NULL,                 -- SKILL
    campaign VARCHAR(128) NOT NULL,             -- CAMPAIGN
    interval_start_et TIMESTAMP_NTZ(0) NOT NULL,   -- DATE + TIME INTERVAL, America/New_York
    interval_start_utc TIMESTAMP_NTZ(0) NOT NULL,  -- converted from interval_start_et
    interval_date_et DATE NOT NULL,
    calls_offered NUMBER(10,0),                 -- CALLS
    calls_answered NUMBER(10,0),                -- derived: offered - abandoned (ACD answered count)
    calls_abandoned NUMBER(10,0),               -- ABANDONED
    service_level_pct NUMBER(6,1),              -- SERVICE LEVEL (source value, 0-100)
    soa_threshold_sec NUMBER(6,0),              -- SOA MIN/THRESHOLD (SEC), per skill
    queue_wait_seconds NUMBER(12,0),            -- QUEUE WAIT TIME
    total_queue_seconds NUMBER(12,0),           -- TOTAL QUEUE TIME
    handle_seconds NUMBER(12,0),                -- HANDLE TIME
    talk_seconds NUMBER(12,0),                  -- TALK TIME
    hold_seconds NUMBER(12,0),                  -- HOLD TIME
    after_call_work_seconds NUMBER(12,0),       -- AFTER CALL WORK TIME
    ring_seconds NUMBER(12,0),                  -- RING TIME
    speed_of_answer_seconds NUMBER(12,0),       -- SPEED OF ANSWER
    time_to_abandon_seconds NUMBER(12,0),       -- TIME TO ABANDON
    first_call_resolution_flag NUMBER(1,0),     -- FIRST CALL RESOLUTION (added 2026-04-29)
    queue_overflow_flag NUMBER(1,0),            -- QUEUE OVERFLOW (added 2026-04-29)
    media_type VARCHAR(32),                     -- MEDIA TYPE
    dq_status VARCHAR(8) NOT NULL,              -- PASS | WARN | FAIL
    five9_file_name VARCHAR,                    -- FILE_NAME (Five9 metadata)
    five9_load_timestamp TIMESTAMP_NTZ,         -- LOAD_TIMESTAMP (Five9 metadata)
    _batch_id VARCHAR,
    _source_file VARCHAR,
    _load_ts TIMESTAMP_NTZ,
    _record_hash VARCHAR,
    CONSTRAINT pk_stg_acd_interval PRIMARY KEY (skill, campaign, interval_start_et)
)
CLUSTER BY (interval_date_et);

-- -----------------------------------------------------------------------------
-- CC_DEMO.SILVER.stg_call
--   Grain   : one row per CONTACT ID, after deduplication (keep one copy of
--             identical duplicates from bronze).
--   Source  : BRONZE.raw_api_calllog.
--   Notes   : channel is MEDIA TYPE. web_form and secure_message are not valid
--             channels (CLAUDE.md); rows with them are quarantined.
--             customer_key = HMAC-SHA256(secret, COALESCE(salesforce_id, ani)),
--             computed before masking. NULL if both are NULL.
--             Negative HANDLE TIME rows are quarantined, not loaded.
--             agent_in_wfm_flag = 0 when agent_id has no WFM row for the call date.
--             The row is kept, so the call still counts.
--             Masked fields hold '***' (ani, customer_name, first_name, last_name,
--             email, street, city, zip, agent_email, agent_first_name, agent_last_name).
--             salesforce_id is kept (it is the key input) but is marked REVIEW in
--             the column map.
--             call_id is UNIQUE, so stg_call_segment and stg_voicemail can join on it.
-- Clustering: call_date_et. Most queries filter by call date. The table grows by
--             about one batch per day, so date clustering keeps it pruned.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.SILVER.stg_call (
    contact_id VARCHAR(64) NOT NULL,            -- CONTACT ID (dedupe key)
    call_id VARCHAR(64) NOT NULL,               -- CALL ID (join key to segments)
    call_date_et DATE NOT NULL,                 -- derived from call_start_et
    call_start_utc TIMESTAMP_NTZ(0),            -- TIMESTAMP (UTC, assumed)
    call_start_et TIMESTAMP_NTZ(0),             -- converted to America/New_York
    channel VARCHAR(32),                        -- MEDIA TYPE
    skill VARCHAR(64),                          -- SKILL
    campaign VARCHAR(128),                      -- CAMPAIGN
    call_type VARCHAR(32),                      -- CALL TYPE
    agent_id VARCHAR(64),                       -- AGENT ID (AGENT.ID is not used)
    agent_group VARCHAR(64),                    -- AGENT GROUP
    agent_in_wfm_flag NUMBER(1,0),              -- 1 if agent_id is in the WFM extract
    disposition VARCHAR(128),                   -- DISPOSITION
    disposition_group_a VARCHAR(128),           -- DISPOSITION GROUP A
    disposition_group_b VARCHAR(128),           -- DISPOSITION GROUP B
    disposition_group_c VARCHAR(128),           -- DISPOSITION GROUP C
    disposition_path VARCHAR(512),              -- DISPOSITION PATH
    abandoned_flag NUMBER(1,0),                 -- ABANDONED
    contacted_flag NUMBER(1,0),                 -- CONTACTED
    service_level_flag NUMBER(1,0),             -- SERVICE LEVEL (per call, 0/1)
    soa_threshold_sec NUMBER(6,0),              -- SOA MIN/THRESHOLD (SEC)
    handle_seconds NUMBER(12,0),                -- HANDLE TIME (negative values quarantined)
    hold_seconds NUMBER(12,0),                  -- HOLD TIME
    talk_seconds NUMBER(12,0),                  -- TALK TIME
    after_call_work_seconds NUMBER(12,0),       -- AFTER CALL WORK TIME
    ring_seconds NUMBER(12,0),                  -- RING TIME
    queue_wait_seconds NUMBER(12,0),            -- QUEUE WAIT TIME
    time_to_abandon_seconds NUMBER(12,0),       -- TIME TO ABANDON
    first_call_resolution_flag NUMBER(1,0),     -- FIRST CALL RESOLUTION
    queue_overflow_flag NUMBER(1,0),            -- QUEUE OVERFLOW
    customer_key VARCHAR(64),                   -- HMAC of COALESCE(salesforce_id, ani)
    ani VARCHAR(16),                            -- ANI: masked '***'
    customer_name VARCHAR(16),                  -- CUSTOMER NAME: masked
    first_name VARCHAR(16),                     -- FIRST_NAME: masked
    last_name VARCHAR(16),                      -- LAST_NAME: masked
    email VARCHAR(16),                          -- EMAIL: masked
    street VARCHAR(16),                         -- STREET: masked
    city VARCHAR(16),                           -- CITY: masked
    zip VARCHAR(16),                            -- ZIP: masked
    salesforce_id VARCHAR(64),                  -- SALESFORCE_ID (REVIEW: kept, used as key input)
    agent_email VARCHAR(16),                    -- AGENT EMAIL: masked
    agent_first_name VARCHAR(16),               -- AGENT FIRST NAME: masked
    agent_last_name VARCHAR(16),                -- AGENT LAST NAME: masked
    five9_file_name VARCHAR,                    -- FILE_NAME (Five9 metadata)
    five9_load_timestamp TIMESTAMP_NTZ,         -- LOAD_TIMESTAMP (Five9 metadata)
    _batch_id VARCHAR,
    _source_file VARCHAR,
    _load_ts TIMESTAMP_NTZ,
    _record_hash VARCHAR,
    CONSTRAINT pk_stg_call PRIMARY KEY (contact_id),
    CONSTRAINT uq_stg_call_call_id UNIQUE (call_id)
)
CLUSTER BY (call_date_et);

-- -----------------------------------------------------------------------------
-- CC_DEMO.SILVER.stg_call_segment
--   Grain   : one row per CALL SEGMENT ID.
--   Source  : BRONZE.raw_api_call_segment.
--   Notes   : call_id must exist in stg_call. Segments whose CALL ID has no
--             matching call are quarantined (reason ORPHAN_CALL_ID), not loaded.
--             The FK is informational; the orphan check enforces it.
-- Clustering: segment_date_et. Segment reports are read by date, and clustering on
--             the date keeps those reads to the relevant partitions.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.SILVER.stg_call_segment (
    call_segment_id VARCHAR(80) NOT NULL,       -- CALL SEGMENT ID
    call_id VARCHAR(64) NOT NULL,               -- CALL ID (FK to stg_call.call_id)
    contact_id VARCHAR(64),                     -- CONTACT ID
    segment_date_et DATE NOT NULL,              -- derived from segment_start_et
    segment_start_utc TIMESTAMP_NTZ(0),         -- TIMESTAMP (UTC, assumed)
    segment_start_et TIMESTAMP_NTZ(0),
    segment_type VARCHAR(32),                   -- SEGMENT TYPE (QUEUE | AGENT | IVR)
    result VARCHAR(32),                         -- RESULT
    channel VARCHAR(32),                        -- MEDIA TYPE
    skill VARCHAR(64),                          -- SKILL
    agent_id VARCHAR(64),                       -- AGENT (segment agent; AGENT ID is the call-level field)
    disposition_group_a VARCHAR(128),           -- DISPOSITION GROUP A
    segment_seconds NUMBER(12,0),               -- SEGMENT TIME
    queue_wait_seconds NUMBER(12,0),            -- QUEUE WAIT TIME
    handle_seconds NUMBER(12,0),                -- HANDLE TIME
    talk_seconds NUMBER(12,0),                  -- TALK TIME
    hold_seconds NUMBER(12,0),                  -- HOLD TIME
    ivr_seconds NUMBER(12,0),                   -- IVR TIME
    abandoned_flag NUMBER(1,0),                 -- ABANDONED
    service_level_flag NUMBER(1,0),             -- SERVICE LEVEL
    five9_file_name VARCHAR,
    five9_load_timestamp TIMESTAMP_NTZ,
    _batch_id VARCHAR,
    _source_file VARCHAR,
    _load_ts TIMESTAMP_NTZ,
    _record_hash VARCHAR,
    CONSTRAINT pk_stg_call_segment PRIMARY KEY (call_segment_id),
    CONSTRAINT fk_stg_call_segment_call FOREIGN KEY (call_id) REFERENCES CC_DEMO.SILVER.stg_call (call_id)
)
CLUSTER BY (segment_date_et);

-- -----------------------------------------------------------------------------
-- CC_DEMO.SILVER.stg_voicemail
--   Grain   : one row per VOICEMAIL ID.
--   Source  : BRONZE.raw_api_voicemail_lifecycle.
--   Notes   : contact_id links to stg_call.contact_id (informational FK). Voicemail
--             rows whose contact_id has no call are quarantined (ORPHAN_CONTACT_ID).
--             PII is masked the same way as stg_call.
--             TIME-typed source columns are parsed to seconds by parse_duration_seconds.
-- Clustering: received_date_et. Voicemail work queues are read by received date.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.SILVER.stg_voicemail (
    voicemail_id VARCHAR(64) NOT NULL,          -- VOICEMAIL ID
    call_id VARCHAR(64),                        -- CALL ID (original call)
    contact_id VARCHAR(64),                     -- CONTACT ID (FK to stg_call.contact_id)
    received_date_et DATE NOT NULL,             -- RECEIVED DATE (America/New_York)
    received_ts_utc TIMESTAMP_NTZ(0),           -- RECEIVED TIMESTAMP (UTC, assumed)
    received_ts_et TIMESTAMP_NTZ(0),
    voicemail_status VARCHAR(32),               -- VOICEMAIL STATUS
    voicemail_type VARCHAR(32),                 -- VOICEMAIL TYPE
    skill VARCHAR(64),                          -- SKILL
    processed_flag NUMBER(1,0),                 -- PROCESSED
    listened_flag NUMBER(1,0),                  -- LISTENED
    deleted_flag NUMBER(1,0),                   -- DELETED
    returned_call_id VARCHAR(64),               -- RETURNED CALL ID
    returned_ts_utc TIMESTAMP_NTZ(0),           -- RETURNED TIMESTAMP (UTC)
    time_to_process_seconds NUMBER(12,0),       -- TIME TO PROCESSED
    time_to_return_seconds NUMBER(12,0),        -- TIME TO RETURN
    handle_seconds NUMBER(12,0),                -- HANDLE TIME
    queue_wait_seconds NUMBER(12,0),            -- QUEUE WAIT TIME
    customer_key VARCHAR(64),                   -- HMAC of COALESCE(salesforce_id, ani)
    ani VARCHAR(16),                            -- ANI: masked
    customer_name VARCHAR(16),                  -- masked
    email VARCHAR(16),                          -- masked
    street VARCHAR(16),                         -- masked
    city VARCHAR(16),                           -- masked
    zip VARCHAR(16),                            -- masked
    salesforce_id VARCHAR(64),                  -- REVIEW (kept, key input)
    five9_file_name VARCHAR,
    five9_load_timestamp TIMESTAMP_NTZ,
    _batch_id VARCHAR,
    _source_file VARCHAR,
    _load_ts TIMESTAMP_NTZ,
    _record_hash VARCHAR,
    CONSTRAINT pk_stg_voicemail PRIMARY KEY (voicemail_id),
    CONSTRAINT fk_stg_voicemail_call FOREIGN KEY (contact_id) REFERENCES CC_DEMO.SILVER.stg_call (contact_id)
)
CLUSTER BY (received_date_et);

-- -----------------------------------------------------------------------------
-- CC_DEMO.SILVER.stg_wfm_daily   (ASSUMPTION: WFM extract is not a Five9 source)
--   Grain   : one row per work_date and agent_id.
--   Source  : BRONZE.raw_wfm_daily. The file's '#' comment lines are skipped on load.
--   Notes   : adherence_pct is NULL on OFF days (scheduled_minutes = 0). The
--             WFM agent list is the reference for stg_call.agent_in_wfm_flag.
-- Clustering: work_date. Daily schedule reads are by date.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.SILVER.stg_wfm_daily (
    work_date DATE NOT NULL,                    -- work_date
    agent_id VARCHAR(64) NOT NULL,              -- agent_id
    scheduled_minutes NUMBER(6,0),              -- scheduled_minutes
    logged_in_minutes NUMBER(6,0),              -- logged_in_minutes
    adherence_pct NUMBER(5,1),                  -- adherence_pct
    _batch_id VARCHAR,
    _source_file VARCHAR,
    _load_ts TIMESTAMP_NTZ,
    _record_hash VARCHAR,
    CONSTRAINT pk_stg_wfm_daily PRIMARY KEY (work_date, agent_id)
)
CLUSTER BY (work_date);

-- -----------------------------------------------------------------------------
-- CC_DEMO.SILVER.dim_agent_scd2   (type 2; ASSUMPTION on effective dates)
--   Grain   : one row per agent_id per version. The business key is agent_id.
--   Source  : agent fields in BRONZE.raw_api_calllog (AGENT ID, AGENT GROUP,
--             AGENT FIRST NAME, AGENT LAST NAME, AGENT EMAIL) and the agent list
--             in BRONZE.raw_wfm_daily.
--   ASSUMPTION: the workbook has no effective-date fields for agents. effective_start_date
--             is the first date an agent_id appears in calls or WFM for a given group.
--             A change in AGENT GROUP starts a new version. effective_end_date is
--             9999-12-31 for the current version. Replace this with real effective
--             dates when the client provides them.
--   Notes   : agent names and email are masked ('***' or a fixed masked token).
--             agent_name (REVIEW in the column map) is not stored here.
-- Clustering: none. This is a small dimension (tens of rows).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.SILVER.dim_agent_scd2 (
    agent_key NUMBER(38,0) AUTOINCREMENT,       -- surrogate key
    agent_id VARCHAR(64) NOT NULL,              -- business key
    agent_group VARCHAR(64),                    -- AGENT GROUP
    agent_first_name VARCHAR(16),               -- masked
    agent_last_name VARCHAR(16),                -- masked
    agent_email VARCHAR(16),                    -- masked
    effective_start_date DATE NOT NULL,         -- ASSUMPTION (see header)
    effective_end_date DATE NOT NULL,           -- 9999-12-31 for the current version
    is_current BOOLEAN NOT NULL,
    in_wfm_flag NUMBER(1,0),                    -- 1 if the agent is in the WFM extract
    row_hash VARCHAR(64),                       -- hash of the tracked attributes
    _batch_id VARCHAR,
    _load_ts TIMESTAMP_NTZ,
    CONSTRAINT pk_dim_agent_scd2 PRIMARY KEY (agent_key)
);

-- -----------------------------------------------------------------------------
-- CC_DEMO.SILVER.stg_quarantine_record
--   Purpose : rows rejected by a silver rule, with the reason and the original payload.
--   Naming  : the brief said quarantine_record. The CLAUDE.md rule needs a stg_
--             prefix for silver tables, so this table uses stg_quarantine_record.
--   Grain   : one row per rejected source record.
--   Reason codes (from ISSUE_RULES in the generator and CLAUDE.md):
--     NEGATIVE_HANDLE_TIME, ORPHAN_CALL_ID, ORPHAN_CONTACT_ID,
--     SERVICE_LEVEL_OUT_OF_RANGE, UNSUPPORTED_CHANNEL, DUPLICATE_CONTACT_ID,
--     UNMASKED_PII (should be 0; a hit here is a load failure).
-- Clustering: created_ts. The quarantine is reviewed by load time.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.SILVER.stg_quarantine_record (
    quarantine_id NUMBER(38,0) AUTOINCREMENT,
    source_table VARCHAR(128) NOT NULL,         -- e.g. raw_api_calllog
    batch_id VARCHAR(16),
    record_key VARCHAR(256),                    -- CONTACT ID, CALL ID, VOICEMAIL ID, etc.
    reason_code VARCHAR(64) NOT NULL,
    reason_detail VARCHAR(4000),
    original_payload VARIANT,                   -- the source row as received
    created_ts TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT pk_stg_quarantine_record PRIMARY KEY (quarantine_id)
)
CLUSTER BY (created_ts);
