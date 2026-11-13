-- =============================================================================
-- 03_gold.sql  (Snowflake)  -- GOLD layer: star schema for analytics
-- =============================================================================
-- Purpose : a star schema built only from CC_DEMO.SILVER (never from bronze).
-- Keys    : surrogate keys are *_key. Dimensions use AUTOINCREMENT; date uses the
--           integer yyyymmdd form (date_key). Primary and foreign keys are
--           informational only (not enforced).
--
-- Dimensions : dim_date, dim_skill, dim_media_type, dim_disposition,
--              dim_agent (view over dim_agent_scd2, current version).
--              dim_customer_type is NOT created: Five9 does not supply a customer
--              type field in these exports (see the assumptions list).
-- Facts      : fact_call_daily, fact_service_level_interval,
--              fact_agent_daily (ASSUMPTION: depends on the WFM extract),
--              fact_repeat_call.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- CC_DEMO.GOLD.dim_date
--   Grain   : one row per calendar date.
--   Source  : generated calendar (no source table).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.GOLD.dim_date (
    date_key NUMBER(8,0) NOT NULL,              -- yyyymmdd
    calendar_date DATE NOT NULL,
    year_num NUMBER(4,0),
    quarter_num NUMBER(1,0),
    month_num NUMBER(2,0),
    month_name VARCHAR(16),
    day_of_month NUMBER(2,0),
    day_of_week_num NUMBER(1,0),                -- 1 = Monday, 7 = Sunday (ISO)
    day_name VARCHAR(16),
    week_of_year NUMBER(2,0),
    is_weekend BOOLEAN NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);

-- -----------------------------------------------------------------------------
-- CC_DEMO.GOLD.dim_skill
--   Grain   : one row per skill.
--   Source  : SILVER.stg_acd_interval and SILVER.stg_call (skill, campaign,
--             soa_threshold_sec).
--   Notes   : soa_threshold_sec is the current threshold for the skill. Thresholds
--             vary by skill (20, 30, 60 in the demo data). If a threshold changes
--             over time, convert this to a type 2 dimension.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.GOLD.dim_skill (
    skill_key NUMBER(38,0) AUTOINCREMENT,
    skill_name VARCHAR(64) NOT NULL,            -- SKILL
    campaign_name VARCHAR(128),                 -- CAMPAIGN (one per skill in the demo data)
    soa_threshold_sec NUMBER(6,0),              -- SOA MIN/THRESHOLD (SEC)
    CONSTRAINT pk_dim_skill PRIMARY KEY (skill_key),
    CONSTRAINT uq_dim_skill_name UNIQUE (skill_name)
);

-- -----------------------------------------------------------------------------
-- CC_DEMO.GOLD.dim_media_type
--   Grain   : one row per MEDIA TYPE value.
--   Source  : SILVER.stg_call.channel (from MEDIA TYPE).
--   Notes   : web_form and secure_message are not channels (CLAUDE.md). The
--             demo data has only 'Voice'; the value list is an assumption until
--             the client confirms the MEDIA TYPE values.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.GOLD.dim_media_type (
    media_type_key NUMBER(38,0) AUTOINCREMENT,
    media_type VARCHAR(32) NOT NULL,            -- MEDIA TYPE
    CONSTRAINT pk_dim_media_type PRIMARY KEY (media_type_key),
    CONSTRAINT uq_dim_media_type UNIQUE (media_type)
);

-- -----------------------------------------------------------------------------
-- CC_DEMO.GOLD.dim_disposition
--   Grain   : one row per distinct disposition path (the full hierarchy).
--   Source  : SILVER.stg_call (DISPOSITION, DISPOSITION GROUP A/B/C, DISPOSITION PATH).
--   Notes   : replaces the earlier contact_reason_code. The hierarchy is
--             group A > group B > group C > disposition, as DISPOSITION PATH.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.GOLD.dim_disposition (
    disposition_key NUMBER(38,0) AUTOINCREMENT,
    disposition VARCHAR(128) NOT NULL,          -- DISPOSITION
    disposition_group_a VARCHAR(128),           -- DISPOSITION GROUP A
    disposition_group_b VARCHAR(128),           -- DISPOSITION GROUP B
    disposition_group_c VARCHAR(128),           -- DISPOSITION GROUP C
    disposition_path VARCHAR(512) NOT NULL,     -- DISPOSITION PATH
    CONSTRAINT pk_dim_disposition PRIMARY KEY (disposition_key),
    CONSTRAINT uq_dim_disposition_path UNIQUE (disposition_path)
);

-- -----------------------------------------------------------------------------
-- CC_DEMO.GOLD.dim_agent   (VIEW: current version of dim_agent_scd2)
--   Grain   : one row per agent (current version only).
--   Source  : SILVER.dim_agent_scd2 where is_current.
--   Notes   : effective dates are the WFM/call-derived assumption (see silver).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW CC_DEMO.GOLD.dim_agent AS
SELECT
    agent_key,
    agent_id,
    agent_group,
    agent_first_name,
    agent_last_name,
    agent_email,
    effective_start_date,
    in_wfm_flag
FROM CC_DEMO.SILVER.dim_agent_scd2
WHERE is_current;

-- -----------------------------------------------------------------------------
-- CC_DEMO.GOLD.fact_call_daily
--   Grain   : one row per calendar date, skill, media type, and disposition.
--   Source  : SILVER.stg_call (aggregated), with repeat_calls from fact_repeat_call.
--   Measures: calls_offered (all calls), calls_answered (contacted = 1),
--             calls_abandoned, handle_seconds_total, repeat_calls.
--   Clustering: date_key. Nearly every report is a date range; clustering keeps
--             range scans to the matching partitions. Cardinality of the other
--             keys is low, so date is the only useful cluster key.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.GOLD.fact_call_daily (
    date_key NUMBER(8,0) NOT NULL,              -- FK dim_date
    skill_key NUMBER(38,0) NOT NULL,            -- FK dim_skill
    media_type_key NUMBER(38,0) NOT NULL,       -- FK dim_media_type
    disposition_key NUMBER(38,0) NOT NULL,      -- FK dim_disposition
    calls_offered NUMBER(12,0) NOT NULL,
    calls_answered NUMBER(12,0) NOT NULL,
    calls_abandoned NUMBER(12,0) NOT NULL,
    handle_seconds_total NUMBER(14,0) NOT NULL,
    repeat_calls NUMBER(12,0) NOT NULL,
    CONSTRAINT pk_fact_call_daily PRIMARY KEY (date_key, skill_key, media_type_key, disposition_key)
)
CLUSTER BY (date_key);

-- -----------------------------------------------------------------------------
-- CC_DEMO.GOLD.fact_service_level_interval
--   Grain   : one row per skill and 30-minute interval (America/New_York).
--             campaign_name is an attribute (one campaign per skill in the demo data).
--   Source  : SILVER.stg_acd_interval.
--   Notes   : service_level_pct is the Five9 SERVICE LEVEL value, kept as received.
--             It is not recalculated. A FAIL interval (dq_status = 'FAIL') has a NULL
--             service_level_pct.
--   Clustering: date_key. Service-level reports are read by day and by week. Within
--             a day the interval is the sort order that matters, so date is the
--             cluster key.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.GOLD.fact_service_level_interval (
    date_key NUMBER(8,0) NOT NULL,              -- FK dim_date (interval_date_et)
    interval_start_et TIMESTAMP_NTZ(0) NOT NULL,
    skill_key NUMBER(38,0) NOT NULL,            -- FK dim_skill
    campaign_name VARCHAR(128),
    calls_offered NUMBER(10,0),
    calls_answered NUMBER(10,0),
    calls_abandoned NUMBER(10,0),
    service_level_pct NUMBER(6,1),              -- SERVICE LEVEL (source value)
    soa_threshold_sec NUMBER(6,0),              -- SOA MIN/THRESHOLD (SEC) for this interval
    queue_wait_seconds NUMBER(12,0),
    handle_seconds NUMBER(12,0),
    dq_status VARCHAR(8) NOT NULL,
    CONSTRAINT pk_fact_service_level_interval PRIMARY KEY (skill_key, interval_start_et)
)
CLUSTER BY (date_key);

-- -----------------------------------------------------------------------------
-- CC_DEMO.GOLD.fact_agent_daily   (ASSUMPTION: depends on the WFM extract)
--   Grain   : one row per agent and day.
--   Source  : SILVER.stg_wfm_daily (schedule, login, adherence) and SILVER.stg_call
--             (handled_calls = calls with a non-null agent_id on that date).
--   Notes   : agent_key is resolved from dim_agent_scd2 on work_date (the version
--             in effect that day). Because effective dates are assumed (see
--             silver), this resolution inherits that assumption.
--   Clustering: date_key. Workforce reports are read by day.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.GOLD.fact_agent_daily (
    date_key NUMBER(8,0) NOT NULL,              -- FK dim_date
    agent_key NUMBER(38,0) NOT NULL,            -- FK dim_agent_scd2 (version in effect)
    agent_id VARCHAR(64) NOT NULL,
    scheduled_minutes NUMBER(6,0),
    logged_in_minutes NUMBER(6,0),
    adherence_pct NUMBER(5,1),
    handled_calls NUMBER(12,0),
    CONSTRAINT pk_fact_agent_daily PRIMARY KEY (date_key, agent_id)
)
CLUSTER BY (date_key);

-- -----------------------------------------------------------------------------
-- CC_DEMO.GOLD.fact_repeat_call
--   Grain   : one row per call that is a repeat contact. A repeat is a call with
--             the same customer_key as an earlier call within 7 calendar days
--             (CLAUDE.md).
--   Source  : SILVER.stg_call, joined to the earlier call on customer_key.
--   Notes   : prior_contact_id is the most recent earlier call for that key within
--             7 days. days_since_prior_call is the calendar-day difference.
--             customer_key is an HMAC token, not a raw ANI or SALESFORCE_ID.
--   Clustering: call_date_key. Repeat-rate reports are read by date.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.GOLD.fact_repeat_call (
    repeat_contact_id VARCHAR(64) NOT NULL,     -- the repeat call (FK stg_call.contact_id)
    prior_contact_id VARCHAR(64) NOT NULL,      -- the earlier call with the same key
    customer_key VARCHAR(64) NOT NULL,
    call_date_key NUMBER(8,0) NOT NULL,         -- FK dim_date (repeat call date)
    prior_call_date_key NUMBER(8,0) NOT NULL,   -- FK dim_date (earlier call date)
    days_since_prior_call NUMBER(3,0) NOT NULL, -- 0 to 7
    skill_key NUMBER(38,0) NOT NULL,            -- FK dim_skill (repeat call skill)
    media_type_key NUMBER(38,0) NOT NULL,       -- FK dim_media_type
    prior_disposition_key NUMBER(38,0),         -- FK dim_disposition for the EARLIER call (added for Q3)
    CONSTRAINT pk_fact_repeat_call PRIMARY KEY (repeat_contact_id)
)
CLUSTER BY (call_date_key);
