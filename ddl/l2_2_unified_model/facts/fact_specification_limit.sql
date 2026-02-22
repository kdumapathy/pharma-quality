-- =============================================================================
-- Table  : fact_specification_limit
-- Schema : l2_2_spec_unified
-- Layer  : L2.2 — Unified Data Model (Business Conform Layer)
-- Domain : Pharmaceutical Quality — Specifications
-- Grain  : One row per limit type per specification item per stage / time point
-- Ref    : specification_data_model_30-jan.html → SPECIFICATION_LIMIT_SET + CONTROL_LIMITS
-- Changes: v2 — added effective_end_date (ref ERD: SPECIFICATION_LIMIT_SET),
--               merged CONTROL_LIMITS SPC fields: calculation_method,
--               sample_size, last_calculated_date (populated for ALERT/ACTION rows).
-- Purpose: Central fact table of the Specification star schema. Stores ALL limit
--          values in a normalized, single-table structure — Acceptance Criteria
--          (AC), Normal Operating Range (NOR), Proven Acceptable Range (PAR),
--          Alert, Action, and IPC limits coexist in one table differentiated by
--          limit_type_key. Merges SPECIFICATION_LIMIT_SET + CONTROL_LIMITS
--          from ref ERD into one unified limits table. This normalized design supports:
--            - Cross-limit-type comparison (AC vs NOR vs PAR)
--            - Regulatory hierarchy validation (PAR >= AC >= NOR)
--            - Incremental loading of individual limit types
--            - Extension to new limit types without schema changes
--          For pivoted/denormalized output, see dspec_specification (L2.2) or
--          obt_specification_ctd (L3).
-- CTD    : is_in_filing = TRUE rows feed CTD 3.2.S.4.1 / 3.2.P.5.1 tables
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.fact_specification_limit
(
    -- -------------------------------------------------------------------------
    -- Keys
    -- -------------------------------------------------------------------------
    spec_limit_key              BIGINT          NOT NULL    GENERATED ALWAYS AS IDENTITY
                                                            COMMENT 'Surrogate primary key (system-generated)',

    -- Dimension foreign keys
    spec_key                    BIGINT          NOT NULL    COMMENT 'FK to dim_specification (degenerate — also carried for direct query performance)',
    spec_item_key               BIGINT          NOT NULL    COMMENT 'FK to dim_specification_item',
    limit_type_key              BIGINT          NOT NULL    COMMENT 'FK to dim_limit_type (AC|NOR|PAR|ALERT|ACTION|IPC_LIMIT|REPORT|COMPENDIA)',
    uom_key                     BIGINT                      COMMENT 'FK to dim_uom — unit for this specific limit (may differ from test result unit)',

    -- -------------------------------------------------------------------------
    -- Numeric Limit Values
    -- NULL indicates the limit is one-sided (only lower or only upper)
    -- or that the limit is non-numeric (use limit_text)
    -- -------------------------------------------------------------------------
    lower_limit_value           DECIMAL(18, 6)              COMMENT 'Lower bound of the limit (numeric). NULL = no lower bound.',
    upper_limit_value           DECIMAL(18, 6)              COMMENT 'Upper bound of the limit (numeric). NULL = no upper bound.',
    target_value                DECIMAL(18, 6)              COMMENT 'Nominal / target value. May equal the midpoint of lower/upper or be a separate specification.',

    -- -------------------------------------------------------------------------
    -- Limit Operators
    -- Defines the relationship: result [lower_operator] lower_limit_value
    --                                  [upper_operator] upper_limit_value
    --
    -- lower_limit_operator values:
    --   NLT  = Not Less Than (result >= lower_limit_value)          — most common
    --   GT   = Greater Than  (result >  lower_limit_value)          — strictly greater
    --   GTE  = Greater Than or Equal (synonym for NLT)
    --   NONE = No lower limit (one-sided upper only)
    --
    -- upper_limit_operator values:
    --   NMT  = Not More Than (result <= upper_limit_value)          — most common
    --   LT   = Less Than     (result <  upper_limit_value)          — strictly less
    --   LTE  = Less Than or Equal (synonym for NMT)
    --   NONE = No upper limit (one-sided lower only)
    -- -------------------------------------------------------------------------
    lower_limit_operator        STRING                      COMMENT 'Lower bound operator: NLT|GT|GTE|NONE',
    upper_limit_operator        STRING                      COMMENT 'Upper bound operator: NMT|LT|LTE|NONE',

    -- -------------------------------------------------------------------------
    -- Text / Qualitative Limits
    -- Used when reporting_type = PASS_FAIL or TEXT
    -- -------------------------------------------------------------------------
    limit_text                  STRING                      COMMENT 'Non-numeric limit expression (e.g., "Clear, colorless solution", "White or off-white powder")',

    -- -------------------------------------------------------------------------
    -- Formatted Limit Description (CTD-ready expression)
    -- This is the human-readable, fully formatted limit string that appears
    -- verbatim in the specification document / CTD filing table.
    -- Examples:
    --   "NLT 98.0% and NMT 102.0% (on anhydrous basis)"
    --   "NMT 0.1% (any individual impurity); NMT 0.5% (total impurities)"
    --   "Q = 70% in 45 minutes (USP <711>)"
    --   "Passes test"
    -- -------------------------------------------------------------------------
    limit_description           STRING                      COMMENT 'Full formatted limit expression for CTD/specification document rendering',

    -- -------------------------------------------------------------------------
    -- Limit Basis
    -- Defines the denominator / reference basis for the limit value
    -- limit_basis values:
    --   AS_IS          = As received / as-is (no correction)
    --   ANHYDROUS      = Corrected for water content (anhydrous basis)
    --   AS_LABELED     = Based on labeled amount (drug product assay)
    --   DRIED_BASIS    = Corrected for loss on drying
    --   INTACT_PROTEIN = Protein content basis (biologics)
    -- -------------------------------------------------------------------------
    limit_basis                 STRING                      COMMENT 'Limit calculation basis: AS_IS|ANHYDROUS|AS_LABELED|DRIED_BASIS|INTACT_PROTEIN',

    -- -------------------------------------------------------------------------
    -- Applicability — Stage
    -- stage_code values:
    --   RELEASE    = Batch release testing
    --   STABILITY  = Stability study testing
    --   IPC        = In-process control during manufacturing
    --   BOTH       = Both release and stability
    -- -------------------------------------------------------------------------
    stage_code                  STRING          NOT NULL    COMMENT 'Applicable stage: RELEASE|STABILITY|IPC|BOTH',

    -- -------------------------------------------------------------------------
    -- Stability-Specific Attributes (populated when stage_code = STABILITY)
    -- stability_time_point values:
    --   T0, T1M, T3M, T6M, T9M, T12M, T18M, T24M, T36M, T48M, T60M
    --
    -- stability_condition values (ICH Q1A storage conditions):
    --   25C60RH   = Long-term:       25°C ± 2°C / 60% RH ± 5% RH
    --   30C65RH   = Intermediate:    30°C ± 2°C / 65% RH ± 5% RH
    --   40C75RH   = Accelerated:     40°C ± 2°C / 75% RH ± 5% RH
    --   REFRIG    = Refrigerated:    5°C ± 3°C
    --   FREEZE    = Frozen:         -20°C ± 5°C
    --   DEEPFREEZE= Deep freeze:    -80°C ± 10°C
    --   ACCEL     = Accelerated (generic)
    --   PHOTO     = Photostability (ICH Q1B)
    -- -------------------------------------------------------------------------
    stability_time_point        STRING                      COMMENT 'Stability time point: T0|T1M|T3M|T6M|T9M|T12M|T18M|T24M|T36M|T48M|T60M',
    stability_condition         STRING                      COMMENT 'ICH Q1A storage condition: 25C60RH|30C65RH|40C75RH|REFRIG|FREEZE|DEEPFREEZE|PHOTO',

    -- -------------------------------------------------------------------------
    -- Conditional Limits
    -- Some limits apply only under specific conditions (e.g., different limits
    -- for different temperature zones, different sample types)
    -- -------------------------------------------------------------------------
    is_conditional              BOOLEAN                     COMMENT 'TRUE = this limit applies only under the defined condition',
    condition_description       STRING                      COMMENT 'Description of the condition under which this limit applies',

    -- -------------------------------------------------------------------------
    -- Regulatory Filing Attributes
    -- -------------------------------------------------------------------------
    regulatory_basis            STRING                      COMMENT 'Regulatory / compendia basis (e.g., ICH Q6A, USP <621>, EP 2.2.29)',
    is_in_filing                BOOLEAN         NOT NULL    COMMENT 'TRUE = this limit appears in the regulatory submission (CTD) filing',

    -- -------------------------------------------------------------------------
    -- Source System Provenance
    -- -------------------------------------------------------------------------
    source_system_code          STRING          NOT NULL    COMMENT 'Source system identifier: LIMS|SAP|VAULT|MANUAL',
    source_system_id            STRING                      COMMENT 'Original record identifier in source system',

    -- -------------------------------------------------------------------------
    -- SCD / Audit Tracking
    -- -------------------------------------------------------------------------
    -- Limit Effective Date Range (aligned with SPECIFICATION_LIMIT_SET in ref ERD)
    -- Limits can have their own effective window independent of the spec version.
    -- -------------------------------------------------------------------------
    effective_date              DATE                        COMMENT 'Date this limit version became effective (ref ERD: effective_start_date)',
    effective_end_date          DATE                        COMMENT 'Date this limit version expires — NULL = open-ended (ref ERD: effective_end_date)',

    -- -------------------------------------------------------------------------
    -- Statistical Process Control (SPC) Attributes
    -- From ref ERD: CONTROL_LIMITS entity — merged into this table for limit_type_code IN (ALERT, ACTION)
    -- These columns are populated ONLY for ALERT and ACTION limit type rows.
    -- NULL for AC, NOR, PAR, IPC_LIMIT, REPORT rows.
    --
    -- calculation_method values:
    --   3_SIGMA      = Three standard deviation control limits (Shewhart)
    --   CPK          = Process Capability Index-based limits
    --   EWMA         = Exponentially Weighted Moving Average
    --   CUSUM        = Cumulative Sum control chart
    --   USP_PROC_PERF= USP Process Performance criteria
    --   MANUAL       = Manually set limits (not statistically derived)
    -- -------------------------------------------------------------------------
    calculation_method          STRING                      COMMENT 'SPC calculation method (ALERT/ACTION only — ref ERD: CONTROL_LIMITS.calculation_method): 3_SIGMA|CPK|EWMA|CUSUM|MANUAL',
    sample_size                 INT                         COMMENT 'Number of data points used to derive statistical control limits (ref ERD: CONTROL_LIMITS.sample_size)',
    last_calculated_date        DATE                        COMMENT 'Date control limits were last recalculated from historical data (ref ERD: CONTROL_LIMITS.last_calculated_date)',

    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'ETL batch load timestamp (UTC)',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'TRUE = current active limit; FALSE = superseded limit version'
)
USING DELTA
PARTITIONED BY (stage_code)
COMMENT 'L2.2 Specification limit fact table v2. Normalized: one row per limit type per spec item per stage/time point. Merges SPECIFICATION_LIMIT_SET + CONTROL_LIMITS from ref ERD. v2 adds effective_end_date and SPC fields (calculation_method, sample_size, last_calculated_date) for ALERT/ACTION rows.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.grain'                     = 'limit_type_per_spec_item_per_stage_timepoint',
    'quality.model_version'             = '2',
    'quality.ctd_filter'                = 'is_in_filing = TRUE AND is_current = TRUE AND stage_code IN (RELEASE, STABILITY)',
    'quality.source_model'              = 'SPECIFICATION_LIMIT_SET + CONTROL_LIMITS (specification_data_model_30-jan.html)'
);

-- OPTIMIZE l2_2_spec_unified.fact_specification_limit ZORDER BY (spec_item_key, limit_type_key);

-- =============================================================================
-- Key Query Patterns
-- =============================================================================

-- 1. Get all Acceptance Criteria for a specification (for CTD table)
-- SELECT f.*, i.test_name, i.sequence_number, u.uom_code
-- FROM l2_2_spec_unified.fact_specification_limit f
-- JOIN l2_2_spec_unified.dim_specification_item i ON f.spec_item_key = i.spec_item_key
-- JOIN l2_2_spec_unified.dim_limit_type lt ON f.limit_type_key = lt.limit_type_key
-- LEFT JOIN l2_2_spec_unified.dim_uom u ON f.uom_key = u.uom_key
-- WHERE f.spec_key = :spec_key
--   AND lt.limit_type_code = 'AC'
--   AND f.stage_code = 'RELEASE'
--   AND f.is_in_filing = TRUE
--   AND f.is_current = TRUE
-- ORDER BY i.sequence_number;

-- 2. Validate PAR >= AC >= NOR hierarchy
-- SELECT
--     i.test_name,
--     MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value END) AS nor_lower,
--     MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END) AS ac_lower,
--     MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value END) AS par_lower,
--     MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value END) AS nor_upper,
--     MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END) AS ac_upper,
--     MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value END) AS par_upper
-- FROM l2_2_spec_unified.fact_specification_limit f
-- JOIN l2_2_spec_unified.dim_specification_item i ON f.spec_item_key = i.spec_item_key
-- JOIN l2_2_spec_unified.dim_limit_type lt ON f.limit_type_key = lt.limit_type_key
-- WHERE f.spec_key = :spec_key AND f.is_current = TRUE AND f.stage_code = 'RELEASE'
-- GROUP BY i.spec_item_key, i.test_name
-- HAVING nor_lower < ac_lower OR ac_lower < par_lower  -- flag violations
--     OR nor_upper > ac_upper OR ac_upper > par_upper;
