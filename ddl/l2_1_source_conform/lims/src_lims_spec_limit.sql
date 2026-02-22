-- =============================================================================
-- Table  : src_lims_spec_limit
-- Schema : l2_1_lims
-- Layer  : L2.1 — Source Conform Layer (LIMS-specific)
-- Source : l1_raw.raw_lims_spec_limit
-- Grain  : One row per LIMS limit record (latest, deduplicated)
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l2_1_lims.src_lims_spec_limit
(
    source_limit_id             STRING          NOT NULL    COMMENT 'LIMS limit natural key',
    source_spec_item_id         STRING          NOT NULL    COMMENT 'LIMS parent spec item ID',
    source_specification_id     STRING          NOT NULL    COMMENT 'LIMS parent spec ID',
    source_batch_id             STRING          NOT NULL    COMMENT 'ETL batch ID',
    source_ingestion_timestamp  TIMESTAMP       NOT NULL    COMMENT 'Ingestion timestamp',
    record_hash                 STRING                      COMMENT 'SHA-256 for CDC',

    -- Mapped limit type (LIMS free-text → standard code in dim_limit_type)
    -- Mapping: Acceptance → AC, NOR → NOR, Proven Acceptable Range → PAR,
    --          Alert → ALERT, Action → ACTION, In-Process → IPC_LIMIT
    limit_type_code             STRING          NOT NULL    COMMENT 'Mapped: AC|NOR|PAR|ALERT|ACTION|IPC_LIMIT|REPORT',

    -- Numeric limits (cast from STRING; NULL if non-numeric or parse failure)
    lower_limit_value           DECIMAL(18, 6)              COMMENT 'Lower limit (cast from LIMS string)',
    upper_limit_value           DECIMAL(18, 6)              COMMENT 'Upper limit (cast from LIMS string)',
    target_value                DECIMAL(18, 6)              COMMENT 'Target value (cast)',

    -- Operator standardization: LIMS stores as free text
    -- Mapping: "Not Less Than" / ">=" → NLT, "Not More Than" / "<=" → NMT
    lower_limit_operator        STRING                      COMMENT 'Standardized lower operator: NLT|GT|GTE|NONE',
    upper_limit_operator        STRING                      COMMENT 'Standardized upper operator: NMT|LT|LTE|NONE',

    limit_text                  STRING                      COMMENT 'Non-numeric limit text (cleansed)',
    limit_description           STRING                      COMMENT 'Full formatted expression (constructed from limits + operators)',
    uom_code                    STRING                      COMMENT 'Unit code (standardized)',
    limit_basis                 STRING                      COMMENT 'Mapped basis: AS_IS|ANHYDROUS|AS_LABELED|DRIED_BASIS',
    stage_code                  STRING                      COMMENT 'Mapped: RELEASE|STABILITY|IPC|BOTH',
    stability_time_point        STRING                      COMMENT 'Standardized time point: T0|T3M|T6M|T12M|T24M|T36M',
    stability_condition         STRING                      COMMENT 'Standardized condition: 25C60RH|40C75RH|REFRIG',

    -- Effective date range (from SPECIFICATION_LIMIT_SET in ref ERD)
    effective_start_date        DATE                        COMMENT 'Limit effective start (cast)',
    effective_end_date          DATE                        COMMENT 'Limit effective end (NULL = open)',

    -- SPC fields (from CONTROL_LIMITS in ref ERD — populated for ALERT/ACTION)
    calculation_method          STRING                      COMMENT 'SPC method: 3_SIGMA|CPK|EWMA|CUSUM|MANUAL',
    sample_size                 INT                         COMMENT 'SPC sample size (cast)',
    last_calculated_date        DATE                        COMMENT 'SPC last recalculation date (cast)',

    is_in_filing                BOOLEAN                     COMMENT 'Regulatory filing flag (cast from LIMS)',
    regulatory_basis            STRING                      COMMENT 'Regulatory basis (cleansed)',

    -- Data quality
    dq_limit_type_mapped        BOOLEAN                     COMMENT 'TRUE if limit_type_code was successfully mapped',
    dq_operator_mapped          BOOLEAN                     COMMENT 'TRUE if operators were successfully parsed',
    dq_numeric_cast_error       BOOLEAN                     COMMENT 'TRUE if any numeric cast failed',
    dq_date_parse_error         BOOLEAN                     COMMENT 'TRUE if any date parse failed',

    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'L2.1 load timestamp',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Latest version flag'
)
USING DELTA
PARTITIONED BY (limit_type_code)
COMMENT 'L2.1 Source conform: LIMS specification limits. Maps LIMS free-text limit types and operators to standard codes. Includes SPC fields for ALERT/ACTION types (from CONTROL_LIMITS in ref ERD).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.1',
    'quality.source'                    = 'LIMS',
    'quality.source_raw_table'          = 'l1_raw.raw_lims_spec_limit'
);
