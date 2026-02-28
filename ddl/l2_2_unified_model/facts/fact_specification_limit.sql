CREATE TABLE IF NOT EXISTS l2_2_unified_model.fact_specification_limit
(
    spec_limit_key              BIGINT          NOT NULL    COMMENT 'Surrogate key',
    spec_key                    BIGINT          NOT NULL    COMMENT 'FK to dim_specification',
    spec_item_key               BIGINT          NOT NULL    COMMENT 'FK to dim_specification_item',
    limit_type_key              INT             NOT NULL    COMMENT 'FK to dim_limit_type',
    uom_key                     INT                         COMMENT 'FK to dim_uom',
    effective_start_date_key    INT                         COMMENT 'FK to dim_date',
    effective_end_date_key      INT                         COMMENT 'FK to dim_date',

    -- Measures
    lower_limit_value           DECIMAL(18, 6)              COMMENT 'Lower limit numeric value',
    upper_limit_value           DECIMAL(18, 6)              COMMENT 'Upper limit numeric value',
    target_value                DECIMAL(18, 6)              COMMENT 'Target / nominal value',
    limit_range_width           DECIMAL(18, 6)              COMMENT 'Calculated: upper - lower',
    lower_limit_operator        STRING                      COMMENT 'Operator: NLT|GT|GTE|NONE',
    upper_limit_operator        STRING                      COMMENT 'Operator: NMT|LT|LTE|NONE',
    limit_text                  STRING                      COMMENT 'Non-numeric qualitative limit text',
    limit_description           STRING                      COMMENT 'Full formatted limit expression (CTD-ready)',

    -- Context
    limit_basis                 STRING                      COMMENT 'Basis: AS_IS|ANHYDROUS|AS_LABELED|DRIED_BASIS',
    stage_code                  STRING                      COMMENT 'Stage: RELEASE|STABILITY|IPC|BOTH',
    stability_time_point        STRING                      COMMENT 'Time point: T0|T3M|T6M|T12M|T24M|T36M',
    stability_condition         STRING                      COMMENT 'Condition: 25C60RH|30C65RH|40C75RH|REFRIG',

    -- SPC fields (for NOR/PAR/ALERT/ACTION limits)
    calculation_method          STRING                      COMMENT 'SPC method: 3_SIGMA|CPK|EWMA|CUSUM|MANUAL',
    sample_size                 INT                         COMMENT 'SPC sample size',
    last_calculated_date_key    INT                         COMMENT 'FK to dim_date (SPC recalculation)',

    -- Regulatory
    is_in_filing                BOOLEAN                     COMMENT 'TRUE = appears in regulatory filing (CTD)',
    regulatory_basis            STRING                      COMMENT 'Regulatory basis (ICH Q6A, ICH Q3B, USP, etc.)',

    -- Lineage
    source_limit_id             STRING                      COMMENT 'Source system natural key',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Current version flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (stage_code)
COMMENT 'L2.2 Fact: Specification limits per ICH Q6A. Grain = spec x item x limit type x stage.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'fact'
);
