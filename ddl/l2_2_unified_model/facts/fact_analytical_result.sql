CREATE OR REPLACE TABLE l2_2_unified_model.fact_analytical_result
(
    analytical_result_key       BIGINT          NOT NULL    COMMENT 'Surrogate key',
    batch_key                   BIGINT          NOT NULL    COMMENT 'FK to dim_batch',
    spec_key                    BIGINT                      COMMENT 'FK to dim_specification',
    spec_item_key               BIGINT                      COMMENT 'FK to dim_specification_item',
    condition_key               INT                         COMMENT 'FK to dim_stability_condition',
    timepoint_key               INT                         COMMENT 'FK to dim_timepoint',
    instrument_key              BIGINT                      COMMENT 'FK to dim_instrument',
    laboratory_key              BIGINT                      COMMENT 'FK to dim_laboratory',
    uom_key                     INT                         COMMENT 'FK to dim_uom',
    test_date_key               INT                         COMMENT 'FK to dim_date (test date)',

    -- Measures
    result_value                DECIMAL(18, 6)              COMMENT 'Numeric test result value',
    result_text                 STRING                      COMMENT 'Text result (non-numeric tests)',
    result_status_code          STRING          NOT NULL    COMMENT 'Status: PASS|FAIL|OOS|OOT|PENDING|REPORT',
    percent_label_claim         DECIMAL(18, 6)              COMMENT 'Result as percent of label claim (standard pharma reporting)',

    -- Reported limits (vendor-provided for comparison)
    reported_lower_limit        DECIMAL(18, 6)              COMMENT 'Vendor-reported lower limit',
    reported_upper_limit        DECIMAL(18, 6)              COMMENT 'Vendor-reported upper limit',
    reported_target             DECIMAL(18, 6)              COMMENT 'Vendor-reported target',

    -- Derived OOS/OOT flags
    is_oos                      BOOLEAN                     COMMENT 'TRUE if result is Out of Specification per ICH Q7',
    is_oot                      BOOLEAN                     COMMENT 'TRUE if result is Out of Trend per FDA guidance',

    -- Sample context
    sample_type                 STRING                      COMMENT 'Type: RELEASE|STABILITY|IPC|INVESTIGATIONAL|REFERENCE',
    replicate_number            INT                         COMMENT 'Replicate number (1, 2, 3, ...)',

    -- Personnel
    analyst_name                STRING                      COMMENT 'Analyst who performed the test',
    reviewer_name               STRING                      COMMENT 'Reviewer who approved the result',

    -- Report
    lab_name                    STRING                      COMMENT 'Laboratory name',
    report_id                   STRING                      COMMENT 'Analytical report / CoA ID',
    coa_number                  STRING                      COMMENT 'Certificate of Analysis number',
    stability_study_id          STRING                      COMMENT 'Stability study identifier per ICH Q1A',

    -- Lineage
    source_result_id            STRING                      COMMENT 'Source system natural key',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Current version flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (result_status_code)
COMMENT 'L2.2 Fact: Analytical test results. Grain = batch x test x condition x time point.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'analytical_results',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'fact'
);
