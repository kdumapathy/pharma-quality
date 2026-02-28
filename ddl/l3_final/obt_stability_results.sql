CREATE TABLE IF NOT EXISTS l3_data_product.obt_stability_results
(
    obt_stab_key                BIGINT          NOT NULL    COMMENT 'Surrogate key',

    -- Batch
    batch_number                STRING          NOT NULL    COMMENT 'Manufacturing batch / lot number',
    batch_type                  STRING                      COMMENT 'Batch type (DEVELOPMENT|PILOT|COMMERCIAL|VALIDATION)',
    manufacturing_date          DATE                        COMMENT 'Batch manufacturing date',
    expiry_date                 DATE                        COMMENT 'Batch expiry date',
    batch_size                  DECIMAL(18, 4)              COMMENT 'Batch size',
    batch_size_unit             STRING                      COMMENT 'Batch size unit',

    -- Product (PQ/CMC)
    product_name                STRING                      COMMENT 'Product name',
    product_family              STRING                      COMMENT 'Product family',
    inn_name                    STRING                      COMMENT 'International Nonproprietary Name',
    dosage_form                 STRING                      COMMENT 'Dosage form',
    strength                    STRING                      COMMENT 'Strength',

    -- Material (CMC)
    material_name               STRING                      COMMENT 'Material name',
    material_type               STRING                      COMMENT 'Material type',

    -- Site (GMP)
    site_name                   STRING                      COMMENT 'Testing site name',
    site_code                   STRING                      COMMENT 'Site code',
    country_code                STRING                      COMMENT 'Site country code',
    lab_name                    STRING                      COMMENT 'Laboratory name',

    -- Specification
    spec_number                 STRING                      COMMENT 'Specification number',
    spec_version                STRING                      COMMENT 'Specification version',
    spec_type_code              STRING                      COMMENT 'Type: DS|DP|RM|EXCIP|INTERMED',

    -- Test / Item
    test_name                   STRING          NOT NULL    COMMENT 'Test name',
    test_code                   STRING                      COMMENT 'Test code',
    test_category_code          STRING                      COMMENT 'Category: PHY|CHE|IMP|MIC|BIO|STER',
    test_category_name          STRING                      COMMENT 'Category display name',
    criticality                 STRING                      COMMENT 'CQA classification',
    method_name                 STRING                      COMMENT 'Test method name',
    technique                   STRING                      COMMENT 'Analytical technique',

    -- Stability Context
    stability_study_id          STRING                      COMMENT 'Stability study identifier',
    storage_condition_code      STRING                      COMMENT 'Condition: 25C60RH|30C65RH|40C75RH|5C',
    storage_condition_name      STRING                      COMMENT 'Condition display name',
    ich_condition_type          STRING                      COMMENT 'ICH type: LONG_TERM|ACCELERATED|INTERMEDIATE',
    time_point_code             STRING                      COMMENT 'Time point: T0|T3M|T6M|T12M|T24M|T36M',
    time_point_months           INT                         COMMENT 'Time point in months',
    time_point_name             STRING                      COMMENT 'Time point display name',

    -- Result
    result_value                DECIMAL(18, 6)              COMMENT 'Numeric test result',
    result_text                 STRING                      COMMENT 'Text result (non-numeric)',
    percent_label_claim         DECIMAL(18, 6)              COMMENT 'Percent of label claim',
    uom_code                    STRING                      COMMENT 'Unit of measure code',
    uom_name                    STRING                      COMMENT 'Unit display name',
    result_status_code          STRING          NOT NULL    COMMENT 'Status: PASS|FAIL|OOS|OOT|PENDING|REPORT',

    -- Reported limits (vendor-provided)
    reported_lower_limit        DECIMAL(18, 6)              COMMENT 'Vendor-reported lower limit',
    reported_upper_limit        DECIMAL(18, 6)              COMMENT 'Vendor-reported upper limit',
    reported_target             DECIMAL(18, 6)              COMMENT 'Vendor-reported target',

    -- Specification limits (from unified model, for comparison)
    spec_ac_lower_limit         DECIMAL(18, 6)              COMMENT 'Specification AC lower limit',
    spec_ac_upper_limit         DECIMAL(18, 6)              COMMENT 'Specification AC upper limit',

    -- Derived flags
    is_oos                      BOOLEAN                     COMMENT 'Out of Specification flag',
    is_oot                      BOOLEAN                     COMMENT 'Out of Trend flag',

    -- Instrument
    instrument_name             STRING                      COMMENT 'Instrument name',

    -- Personnel
    analyst_name                STRING                      COMMENT 'Analyst',
    reviewer_name               STRING                      COMMENT 'Reviewer',

    -- Report
    report_id                   STRING                      COMMENT 'Analytical report ID',
    coa_number                  STRING                      COMMENT 'Certificate of Analysis number',

    -- Dates
    test_date                   DATE                        COMMENT 'Date test was performed',
    pull_date                   DATE                        COMMENT 'Sample pull date',

    -- Metadata
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Current version flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (storage_condition_code)
COMMENT 'L3 OBT: Stability analytical results. Grain = batch x test x condition x time point.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'analytical_results',
    'quality.layer'                     = 'L3',
    'quality.table_type'                = 'obt'
);
