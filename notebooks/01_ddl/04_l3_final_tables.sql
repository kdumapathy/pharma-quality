-- Databricks notebook source
-- MAGIC %md
-- MAGIC # L3 Final Data Products — Pharmaceutical Quality (PQ/CMC)
-- MAGIC Creates the One Big Table (OBT) final data products for CTD submissions, acceptance criteria, and stability analysis.
-- MAGIC Aligned with ICH Q6A/Q6B, ICH Q1A(R2), CTD Module 3, and GMP data standards.
-- MAGIC
-- MAGIC **Tables:**
-- MAGIC - `obt_specification_ctd` — CTD-ready specification output (one row per spec × item × limit)
-- MAGIC - `obt_acceptance_criteria` — Acceptance criteria with limit hierarchy metrics (one row per spec × item)
-- MAGIC - `obt_stability_results` — Stability analytical results (one row per batch × test × condition × time point)

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

USE SCHEMA l3_data_product;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### obt_specification_ctd
-- MAGIC CTD-ready One Big Table. Fully denormalized, ready for regulatory submission document generation.
-- MAGIC Grain: one row per specification × item × limit type × stage.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l3_data_product.obt_specification_ctd
(
    obt_ctd_key                 BIGINT          NOT NULL    COMMENT 'Surrogate key',

    -- Specification header
    spec_key                    BIGINT          NOT NULL    COMMENT 'FK to L2.2 dim_specification',
    spec_number                 STRING          NOT NULL    COMMENT 'Specification number',
    spec_version                STRING          NOT NULL    COMMENT 'Specification version',
    spec_title                  STRING                      COMMENT 'Specification title',
    spec_type_code              STRING          NOT NULL    COMMENT 'Type: DS|DP|RM|EXCIP|INTERMED|IPC|CCS',
    spec_type_name              STRING                      COMMENT 'Type display name',
    status_code                 STRING          NOT NULL    COMMENT 'Status: DRA|APP|SUP|OBS|ARCH',
    stage_code                  STRING                      COMMENT 'Stage: DEV|CLI|COM',
    stage_name                  STRING                      COMMENT 'Stage display name',

    -- Product (PQ/CMC)
    product_name                STRING                      COMMENT 'Product name',
    inn_name                    STRING                      COMMENT 'International Nonproprietary Name (WHO INN)',
    brand_name                  STRING                      COMMENT 'Brand / trade name',
    dosage_form_code            STRING                      COMMENT 'Dosage form code',
    dosage_form_name            STRING                      COMMENT 'Dosage form display name',
    route_of_administration     STRING                      COMMENT 'Route of administration',
    strength                    STRING                      COMMENT 'Strength string',
    nda_number                  STRING                      COMMENT 'NDA/ANDA/BLA/MAA registration number',

    -- Material (CMC)
    material_name               STRING                      COMMENT 'Material name',
    material_type_code          STRING                      COMMENT 'Material type code',
    cas_number                  STRING                      COMMENT 'CAS Registry Number',

    -- Site (GMP)
    site_code                   STRING                      COMMENT 'Site code',
    site_name                   STRING                      COMMENT 'Site name',
    site_regulatory_region      STRING                      COMMENT 'Regulatory region',

    -- Market
    region_code                 STRING                      COMMENT 'Market region code',
    market_country_code         STRING                      COMMENT 'Market country code',
    market_country_name         STRING                      COMMENT 'Market country name',
    market_status               STRING                      COMMENT 'Marketing authorization status',

    -- Test / Item
    test_name                   STRING          NOT NULL    COMMENT 'Test name',
    test_code                   STRING                      COMMENT 'Test code',
    test_category_code          STRING                      COMMENT 'Category: PHY|CHE|IMP|MIC|BIO|STER|PACK',
    test_category_name          STRING                      COMMENT 'Category display name',
    criticality                 STRING                      COMMENT 'CQA classification: CQA|CCQA|NCQA|KQA|REPORT',
    sequence_number             INT                         COMMENT 'Display order',
    is_required                 BOOLEAN                     COMMENT 'Mandatory flag',
    reporting_type              STRING                      COMMENT 'NUMERIC|PASS_FAIL|TEXT|REPORT_ONLY',
    compendia_reference         STRING                      COMMENT 'Compendia reference for specification',
    compendia_test_ref          STRING                      COMMENT 'Compendia test reference (USP <621>, EP 2.9.3)',

    -- Method
    test_method_name            STRING                      COMMENT 'Test method name',
    test_method_number          STRING                      COMMENT 'Method document number',

    -- Limit
    limit_type_code             STRING                      COMMENT 'Limit type code',
    limit_type_name             STRING                      COMMENT 'Limit type name',
    lower_limit_value           DECIMAL(18, 6)              COMMENT 'Lower limit',
    upper_limit_value           DECIMAL(18, 6)              COMMENT 'Upper limit',
    target_value                DECIMAL(18, 6)              COMMENT 'Target value',
    limit_description           STRING                      COMMENT 'Formatted limit expression',
    limit_text                  STRING                      COMMENT 'Non-numeric limit text',
    uom_code                    STRING                      COMMENT 'Unit of measure',
    uom_name                    STRING                      COMMENT 'Unit display name',
    limit_basis                 STRING                      COMMENT 'Basis: AS_IS|ANHYDROUS|AS_LABELED|DRIED_BASIS',

    -- Stability context
    stability_time_point        STRING                      COMMENT 'Time point: T0|T3M|T6M|T12M|T24M|T36M',
    stability_condition         STRING                      COMMENT 'Condition: 25C60RH|40C75RH|REFRIG',

    -- Regulatory
    ctd_section                 STRING                      COMMENT 'CTD section reference',
    is_in_filing                BOOLEAN                     COMMENT 'Appears in regulatory filing',
    regulatory_basis            STRING                      COMMENT 'Regulatory basis',

    -- Dates
    effective_start_date        DATE                        COMMENT 'Limit effective start date',
    effective_end_date          DATE                        COMMENT 'Limit effective end date',
    approval_date               DATE                        COMMENT 'Specification approval date',
    approver_name               STRING                      COMMENT 'Approving authority name',

    -- Metadata
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Current version flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (spec_type_code)
COMMENT 'L3 OBT: CTD-ready specification data product. Grain = spec × item × limit type × stage.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L3',
    'quality.table_type'                = 'obt'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### obt_acceptance_criteria
-- MAGIC Acceptance criteria OBT with pivoted limits and hierarchy metrics.
-- MAGIC Grain: one row per specification × item (with AC, NOR, PAR limits pivoted).

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l3_data_product.obt_acceptance_criteria
(
    obt_ac_key                  BIGINT          NOT NULL    COMMENT 'Surrogate key',

    -- Specification
    spec_number                 STRING          NOT NULL    COMMENT 'Specification number',
    spec_version                STRING          NOT NULL    COMMENT 'Specification version',
    spec_type_code              STRING          NOT NULL    COMMENT 'Type code',
    status_code                 STRING          NOT NULL    COMMENT 'Status code',
    stage_code                  STRING                      COMMENT 'Stage code',

    -- Product & Material
    product_name                STRING                      COMMENT 'Product name',
    material_name               STRING                      COMMENT 'Material name',
    dosage_form                 STRING                      COMMENT 'Dosage form',
    strength                    STRING                      COMMENT 'Strength',

    -- Test / Item
    test_name                   STRING          NOT NULL    COMMENT 'Test name',
    test_code                   STRING                      COMMENT 'Test code',
    test_category_code          STRING                      COMMENT 'Test category',
    criticality                 STRING                      COMMENT 'CQA classification',
    uom_code                    STRING                      COMMENT 'Unit of measure',
    sequence_number             INT                         COMMENT 'Display order',
    reporting_type              STRING                      COMMENT 'Reporting type',
    is_required                 BOOLEAN                     COMMENT 'Mandatory flag',

    -- AC limits (pivoted)
    ac_lower_limit              DECIMAL(18, 6)              COMMENT 'Acceptance criteria lower limit',
    ac_upper_limit              DECIMAL(18, 6)              COMMENT 'Acceptance criteria upper limit',
    ac_target_value             DECIMAL(18, 6)              COMMENT 'AC target value',
    ac_width                    DECIMAL(18, 6)              COMMENT 'AC range width (upper - lower)',

    -- NOR limits (pivoted)
    nor_lower_limit             DECIMAL(18, 6)              COMMENT 'Normal operating range lower limit',
    nor_upper_limit             DECIMAL(18, 6)              COMMENT 'Normal operating range upper limit',
    nor_target_value            DECIMAL(18, 6)              COMMENT 'NOR target value',
    nor_width                   DECIMAL(18, 6)              COMMENT 'NOR range width',

    -- PAR limits (pivoted)
    par_lower_limit             DECIMAL(18, 6)              COMMENT 'Proven acceptable range lower limit',
    par_upper_limit             DECIMAL(18, 6)              COMMENT 'Proven acceptable range upper limit',
    par_target_value            DECIMAL(18, 6)              COMMENT 'PAR target value',
    par_width                   DECIMAL(18, 6)              COMMENT 'PAR range width',

    -- Hierarchy metrics
    nor_tightness_pct           DECIMAL(8, 4)               COMMENT 'NOR width / AC width × 100 (tightness %)',
    par_vs_ac_factor            DECIMAL(8, 4)               COMMENT 'PAR width / AC width (ratio)',
    is_hierarchy_valid          BOOLEAN                     COMMENT 'TRUE if PAR >= AC >= NOR holds',

    -- Metadata
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Current version flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (spec_type_code)
COMMENT 'L3 OBT: Acceptance criteria with pivoted limits and hierarchy metrics. Grain = spec × item.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L3',
    'quality.table_type'                = 'obt'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### obt_stability_results
-- MAGIC Stability analytical results OBT. Fully denormalized, ready for trend analysis and reporting.
-- MAGIC Grain: one row per batch × test × stability condition × time point.

-- COMMAND ----------

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
COMMENT 'L3 OBT: Stability analytical results. Grain = batch × test × condition × time point.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'analytical_results',
    'quality.layer'                     = 'L3',
    'quality.table_type'                = 'obt'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify L3 Tables

-- COMMAND ----------

SHOW TABLES IN l3_data_product;
