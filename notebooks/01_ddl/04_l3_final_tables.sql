-- Databricks notebook source
-- MAGIC %md
-- MAGIC # L3 Final Data Products — Specifications Domain
-- MAGIC Creates the One Big Table (OBT) final data products for CTD submissions and acceptance criteria analysis.
-- MAGIC
-- MAGIC **Tables:**
-- MAGIC - `obt_specification_ctd` — CTD-ready specification output (one row per spec × item × limit)
-- MAGIC - `obt_acceptance_criteria` — Acceptance criteria with limit hierarchy metrics (one row per spec × item)

-- COMMAND ----------

USE CATALOG pharma_quality;
USE SCHEMA l3_spec_products;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### obt_specification_ctd
-- MAGIC CTD-ready One Big Table. Fully denormalized, ready for regulatory submission document generation.
-- MAGIC Grain: one row per specification × item × limit type × stage.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l3_spec_products.obt_specification_ctd
(
    obt_ctd_key                 BIGINT          NOT NULL    COMMENT 'Surrogate key',

    -- Specification
    spec_number                 STRING          NOT NULL    COMMENT 'Specification number',
    spec_version                STRING          NOT NULL    COMMENT 'Specification version',
    spec_title                  STRING                      COMMENT 'Specification title',
    spec_type_code              STRING          NOT NULL    COMMENT 'Type: DS|DP|RM|EXCIP|INTERMED|IPC|CCS',
    spec_type_name              STRING                      COMMENT 'Type display name',
    status_code                 STRING          NOT NULL    COMMENT 'Status: DRA|APP|SUP|OBS|ARCH',
    stage_code                  STRING                      COMMENT 'Stage: DEV|CLI|COM',

    -- Product & Material
    product_name                STRING                      COMMENT 'Product name',
    product_family              STRING                      COMMENT 'Product family',
    dosage_form                 STRING                      COMMENT 'Dosage form',
    strength                    STRING                      COMMENT 'Strength',
    material_name               STRING                      COMMENT 'Material name',
    material_type               STRING                      COMMENT 'Material type',

    -- Site & Market
    site_name                   STRING                      COMMENT 'Site name',
    country_code                STRING                      COMMENT 'Site country code',
    market_name                 STRING                      COMMENT 'Market name',
    region_code                 STRING                      COMMENT 'Region code',

    -- Test / Item
    test_name                   STRING          NOT NULL    COMMENT 'Test name',
    test_code                   STRING                      COMMENT 'Test code',
    test_category_code          STRING                      COMMENT 'Category: PHY|CHE|IMP|MIC|BIO|STER|PACK',
    test_category_name          STRING                      COMMENT 'Category display name',
    criticality_code            STRING                      COMMENT 'Criticality: CQA|CCQA|NCQA|KQA|REPORT',
    sequence_number             INT                         COMMENT 'Display order',
    reporting_type              STRING                      COMMENT 'NUMERIC|PASS_FAIL|TEXT|REPORT_ONLY',
    is_required                 BOOLEAN                     COMMENT 'Mandatory flag',

    -- Method
    method_name                 STRING                      COMMENT 'Test method name',
    method_number               STRING                      COMMENT 'Method document number',
    technique                   STRING                      COMMENT 'Analytical technique',
    compendia_test_ref          STRING                      COMMENT 'Compendia test reference',

    -- Limit
    limit_type_code             STRING          NOT NULL    COMMENT 'Limit type code',
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

CREATE TABLE IF NOT EXISTS l3_spec_products.obt_acceptance_criteria
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
    criticality_code            STRING                      COMMENT 'Criticality code',
    uom_code                    STRING                      COMMENT 'Unit of measure',
    sequence_number             INT                         COMMENT 'Display order',
    reporting_type              STRING                      COMMENT 'Reporting type',

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
-- MAGIC ### Verify L3 Tables

-- COMMAND ----------

SHOW TABLES IN l3_spec_products;
