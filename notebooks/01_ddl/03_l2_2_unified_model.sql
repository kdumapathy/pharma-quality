-- Databricks notebook source
-- MAGIC %md
-- MAGIC # L2.2 Unified Data Model — Specifications & Analytical Results
-- MAGIC Creates the star schema and denormalized tables for the pharma quality domain.
-- MAGIC
-- MAGIC **Reference Dimensions:** `dim_date`, `dim_uom`, `dim_limit_type`, `dim_regulatory_context`, `dim_stability_condition`, `dim_timepoint`
-- MAGIC **MDM Dimensions:** `dim_product`, `dim_material`, `dim_test_method`, `dim_site`, `dim_market`
-- MAGIC **Conformed Dimensions:** `dim_specification`, `dim_specification_item`
-- MAGIC **Analytical Dimensions:** `dim_batch`, `dim_instrument`
-- MAGIC **Fact Tables:** `fact_specification_limit`, `fact_analytical_result`
-- MAGIC **Denormalized:** `dspec_specification`

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

USE SCHEMA l2_2_spec_unified;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Reference Dimensions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_date
-- MAGIC Calendar date dimension. Populated by seed notebook.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_date
(
    date_key                    INT             NOT NULL    COMMENT 'Surrogate key (YYYYMMDD integer)',
    full_date                   DATE            NOT NULL    COMMENT 'Calendar date',
    year                        INT             NOT NULL    COMMENT 'Calendar year',
    quarter                     INT             NOT NULL    COMMENT 'Quarter (1-4)',
    month                       INT             NOT NULL    COMMENT 'Month (1-12)',
    month_name                  STRING          NOT NULL    COMMENT 'Month name (January, ...)',
    day_of_month                INT             NOT NULL    COMMENT 'Day of month (1-31)',
    day_of_week                 INT             NOT NULL    COMMENT 'Day of week (1=Mon, 7=Sun)',
    day_name                    STRING          NOT NULL    COMMENT 'Day name (Monday, ...)',
    week_of_year                INT             NOT NULL    COMMENT 'ISO week number',
    is_weekend                  BOOLEAN         NOT NULL    COMMENT 'TRUE if Saturday or Sunday',
    fiscal_year                 INT                         COMMENT 'Fiscal year (configurable offset)',
    fiscal_quarter              INT                         COMMENT 'Fiscal quarter'
)
USING DELTA
COMMENT 'L2.2 Reference: Calendar date dimension.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'reference_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_uom
-- MAGIC Unit of measure reference dimension.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_uom
(
    uom_key                     INT             NOT NULL    COMMENT 'Surrogate key',
    uom_code                    STRING          NOT NULL    COMMENT 'Standardized unit code (mg, %, ppm, CFU, etc.)',
    uom_name                    STRING          NOT NULL    COMMENT 'Display name (milligrams, percent, etc.)',
    uom_category                STRING          NOT NULL    COMMENT 'Category: MASS|CONCENTRATION|COUNT|RATIO|LENGTH|VOLUME|OTHER',
    si_conversion_factor        DECIMAL(18, 10)             COMMENT 'Factor to convert to SI base unit',
    si_base_unit                STRING                      COMMENT 'SI base unit code',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag'
)
USING DELTA
COMMENT 'L2.2 Reference: Unit of measure dimension.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'reference_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_limit_type
-- MAGIC Limit type reference dimension.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_limit_type
(
    limit_type_key              INT             NOT NULL    COMMENT 'Surrogate key',
    limit_type_code             STRING          NOT NULL    COMMENT 'Code: AC|NOR|PAR|ALERT|ACTION|IPC_LIMIT|REPORT',
    limit_type_name             STRING          NOT NULL    COMMENT 'Display name',
    limit_type_description      STRING                      COMMENT 'Description of the limit type',
    hierarchy_rank              INT                         COMMENT 'Rank in limit hierarchy (1=tightest NOR, 3=widest PAR)',
    is_regulatory               BOOLEAN         NOT NULL    COMMENT 'TRUE if limit type appears in regulatory filings',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag'
)
USING DELTA
COMMENT 'L2.2 Reference: Limit type dimension with hierarchy rank.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'reference_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_regulatory_context
-- MAGIC Regulatory submission context dimension.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_regulatory_context
(
    regulatory_context_key      INT             NOT NULL    COMMENT 'Surrogate key',
    region_code                 STRING          NOT NULL    COMMENT 'Region: US|EU|JP|CN|ROW',
    region_name                 STRING          NOT NULL    COMMENT 'Region display name',
    submission_type             STRING          NOT NULL    COMMENT 'Submission type: NDA|ANDA|MAA|JNDA|IND|BLA',
    ctd_module                  STRING                      COMMENT 'CTD module (e.g., 3.2.P.5.1)',
    ctd_section_title           STRING                      COMMENT 'CTD section title',
    regulatory_authority        STRING                      COMMENT 'Authority name (FDA, EMA, PMDA, NMPA)',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag'
)
USING DELTA
COMMENT 'L2.2 Reference: Regulatory submission context dimension.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'reference_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## MDM Dimensions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_product
-- MAGIC Master data product dimension (MDM-resolved).

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_product
(
    product_key                 BIGINT          NOT NULL    COMMENT 'Surrogate key',
    product_id                  STRING          NOT NULL    COMMENT 'MDM-resolved product ID',
    product_name                STRING          NOT NULL    COMMENT 'Product name',
    product_family              STRING                      COMMENT 'Product family grouping',
    brand_name                  STRING                      COMMENT 'Brand / trade name',
    dosage_form                 STRING                      COMMENT 'Dosage form (Tablet, Capsule, etc.)',
    route_of_administration     STRING                      COMMENT 'Route (Oral, IV, Topical, etc.)',
    therapeutic_area            STRING                      COMMENT 'Therapeutic area',
    strength                    STRING                      COMMENT 'Strength string',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    effective_from              DATE                        COMMENT 'MDM effective start date',
    effective_to                DATE                        COMMENT 'MDM effective end date (NULL = current)',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 MDM: Product master dimension.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'mdm_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_material
-- MAGIC Master data material/substance dimension (MDM-resolved).

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_material
(
    material_key                BIGINT          NOT NULL    COMMENT 'Surrogate key',
    material_id                 STRING          NOT NULL    COMMENT 'MDM-resolved material ID',
    material_name               STRING          NOT NULL    COMMENT 'Material name',
    material_type               STRING                      COMMENT 'Type: API|EXCIPIENT|INTERMEDIATE|PACKAGING|REFERENCE_STD',
    cas_number                  STRING                      COMMENT 'CAS registry number',
    inn_name                    STRING                      COMMENT 'INN (International Nonproprietary Name)',
    compendial_name             STRING                      COMMENT 'Compendial name (USP/EP/JP)',
    grade                       STRING                      COMMENT 'Material grade',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    effective_from              DATE                        COMMENT 'MDM effective start date',
    effective_to                DATE                        COMMENT 'MDM effective end date',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 MDM: Material/substance master dimension.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'mdm_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_test_method
-- MAGIC Test method dimension (MDM-resolved).

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_test_method
(
    test_method_key             BIGINT          NOT NULL    COMMENT 'Surrogate key',
    test_method_id              STRING          NOT NULL    COMMENT 'MDM-resolved method ID',
    method_name                 STRING          NOT NULL    COMMENT 'Method name',
    method_number               STRING                      COMMENT 'Method document number',
    method_version              STRING                      COMMENT 'Method version',
    method_type                 STRING                      COMMENT 'Type: COMPENDIAL|IN_HOUSE|VALIDATED|TRANSFER',
    technique                   STRING                      COMMENT 'Analytical technique (HPLC, GC, IR, etc.)',
    compendia_reference         STRING                      COMMENT 'Compendia reference (USP <621>, EP 2.2.29)',
    is_validated                BOOLEAN                     COMMENT 'Validation status',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    effective_from              DATE                        COMMENT 'MDM effective start date',
    effective_to                DATE                        COMMENT 'MDM effective end date',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 MDM: Test method master dimension.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'mdm_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_site
-- MAGIC Manufacturing/testing site dimension (MDM-resolved).

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_site
(
    site_key                    BIGINT          NOT NULL    COMMENT 'Surrogate key',
    site_id                     STRING          NOT NULL    COMMENT 'MDM-resolved site ID',
    site_name                   STRING          NOT NULL    COMMENT 'Site name',
    site_type                   STRING                      COMMENT 'Type: MANUFACTURING|TESTING|PACKAGING|DISTRIBUTION',
    country_code                STRING                      COMMENT 'ISO 3166-1 alpha-2 country code',
    country_name                STRING                      COMMENT 'Country name',
    region_code                 STRING                      COMMENT 'Region: US|EU|JP|CN|ROW',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    effective_from              DATE                        COMMENT 'MDM effective start date',
    effective_to                DATE                        COMMENT 'MDM effective end date',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 MDM: Manufacturing/testing site dimension.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'mdm_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_market
-- MAGIC Market/region dimension.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_market
(
    market_key                  BIGINT          NOT NULL    COMMENT 'Surrogate key',
    market_code                 STRING          NOT NULL    COMMENT 'Market code',
    market_name                 STRING          NOT NULL    COMMENT 'Market name',
    region_code                 STRING          NOT NULL    COMMENT 'Region: US|EU|JP|CN|ROW',
    region_name                 STRING          NOT NULL    COMMENT 'Region display name',
    regulatory_authority        STRING                      COMMENT 'Primary regulatory authority',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 MDM: Market/region dimension.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'mdm_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conformed Dimensions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_specification
-- MAGIC Specification header dimension (conformed from L2.1 with MDM keys).

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_specification
(
    spec_key                    BIGINT          NOT NULL    COMMENT 'Surrogate key',
    source_specification_id     STRING          NOT NULL    COMMENT 'Source system natural key',
    spec_number                 STRING          NOT NULL    COMMENT 'Specification number',
    spec_version                STRING          NOT NULL    COMMENT 'Specification version',
    spec_title                  STRING                      COMMENT 'Specification title',
    spec_type_code              STRING          NOT NULL    COMMENT 'Type: DS|DP|RM|EXCIP|INTERMED|IPC|CCS',
    spec_type_name              STRING                      COMMENT 'Type display name',
    product_key                 BIGINT                      COMMENT 'FK → dim_product',
    material_key                BIGINT                      COMMENT 'FK → dim_material',
    site_key                    BIGINT                      COMMENT 'FK → dim_site',
    market_key                  BIGINT                      COMMENT 'FK → dim_market',
    status_code                 STRING          NOT NULL    COMMENT 'Status: DRA|APP|SUP|OBS|ARCH',
    status_name                 STRING                      COMMENT 'Status display name',
    stage_code                  STRING                      COMMENT 'Stage: DEV|CLI|COM',
    dosage_form                 STRING                      COMMENT 'Dosage form',
    strength                    STRING                      COMMENT 'Strength',
    compendia_reference         STRING                      COMMENT 'Compendia (USP|EP|JP)',
    ctd_section                 STRING                      COMMENT 'CTD section reference',
    effective_start_date_key    INT                         COMMENT 'FK → dim_date (start)',
    effective_end_date_key      INT                         COMMENT 'FK → dim_date (end)',
    approval_date_key           INT                         COMMENT 'FK → dim_date (approval)',
    approved_by                 STRING                      COMMENT 'Approver',
    supersedes_spec_key         BIGINT                      COMMENT 'Self-FK to superseded spec',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'SCD2 current flag',
    valid_from                  TIMESTAMP       NOT NULL    COMMENT 'SCD2 valid from',
    valid_to                    TIMESTAMP                   COMMENT 'SCD2 valid to (NULL = current)',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (spec_type_code)
COMMENT 'L2.2 Conformed: Specification header dimension (SCD2).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'conformed_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_specification_item
-- MAGIC Specification item / test dimension (conformed from L2.1 with MDM keys).

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_specification_item
(
    spec_item_key               BIGINT          NOT NULL    COMMENT 'Surrogate key',
    source_spec_item_id         STRING          NOT NULL    COMMENT 'Source system natural key',
    spec_key                    BIGINT          NOT NULL    COMMENT 'FK → dim_specification',
    test_method_key             BIGINT                      COMMENT 'FK → dim_test_method',
    test_code                   STRING                      COMMENT 'Test code',
    test_name                   STRING          NOT NULL    COMMENT 'Test / parameter name',
    analyte_code                STRING                      COMMENT 'Analyte code',
    parameter_name              STRING                      COMMENT 'Parameter name',
    test_category_code          STRING                      COMMENT 'Category: PHY|CHE|IMP|MIC|BIO|STER|PACK',
    test_category_name          STRING                      COMMENT 'Category display name',
    test_subcategory            STRING                      COMMENT 'Subcategory',
    uom_key                     INT                         COMMENT 'FK → dim_uom',
    criticality_code            STRING                      COMMENT 'Criticality: CQA|CCQA|NCQA|KQA|REPORT',
    sequence_number             INT                         COMMENT 'Display order in specification',
    reporting_type              STRING                      COMMENT 'Type: NUMERIC|PASS_FAIL|TEXT|REPORT_ONLY',
    result_precision            INT                         COMMENT 'Decimal places for numeric results',
    is_required                 BOOLEAN                     COMMENT 'Mandatory test flag',
    compendia_test_ref          STRING                      COMMENT 'Compendia test reference',
    stage_applicability         STRING                      COMMENT 'Stage: RELEASE|STABILITY|IPC|BOTH',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'SCD2 current flag',
    valid_from                  TIMESTAMP       NOT NULL    COMMENT 'SCD2 valid from',
    valid_to                    TIMESTAMP                   COMMENT 'SCD2 valid to',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (test_category_code)
COMMENT 'L2.2 Conformed: Specification item dimension (SCD2).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'conformed_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fact Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### fact_specification_limit
-- MAGIC Grain: one row per specification × item × limit type × stage × effective period.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.fact_specification_limit
(
    spec_limit_key              BIGINT          NOT NULL    COMMENT 'Surrogate key',
    spec_key                    BIGINT          NOT NULL    COMMENT 'FK → dim_specification',
    spec_item_key               BIGINT          NOT NULL    COMMENT 'FK → dim_specification_item',
    limit_type_key              INT             NOT NULL    COMMENT 'FK → dim_limit_type',
    uom_key                     INT                         COMMENT 'FK → dim_uom',
    effective_start_date_key    INT                         COMMENT 'FK → dim_date',
    effective_end_date_key      INT                         COMMENT 'FK → dim_date',

    -- Measures
    lower_limit_value           DECIMAL(18, 6)              COMMENT 'Lower limit numeric value',
    upper_limit_value           DECIMAL(18, 6)              COMMENT 'Upper limit numeric value',
    target_value                DECIMAL(18, 6)              COMMENT 'Target / nominal value',
    limit_range_width           DECIMAL(18, 6)              COMMENT 'Calculated: upper - lower',
    lower_limit_operator        STRING                      COMMENT 'Operator: NLT|GT|GTE|NONE',
    upper_limit_operator        STRING                      COMMENT 'Operator: NMT|LT|LTE|NONE',
    limit_text                  STRING                      COMMENT 'Non-numeric limit text',
    limit_description           STRING                      COMMENT 'Full formatted limit expression',

    -- Context
    limit_basis                 STRING                      COMMENT 'Basis: AS_IS|ANHYDROUS|AS_LABELED|DRIED_BASIS',
    stage_code                  STRING                      COMMENT 'Stage: RELEASE|STABILITY|IPC|BOTH',
    stability_time_point        STRING                      COMMENT 'Time point: T0|T3M|T6M|T12M|T24M|T36M',
    stability_condition         STRING                      COMMENT 'Condition: 25C60RH|40C75RH|REFRIG',

    -- SPC fields
    calculation_method          STRING                      COMMENT 'SPC method: 3_SIGMA|CPK|EWMA|CUSUM|MANUAL',
    sample_size                 INT                         COMMENT 'SPC sample size',
    last_calculated_date_key    INT                         COMMENT 'FK → dim_date (SPC recalc)',

    -- Regulatory
    is_in_filing                BOOLEAN                     COMMENT 'Appears in regulatory filing',
    regulatory_basis            STRING                      COMMENT 'Regulatory basis (ICH Q6A, USP, etc.)',

    -- Lineage
    source_limit_id             STRING                      COMMENT 'Source system natural key',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Current version flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (stage_code)
COMMENT 'L2.2 Fact: Specification limits at grain of spec × item × limit type × stage.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'fact'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Denormalized Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dspec_specification
-- MAGIC Wide denormalized view: one row per specification item with pivoted limit columns.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dspec_specification
(
    dspec_key                   BIGINT          NOT NULL    COMMENT 'Surrogate key',
    spec_key                    BIGINT          NOT NULL    COMMENT 'FK → dim_specification',
    spec_item_key               BIGINT          NOT NULL    COMMENT 'FK → dim_specification_item',

    -- Specification header (denormalized)
    spec_number                 STRING          NOT NULL    COMMENT 'Specification number',
    spec_version                STRING          NOT NULL    COMMENT 'Specification version',
    spec_title                  STRING                      COMMENT 'Specification title',
    spec_type_code              STRING          NOT NULL    COMMENT 'Type code',
    spec_type_name              STRING                      COMMENT 'Type name',
    product_name                STRING                      COMMENT 'Product name',
    material_name               STRING                      COMMENT 'Material name',
    site_name                   STRING                      COMMENT 'Site name',
    market_name                 STRING                      COMMENT 'Market name',
    status_code                 STRING          NOT NULL    COMMENT 'Status code',
    stage_code                  STRING                      COMMENT 'Stage code',
    dosage_form                 STRING                      COMMENT 'Dosage form',
    strength                    STRING                      COMMENT 'Strength',

    -- Item (denormalized)
    test_name                   STRING          NOT NULL    COMMENT 'Test name',
    test_code                   STRING                      COMMENT 'Test code',
    test_category_code          STRING                      COMMENT 'Test category',
    criticality_code            STRING                      COMMENT 'Criticality code',
    uom_code                    STRING                      COMMENT 'Unit of measure',
    sequence_number             INT                         COMMENT 'Display order',
    reporting_type              STRING                      COMMENT 'Reporting type',
    is_required                 BOOLEAN                     COMMENT 'Mandatory flag',

    -- Pivoted limit columns — Acceptance Criteria
    ac_lower_limit              DECIMAL(18, 6)              COMMENT 'AC lower limit',
    ac_upper_limit              DECIMAL(18, 6)              COMMENT 'AC upper limit',
    ac_target_value             DECIMAL(18, 6)              COMMENT 'AC target value',
    ac_limit_description        STRING                      COMMENT 'AC limit expression',

    -- Pivoted limit columns — Normal Operating Range
    nor_lower_limit             DECIMAL(18, 6)              COMMENT 'NOR lower limit',
    nor_upper_limit             DECIMAL(18, 6)              COMMENT 'NOR upper limit',
    nor_target_value            DECIMAL(18, 6)              COMMENT 'NOR target value',
    nor_limit_description       STRING                      COMMENT 'NOR limit expression',

    -- Pivoted limit columns — Proven Acceptable Range
    par_lower_limit             DECIMAL(18, 6)              COMMENT 'PAR lower limit',
    par_upper_limit             DECIMAL(18, 6)              COMMENT 'PAR upper limit',
    par_target_value            DECIMAL(18, 6)              COMMENT 'PAR target value',
    par_limit_description       STRING                      COMMENT 'PAR limit expression',

    -- Pivoted limit columns — Alert
    alert_lower_limit           DECIMAL(18, 6)              COMMENT 'Alert lower limit',
    alert_upper_limit           DECIMAL(18, 6)              COMMENT 'Alert upper limit',
    alert_limit_description     STRING                      COMMENT 'Alert limit expression',

    -- Pivoted limit columns — Action
    action_lower_limit          DECIMAL(18, 6)              COMMENT 'Action lower limit',
    action_upper_limit          DECIMAL(18, 6)              COMMENT 'Action upper limit',
    action_limit_description    STRING                      COMMENT 'Action limit expression',

    -- Hierarchy validation
    is_hierarchy_valid          BOOLEAN                     COMMENT 'TRUE if PAR >= AC >= NOR holds',

    -- Metadata
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Current version flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (spec_type_code)
COMMENT 'L2.2 Denormalized: Wide specification with pivoted limit columns per item.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'denormalized'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Analytical Results Dimensions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_batch
-- MAGIC Manufacturing batch / lot dimension for analytical results.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_batch
(
    batch_key                   BIGINT          NOT NULL    COMMENT 'Surrogate key',
    batch_number                STRING          NOT NULL    COMMENT 'Manufacturing batch / lot number',
    batch_system_id             STRING                      COMMENT 'Batch system ID',
    product_key                 BIGINT                      COMMENT 'FK → dim_product',
    site_key                    BIGINT                      COMMENT 'FK → dim_site',
    manufacturing_date          DATE                        COMMENT 'Manufacturing date',
    expiry_date                 DATE                        COMMENT 'Expiry date',
    batch_size                  DECIMAL(18, 4)              COMMENT 'Batch size',
    batch_size_unit             STRING                      COMMENT 'Batch size unit (kg, L, units)',
    batch_status                STRING                      COMMENT 'Status: RELEASED|QUARANTINE|REJECTED|RECALLED',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 Dimension: Manufacturing batch / lot for analytical results.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'analytical_results',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_stability_condition
-- MAGIC ICH stability storage condition dimension.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_stability_condition
(
    condition_key               INT             NOT NULL    COMMENT 'Surrogate key',
    condition_code              STRING          NOT NULL    COMMENT 'Code: 25C60RH|30C65RH|40C75RH|5C|REFRIG|FREEZER',
    condition_name              STRING          NOT NULL    COMMENT 'Display name (e.g., 25°C / 60% RH)',
    temperature_celsius         DECIMAL(5, 1)               COMMENT 'Temperature in Celsius',
    humidity_pct                DECIMAL(5, 1)               COMMENT 'Relative humidity percentage',
    ich_condition_type          STRING          NOT NULL    COMMENT 'ICH type: LONG_TERM|ACCELERATED|INTERMEDIATE|REFRIGERATED|FROZEN',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag'
)
USING DELTA
COMMENT 'L2.2 Reference: ICH stability storage condition dimension.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'analytical_results',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'reference_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_timepoint
-- MAGIC Stability study time point dimension.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_timepoint
(
    timepoint_key               INT             NOT NULL    COMMENT 'Surrogate key',
    timepoint_code              STRING          NOT NULL    COMMENT 'Code: T0|T1M|T3M|T6M|T9M|T12M|T18M|T24M|T36M',
    timepoint_months            INT             NOT NULL    COMMENT 'Time point in months (0, 1, 3, 6, ...)',
    timepoint_name              STRING          NOT NULL    COMMENT 'Display name (e.g., Initial, 3 Months, 6 Months)',
    display_order               INT             NOT NULL    COMMENT 'Sort order for display',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag'
)
USING DELTA
COMMENT 'L2.2 Reference: Stability study time point dimension.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'analytical_results',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'reference_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_instrument
-- MAGIC Analytical instrument / equipment dimension.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_instrument
(
    instrument_key              BIGINT          NOT NULL    COMMENT 'Surrogate key',
    instrument_id               STRING          NOT NULL    COMMENT 'Instrument ID',
    instrument_name             STRING          NOT NULL    COMMENT 'Instrument name or model',
    instrument_type             STRING                      COMMENT 'Type: HPLC|GC|IR|UV_VIS|DISSOLUTION|BALANCE|PH_METER|OTHER',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 Dimension: Analytical instrument / equipment.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'analytical_results',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Analytical Results Fact

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### fact_analytical_result
-- MAGIC Grain: one row per batch × test × stability condition × time point.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.fact_analytical_result
(
    analytical_result_key       BIGINT          NOT NULL    COMMENT 'Surrogate key',
    batch_key                   BIGINT          NOT NULL    COMMENT 'FK → dim_batch',
    spec_key                    BIGINT                      COMMENT 'FK → dim_specification',
    spec_item_key               BIGINT                      COMMENT 'FK → dim_specification_item',
    condition_key               INT                         COMMENT 'FK → dim_stability_condition',
    timepoint_key               INT                         COMMENT 'FK → dim_timepoint',
    instrument_key              BIGINT                      COMMENT 'FK → dim_instrument',
    uom_key                     INT                         COMMENT 'FK → dim_uom',
    test_date_key               INT                         COMMENT 'FK → dim_date (test date)',

    -- Measures
    result_value                DECIMAL(18, 6)              COMMENT 'Numeric test result',
    result_text                 STRING                      COMMENT 'Text result (non-numeric tests)',
    result_status_code          STRING          NOT NULL    COMMENT 'Status: PASS|FAIL|OOS|OOT|PENDING|REPORT',

    -- Reported limits (vendor-provided for comparison)
    reported_lower_limit        DECIMAL(18, 6)              COMMENT 'Vendor-reported lower limit',
    reported_upper_limit        DECIMAL(18, 6)              COMMENT 'Vendor-reported upper limit',
    reported_target             DECIMAL(18, 6)              COMMENT 'Vendor-reported target',

    -- Derived flags
    is_oos                      BOOLEAN                     COMMENT 'TRUE if result is Out of Specification',
    is_oot                      BOOLEAN                     COMMENT 'TRUE if result is Out of Trend',

    -- Context
    analyst_name                STRING                      COMMENT 'Analyst who performed the test',
    reviewer_name               STRING                      COMMENT 'Reviewer who approved the result',
    lab_name                    STRING                      COMMENT 'Laboratory name',
    report_id                   STRING                      COMMENT 'Analytical report / CoA ID',
    coa_number                  STRING                      COMMENT 'Certificate of Analysis number',
    stability_study_id          STRING                      COMMENT 'Stability study identifier',

    -- Lineage
    source_result_id            STRING                      COMMENT 'Source system natural key',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Current version flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (result_status_code)
COMMENT 'L2.2 Fact: Analytical test results at grain of batch × test × condition × time point.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'analytical_results',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'fact'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify L2.2 Tables

-- COMMAND ----------

SHOW TABLES IN l2_2_spec_unified;
