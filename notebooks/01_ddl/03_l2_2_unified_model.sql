-- Databricks notebook source
-- MAGIC %md
-- MAGIC # L2.2 Unified Data Model — Pharmaceutical Quality (PQ/CMC)
-- MAGIC Creates the star schema and denormalized tables for the pharma quality domain.
-- MAGIC Aligned with ICH Q6A/Q6B, ICH Q8-Q12, CTD Module 3, and GMP data standards.
-- MAGIC
-- MAGIC **Reference Dimensions:** `dim_date`, `dim_uom`, `dim_limit_type`, `dim_regulatory_context`, `dim_stability_condition`, `dim_timepoint`
-- MAGIC **MDM Dimensions:** `dim_product`, `dim_material`, `dim_test_method`, `dim_site`, `dim_market`
-- MAGIC **Conformed Dimensions:** `dim_specification`, `dim_specification_item`
-- MAGIC **Analytical Dimensions:** `dim_batch`, `dim_instrument`, `dim_laboratory`
-- MAGIC **Fact Tables:** `fact_specification_limit`, `fact_analytical_result`
-- MAGIC **Denormalized:** `dspec_specification`

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

USE SCHEMA l2_2_unified_model;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Reference Dimensions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_date

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dim_date
(
    date_key                    INT             NOT NULL    COMMENT 'Surrogate key (YYYYMMDD integer)',
    full_date                   DATE            NOT NULL    COMMENT 'Calendar date',
    year                        INT             NOT NULL    COMMENT 'Calendar year',
    quarter                     INT             NOT NULL    COMMENT 'Quarter (1-4)',
    month                       INT             NOT NULL    COMMENT 'Month (1-12)',
    month_name                  STRING          NOT NULL    COMMENT 'Month name (January, ...)',
    day_of_month                INT             NOT NULL    COMMENT 'Day of month (1-31)',
    day_of_week                 INT             NOT NULL    COMMENT 'Day of week (1=Sun, 7=Sat)',
    day_name                    STRING          NOT NULL    COMMENT 'Day name (Monday, ...)',
    week_of_year                INT             NOT NULL    COMMENT 'ISO week number',
    is_weekend                  BOOLEAN         NOT NULL    COMMENT 'TRUE if Saturday or Sunday',
    fiscal_year                 INT                         COMMENT 'Fiscal year (Jul-Jun offset)',
    fiscal_quarter              INT                         COMMENT 'Fiscal quarter'
)
USING DELTA
COMMENT 'L2.2 Reference: Calendar date dimension.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'reference',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'reference_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_uom
-- MAGIC Unit of measure reference dimension per ICH/pharmacopeial standards.

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dim_uom
(
    uom_key                     INT             NOT NULL    COMMENT 'Surrogate key',
    uom_code                    STRING          NOT NULL    COMMENT 'Standardized unit code (mg, %, ppm, CFU/g, pH, etc.)',
    uom_name                    STRING          NOT NULL    COMMENT 'Display name (milligrams, percent, etc.)',
    uom_category                STRING          NOT NULL    COMMENT 'Category: MASS|CONCENTRATION|COUNT|RATIO|LENGTH|VOLUME|OTHER',
    si_conversion_factor        DECIMAL(18, 10)             COMMENT 'Factor to convert to SI base unit',
    si_base_unit                STRING                      COMMENT 'SI base unit code',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag'
)
USING DELTA
COMMENT 'L2.2 Reference: Unit of measure dimension (ICH/pharmacopeial units).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'reference',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'reference_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_limit_type
-- MAGIC Limit type hierarchy per ICH Q8 (PAR >= AC >= NOR).

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dim_limit_type
(
    limit_type_key              INT             NOT NULL    COMMENT 'Surrogate key',
    limit_type_code             STRING          NOT NULL    COMMENT 'Code: AC|NOR|PAR|ALERT|ACTION|IPC_LIMIT|REPORT',
    limit_type_name             STRING          NOT NULL    COMMENT 'Display name',
    limit_type_description      STRING                      COMMENT 'Description of limit type per ICH Q8/Q6A',
    hierarchy_rank              INT                         COMMENT 'Rank in limit hierarchy (1=NOR tightest, 3=PAR widest)',
    is_regulatory               BOOLEAN         NOT NULL    COMMENT 'TRUE if limit type appears in regulatory filings (CTD)',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag'
)
USING DELTA
COMMENT 'L2.2 Reference: Limit type dimension with ICH Q8 hierarchy rank.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'reference_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_regulatory_context
-- MAGIC Regulatory submission context per ICH CTD Module 3.

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dim_regulatory_context
(
    regulatory_context_key      INT             NOT NULL    COMMENT 'Surrogate key',
    region_code                 STRING          NOT NULL    COMMENT 'Region: US|EU|JP|CN|ROW',
    region_name                 STRING          NOT NULL    COMMENT 'Region display name',
    submission_type             STRING          NOT NULL    COMMENT 'Submission: NDA|ANDA|MAA|JNDA|IND|BLA',
    ctd_module                  STRING                      COMMENT 'CTD module (e.g., 3.2.P.5.1)',
    ctd_section_title           STRING                      COMMENT 'CTD section title',
    regulatory_authority        STRING                      COMMENT 'Authority: FDA|EMA|PMDA|NMPA|TGA',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag'
)
USING DELTA
COMMENT 'L2.2 Reference: Regulatory submission context per ICH CTD.'
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
-- MAGIC Drug product master dimension with PQ/CMC regulatory attributes.

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dim_product
(
    product_key                 BIGINT          NOT NULL    COMMENT 'Surrogate key',
    product_id                  STRING          NOT NULL    COMMENT 'MDM-resolved product ID',
    product_name                STRING          NOT NULL    COMMENT 'Product name (INN + strength + dosage form)',
    inn_name                    STRING                      COMMENT 'International Nonproprietary Name (WHO INN)',
    brand_name                  STRING                      COMMENT 'Brand / trade name',
    product_family              STRING                      COMMENT 'Product family grouping',
    dosage_form_code            STRING                      COMMENT 'Dosage form code: TAB|CAP|INJ|SOL|SUS|CRM|OIN|PATCH|INH|LYOPH',
    dosage_form_name            STRING                      COMMENT 'Dosage form display name (Film-Coated Tablet, Capsule, etc.)',
    route_of_administration     STRING                      COMMENT 'Route: ORAL|IV|IM|SC|TOPICAL|INHALATION|NASAL|OPHTHALMIC|OTIC',
    strength                    STRING                      COMMENT 'Strength string (e.g., 500 mg, 250 mg/5 mL)',
    strength_value              DECIMAL(12, 4)              COMMENT 'Numeric strength value',
    strength_uom                STRING                      COMMENT 'Strength unit (mg, mg/mL, %, IU)',
    therapeutic_area            STRING                      COMMENT 'Therapeutic area (Oncology, Cardiology, CNS, etc.)',
    nda_number                  STRING                      COMMENT 'NDA/ANDA/BLA/MAA registration number',
    shelf_life_months           INT                         COMMENT 'Approved shelf life in months',
    storage_conditions          STRING                      COMMENT 'Labeled storage conditions (Store below 25°C)',
    container_closure_system    STRING                      COMMENT 'Primary packaging (HDPE bottle, blister, vial)',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 MDM: Drug product master dimension (PQ/CMC).'
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
-- MAGIC Material/substance master with CMC chemistry attributes.

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dim_material
(
    material_key                BIGINT          NOT NULL    COMMENT 'Surrogate key',
    material_id                 STRING          NOT NULL    COMMENT 'MDM-resolved material ID',
    material_name               STRING          NOT NULL    COMMENT 'Material name (INN or chemical name)',
    material_type_code          STRING                      COMMENT 'Type: API|EXCIPIENT|INTERMEDIATE|PACKAGING|REFERENCE_STD|RAW_MATERIAL',
    material_type_name          STRING                      COMMENT 'Material type display name',
    cas_number                  STRING                      COMMENT 'CAS Registry Number (e.g., 103-90-2)',
    molecular_formula           STRING                      COMMENT 'Molecular formula (e.g., C8H9NO2)',
    molecular_weight            DECIMAL(10, 4)              COMMENT 'Molecular weight in g/mol',
    inn_name                    STRING                      COMMENT 'INN (International Nonproprietary Name)',
    compendial_name             STRING                      COMMENT 'Compendial name (USP/EP/JP monograph name)',
    pharmacopoeia_grade         STRING                      COMMENT 'Grade: USP|EP|JP|NF|ACS|REAGENT|IN_HOUSE',
    grade                       STRING                      COMMENT 'Material grade (pharmaceutical, analytical, etc.)',
    supplier_name               STRING                      COMMENT 'Qualified supplier / manufacturer name',
    retest_period_months        INT                         COMMENT 'Retest period in months (ICH Q1A)',
    storage_requirements        STRING                      COMMENT 'Storage conditions for material',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 MDM: Material/substance master dimension (CMC chemistry).'
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
-- MAGIC Analytical test method dimension per ICH Q2(R1) / CTD 3.2.S.4.2 / 3.2.P.5.2.

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dim_test_method
(
    test_method_key             BIGINT          NOT NULL    COMMENT 'Surrogate key',
    test_method_id              STRING          NOT NULL    COMMENT 'MDM-resolved method ID',
    test_method_name            STRING          NOT NULL    COMMENT 'Method name (e.g., Assay by HPLC)',
    test_method_number          STRING                      COMMENT 'Method document number (e.g., TM-HPLC-001)',
    test_method_version         STRING                      COMMENT 'Method version',
    method_type                 STRING                      COMMENT 'Type: COMPENDIAL|COMPENDIAL_MODIFIED|IN_HOUSE|TRANSFER',
    analytical_technique        STRING                      COMMENT 'Technique: HPLC|GC|UV_VIS|IR|KF|DISSOLUTION|HARDNESS|PSD|LAL',
    compendia_reference         STRING                      COMMENT 'Compendia reference (USP <621>, EP 2.2.29)',
    detection_limit             DECIMAL(18, 6)              COMMENT 'Limit of Detection (LOD) per ICH Q2',
    quantitation_limit          DECIMAL(18, 6)              COMMENT 'Limit of Quantitation (LOQ) per ICH Q2',
    validation_status           STRING                      COMMENT 'Status: VALIDATED|VERIFIED|QUALIFIED|TRANSFER_COMPLETE|IN_PROGRESS',
    validation_date             DATE                        COMMENT 'ICH Q2(R1) validation completion date',
    is_validated                BOOLEAN                     COMMENT 'Validation status flag',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    effective_from              DATE                        COMMENT 'Method effective start date',
    effective_to                DATE                        COMMENT 'Method effective end date',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 MDM: Analytical test method dimension per ICH Q2(R1).'
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
-- MAGIC Manufacturing/testing site dimension with GMP regulatory attributes.

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dim_site
(
    site_key                    BIGINT          NOT NULL    COMMENT 'Surrogate key',
    site_id                     STRING          NOT NULL    COMMENT 'MDM-resolved site ID',
    site_code                   STRING                      COMMENT 'Short site code (e.g., SPR-01, MIL-02)',
    site_name                   STRING          NOT NULL    COMMENT 'Site name',
    site_type                   STRING                      COMMENT 'Type: MANUFACTURING|QC_TESTING|PACKAGING|CMO|CRO|DISTRIBUTION',
    address_line                STRING                      COMMENT 'Street address',
    city                        STRING                      COMMENT 'City',
    state_province              STRING                      COMMENT 'State or province',
    country_code                STRING                      COMMENT 'ISO 3166-1 alpha-2 country code (US, DE, JP, IN)',
    country_name                STRING                      COMMENT 'Country name',
    regulatory_region           STRING                      COMMENT 'Regulatory authority region: FDA|EMA|PMDA|CDSCO|TGA|ANVISA',
    gmp_status                  STRING                      COMMENT 'GMP status: APPROVED|PENDING|WARNING_LETTER|IMPORT_ALERT',
    gmp_certificate_number      STRING                      COMMENT 'GMP certificate or manufacturing license number',
    fda_fei_number              STRING                      COMMENT 'FDA Facility Establishment Identifier (FEI)',
    last_inspection_date        DATE                        COMMENT 'Most recent regulatory inspection date',
    last_inspection_outcome     STRING                      COMMENT 'Outcome: NAI|VAI|OAI (FDA) or SATISFACTORY|NON_COMPLIANT',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 MDM: Manufacturing/testing site dimension with GMP status.'
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
-- MAGIC Market/country dimension with marketing authorization attributes.

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dim_market
(
    market_key                  BIGINT          NOT NULL    COMMENT 'Surrogate key',
    market_code                 STRING          NOT NULL    COMMENT 'Market code (ISO alpha-2 or region code)',
    market_name                 STRING          NOT NULL    COMMENT 'Market name',
    country_code                STRING                      COMMENT 'ISO 3166-1 alpha-2 country code',
    country_name                STRING                      COMMENT 'Country name',
    region_code                 STRING          NOT NULL    COMMENT 'Region: US|EU|JP|CN|ROW',
    region_name                 STRING          NOT NULL    COMMENT 'Region display name',
    regulatory_authority        STRING                      COMMENT 'Primary regulatory authority (FDA, EMA, PMDA, NMPA)',
    primary_pharmacopoeia       STRING                      COMMENT 'Primary pharmacopoeia: USP|EP|JP|BP|IP',
    market_status               STRING                      COMMENT 'MA status: APPROVED|PENDING|FILED|WITHDRAWN|NEVER_FILED',
    marketing_auth_number       STRING                      COMMENT 'Marketing authorization / registration number',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 MDM: Market/country dimension with marketing authorization.'
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
-- MAGIC Specification header dimension per ICH Q6A/Q6B, CTD 3.2.S.4.1 / 3.2.P.5.1.

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dim_specification
(
    spec_key                    BIGINT          NOT NULL    COMMENT 'Surrogate key',
    source_specification_id     STRING          NOT NULL    COMMENT 'Source system natural key',
    spec_number                 STRING          NOT NULL    COMMENT 'Specification document number (e.g., QC-SPEC-2026-0001)',
    spec_version                STRING          NOT NULL    COMMENT 'Specification version (e.g., 1.0, 2.1)',
    spec_title                  STRING                      COMMENT 'Specification title',
    spec_type_code              STRING          NOT NULL    COMMENT 'Type: DS|DP|RM|EXCIP|INTERMED|IPC|CCS',
    spec_type_name              STRING                      COMMENT 'Type display name (Drug Substance, Drug Product, etc.)',
    product_key                 BIGINT                      COMMENT 'FK to dim_product',
    material_key                BIGINT                      COMMENT 'FK to dim_material',
    site_key                    BIGINT                      COMMENT 'FK to dim_site',
    market_key                  BIGINT                      COMMENT 'FK to dim_market',
    status_code                 STRING          NOT NULL    COMMENT 'Status: DRA|APP|SUP|OBS|ARCH',
    status_name                 STRING                      COMMENT 'Status display name (Draft, Approved, Superseded, etc.)',
    stage_code                  STRING                      COMMENT 'Stage: DEV|CLI|COM (Development, Clinical, Commercial)',
    stage_name                  STRING                      COMMENT 'Stage display name',
    ctd_section                 STRING                      COMMENT 'CTD section reference (3.2.S.4.1, 3.2.P.5.1)',
    compendia_reference         STRING                      COMMENT 'Compendia: USP|EP|JP|BP',
    effective_start_date        DATE                        COMMENT 'Specification effective start date',
    effective_end_date          DATE                        COMMENT 'Specification effective end date (NULL = open-ended)',
    effective_start_date_key    INT                         COMMENT 'FK to dim_date (start)',
    effective_end_date_key      INT                         COMMENT 'FK to dim_date (end)',
    approval_date               DATE                        COMMENT 'Formal approval date',
    approval_date_key           INT                         COMMENT 'FK to dim_date (approval)',
    approver_name               STRING                      COMMENT 'Approving authority name',
    approved_by                 STRING                      COMMENT 'Approver user ID',
    supersedes_spec_key         BIGINT                      COMMENT 'Self-FK to superseded specification',
    supersedes_spec_id          STRING                      COMMENT 'Superseded specification source ID',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'SCD2 current flag',
    valid_from                  TIMESTAMP       NOT NULL    COMMENT 'SCD2 valid from',
    valid_to                    TIMESTAMP                   COMMENT 'SCD2 valid to (NULL = current)',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (spec_type_code)
COMMENT 'L2.2 Conformed: Specification header dimension per ICH Q6A/Q6B (SCD2).'
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
-- MAGIC Specification test/parameter dimension per ICH Q6A, with CQA classification per ICH Q8/Q9.

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dim_specification_item
(
    spec_item_key               BIGINT          NOT NULL    COMMENT 'Surrogate key',
    source_spec_item_id         STRING          NOT NULL    COMMENT 'Source system natural key',
    spec_key                    BIGINT          NOT NULL    COMMENT 'FK to dim_specification',
    test_method_key             BIGINT                      COMMENT 'FK to dim_test_method',
    uom_key                     INT                         COMMENT 'FK to dim_uom',
    test_code                   STRING                      COMMENT 'Test code (e.g., ASSAY, DISSO, IMPD)',
    test_name                   STRING          NOT NULL    COMMENT 'Test / parameter name (Assay, Dissolution, etc.)',
    analyte_code                STRING                      COMMENT 'Specific analyte code (APAP, IMP-A, TOTAL-IMP)',
    test_category_code          STRING                      COMMENT 'Category: PHY|CHE|IMP|MIC|BIO|STER|PACK',
    test_category_name          STRING                      COMMENT 'Category display name',
    test_subcategory            STRING                      COMMENT 'Subcategory (Related Substances, Residual Solvents, etc.)',
    criticality                 STRING                      COMMENT 'CQA classification per ICH Q8/Q9: CQA|CCQA|NCQA|KQA|REPORT',
    sequence_number             INT                         COMMENT 'Display order in specification',
    is_required                 BOOLEAN                     COMMENT 'TRUE = mandatory test per specification',
    is_compendial               BOOLEAN                     COMMENT 'TRUE = from official pharmacopoeia (USP/EP/JP)',
    is_stability_indicating     BOOLEAN                     COMMENT 'TRUE = stability-indicating method per ICH Q1A',
    reporting_type              STRING                      COMMENT 'Type: NUMERIC|PASS_FAIL|TEXT|REPORT_ONLY',
    result_precision            INT                         COMMENT 'Decimal places for numeric results',
    compendia_test_ref          STRING                      COMMENT 'Compendia test reference (USP <621>, EP 2.9.3)',
    stage_applicability         STRING                      COMMENT 'Stage: RELEASE|STABILITY|IPC|BOTH',
    reporting_threshold         DECIMAL(18, 6)              COMMENT 'ICH Q3A/Q3B reporting threshold (%)',
    identification_threshold    DECIMAL(18, 6)              COMMENT 'ICH Q3A/Q3B identification threshold (%)',
    qualification_threshold     DECIMAL(18, 6)              COMMENT 'ICH Q3A/Q3B qualification threshold (%)',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'SCD2 current flag',
    valid_from                  TIMESTAMP       NOT NULL    COMMENT 'SCD2 valid from',
    valid_to                    TIMESTAMP                   COMMENT 'SCD2 valid to',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (test_category_code)
COMMENT 'L2.2 Conformed: Specification item/test dimension per ICH Q6A with CQA classification (SCD2).'
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
-- MAGIC ## Fact Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### fact_specification_limit
-- MAGIC Grain: one row per specification x item x limit type x stage x effective period.

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.fact_specification_limit
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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Denormalized Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dspec_specification
-- MAGIC Wide denormalized view: one row per specification item with pivoted limit columns per ICH Q8 hierarchy.

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dspec_specification
(
    dspec_key                   BIGINT          NOT NULL    COMMENT 'Surrogate key',
    spec_key                    BIGINT          NOT NULL    COMMENT 'FK to dim_specification',
    spec_item_key               BIGINT          NOT NULL    COMMENT 'FK to dim_specification_item',

    -- Specification header (denormalized)
    spec_number                 STRING          NOT NULL    COMMENT 'Specification number',
    spec_version                STRING          NOT NULL    COMMENT 'Specification version',
    spec_title                  STRING                      COMMENT 'Specification title',
    spec_type_code              STRING          NOT NULL    COMMENT 'Type code (DS|DP|RM|EXCIP|INTERMED|IPC|CCS)',
    spec_type_name              STRING                      COMMENT 'Type name',
    product_name                STRING                      COMMENT 'Drug product name',
    material_name               STRING                      COMMENT 'Material / substance name',
    site_name                   STRING                      COMMENT 'Manufacturing / testing site',
    market_name                 STRING                      COMMENT 'Target market',
    status_code                 STRING          NOT NULL    COMMENT 'Status code',
    stage_code                  STRING                      COMMENT 'Stage code',
    strength                    STRING                      COMMENT 'Product strength',

    -- Item (denormalized)
    test_name                   STRING          NOT NULL    COMMENT 'Test / parameter name',
    test_code                   STRING                      COMMENT 'Test code',
    test_category_code          STRING                      COMMENT 'Test category',
    criticality_code            STRING                      COMMENT 'CQA criticality (CQA|CCQA|NCQA|KQA|REPORT)',
    uom_code                    STRING                      COMMENT 'Unit of measure code',
    sequence_number             INT                         COMMENT 'Display order',
    reporting_type              STRING                      COMMENT 'Reporting type',
    is_required                 BOOLEAN                     COMMENT 'Mandatory flag',

    -- Pivoted limit columns — Acceptance Criteria (ICH Q6A)
    ac_lower_limit              DECIMAL(18, 6)              COMMENT 'AC lower limit',
    ac_upper_limit              DECIMAL(18, 6)              COMMENT 'AC upper limit',
    ac_target_value             DECIMAL(18, 6)              COMMENT 'AC target value',
    ac_limit_description        STRING                      COMMENT 'AC limit expression (CTD-ready)',

    -- Pivoted limit columns — Normal Operating Range (ICH Q8)
    nor_lower_limit             DECIMAL(18, 6)              COMMENT 'NOR lower limit',
    nor_upper_limit             DECIMAL(18, 6)              COMMENT 'NOR upper limit',
    nor_target_value            DECIMAL(18, 6)              COMMENT 'NOR target value',
    nor_limit_description       STRING                      COMMENT 'NOR limit expression',

    -- Pivoted limit columns — Proven Acceptable Range (ICH Q8 Design Space)
    par_lower_limit             DECIMAL(18, 6)              COMMENT 'PAR lower limit',
    par_upper_limit             DECIMAL(18, 6)              COMMENT 'PAR upper limit',
    par_target_value            DECIMAL(18, 6)              COMMENT 'PAR target value',
    par_limit_description       STRING                      COMMENT 'PAR limit expression',

    -- Pivoted limit columns — Alert (SPC)
    alert_lower_limit           DECIMAL(18, 6)              COMMENT 'Alert lower limit',
    alert_upper_limit           DECIMAL(18, 6)              COMMENT 'Alert upper limit',
    alert_limit_description     STRING                      COMMENT 'Alert limit expression',

    -- Pivoted limit columns — Action (SPC)
    action_lower_limit          DECIMAL(18, 6)              COMMENT 'Action lower limit',
    action_upper_limit          DECIMAL(18, 6)              COMMENT 'Action upper limit',
    action_limit_description    STRING                      COMMENT 'Action limit expression',

    -- Hierarchy validation (ICH Q8: PAR >= AC >= NOR)
    is_hierarchy_valid          BOOLEAN                     COMMENT 'TRUE if PAR >= AC >= NOR holds per ICH Q8',

    -- Metadata
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Current version flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (spec_type_code)
COMMENT 'L2.2 Denormalized: Wide specification with pivoted limit columns per ICH Q8 hierarchy.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'denormalized'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Analytical Results & Stability Dimensions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_stability_condition
-- MAGIC ICH stability storage condition dimension per ICH Q1A(R2).

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dim_stability_condition
(
    condition_key               INT             NOT NULL    COMMENT 'Surrogate key',
    condition_code              STRING          NOT NULL    COMMENT 'Code: 25C60RH|30C65RH|40C75RH|5C|REFRIG|FREEZER',
    condition_name              STRING          NOT NULL    COMMENT 'Display name (e.g., 25 deg C / 60% RH)',
    temperature_celsius         DECIMAL(5, 1)               COMMENT 'Temperature in Celsius',
    humidity_pct                DECIMAL(5, 1)               COMMENT 'Relative humidity percentage',
    ich_condition_type          STRING          NOT NULL    COMMENT 'ICH type: LONG_TERM|ACCELERATED|INTERMEDIATE|REFRIGERATED|FROZEN',
    ich_zone                    STRING                      COMMENT 'ICH climatic zone: I|II|III|IVA|IVB',
    recommended_duration_months INT                         COMMENT 'Recommended minimum study duration per ICH Q1A',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag'
)
USING DELTA
COMMENT 'L2.2 Reference: ICH Q1A(R2) stability storage condition dimension.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'analytical_results',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'reference_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_timepoint
-- MAGIC Stability study time point dimension per ICH Q1A(R2).

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dim_timepoint
(
    timepoint_key               INT             NOT NULL    COMMENT 'Surrogate key',
    timepoint_code              STRING          NOT NULL    COMMENT 'Code: T0|T1M|T3M|T6M|T9M|T12M|T18M|T24M|T36M',
    timepoint_months            INT             NOT NULL    COMMENT 'Time point in months (0, 1, 3, 6, ...)',
    timepoint_name              STRING          NOT NULL    COMMENT 'Display name (e.g., Initial, 3 Months, 6 Months)',
    display_order               INT             NOT NULL    COMMENT 'Sort order for display',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag'
)
USING DELTA
COMMENT 'L2.2 Reference: Stability study time point dimension per ICH Q1A(R2).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'analytical_results',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'reference_dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_batch
-- MAGIC Manufacturing batch / lot dimension with GMP disposition attributes.

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dim_batch
(
    batch_key                   BIGINT          NOT NULL    COMMENT 'Surrogate key',
    batch_number                STRING          NOT NULL    COMMENT 'Manufacturing batch / lot number',
    batch_system_id             STRING                      COMMENT 'Batch system ID (ERP/MES)',
    product_key                 BIGINT                      COMMENT 'FK to dim_product',
    site_key                    BIGINT                      COMMENT 'FK to dim_site (manufacturing site)',
    batch_type                  STRING                      COMMENT 'Type: DEVELOPMENT|PILOT|EXHIBIT|REGISTRATION|COMMERCIAL|VALIDATION|SCALE_UP',
    manufacturing_date          DATE                        COMMENT 'Manufacturing / completion date',
    expiry_date                 DATE                        COMMENT 'Expiry date based on stability data',
    retest_date                 DATE                        COMMENT 'Retest date (for drug substance)',
    batch_size                  DECIMAL(18, 4)              COMMENT 'Batch size value',
    batch_size_unit             STRING                      COMMENT 'Batch size unit (kg, L, units, doses)',
    yield_pct                   DECIMAL(8, 4)               COMMENT 'Batch yield percentage',
    batch_status                STRING                      COMMENT 'Disposition: RELEASED|QUARANTINE|REJECTED|RECALLED|PENDING',
    disposition_date            DATE                        COMMENT 'QA disposition date',
    packaging_configuration     STRING                      COMMENT 'Primary packaging configuration',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 Dimension: Manufacturing batch / lot with GMP disposition.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'analytical_results',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_instrument
-- MAGIC Analytical instrument / equipment dimension with qualification status.

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dim_instrument
(
    instrument_key              BIGINT          NOT NULL    COMMENT 'Surrogate key',
    instrument_id               STRING          NOT NULL    COMMENT 'Instrument / equipment ID',
    instrument_name             STRING          NOT NULL    COMMENT 'Instrument name or model',
    instrument_type             STRING                      COMMENT 'Type: HPLC|GC|UV_VIS|IR|DISSOLUTION|BALANCE|PH_METER|KF|PSD|OTHER',
    serial_number               STRING                      COMMENT 'Manufacturer serial number',
    manufacturer                STRING                      COMMENT 'Instrument manufacturer (Agilent, Waters, Shimadzu)',
    qualification_status        STRING                      COMMENT 'IQ/OQ/PQ status: QUALIFIED|PENDING_OQ|PENDING_PQ|DECOMMISSIONED',
    calibration_due_date        DATE                        COMMENT 'Next calibration due date',
    location                    STRING                      COMMENT 'Lab / room location',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 Dimension: Analytical instrument / equipment with IQ/OQ/PQ status.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'analytical_results',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'dimension'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_laboratory
-- MAGIC QC / analytical laboratory dimension.

-- COMMAND ----------

CREATE OR REPLACE TABLE l2_2_unified_model.dim_laboratory
(
    laboratory_key              BIGINT          NOT NULL    COMMENT 'Surrogate key',
    laboratory_id               STRING          NOT NULL    COMMENT 'Laboratory ID',
    laboratory_name             STRING          NOT NULL    COMMENT 'Laboratory name',
    laboratory_type             STRING                      COMMENT 'Type: QC|R_AND_D|STABILITY|MICROBIOLOGY|CRO|CONTRACT',
    site_key                    BIGINT                      COMMENT 'FK to dim_site (parent site)',
    accreditation_status        STRING                      COMMENT 'Status: ISO_17025|GLP|GMP_COMPLIANT|PENDING',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 Dimension: QC / analytical laboratory.'
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
-- MAGIC Grain: one row per batch x test x stability condition x time point.

-- COMMAND ----------

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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify L2.2 Tables

-- COMMAND ----------

SHOW TABLES IN l2_2_unified_model;
