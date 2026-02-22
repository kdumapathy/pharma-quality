-- Databricks notebook source
-- MAGIC %md
-- MAGIC # L2.1 Source Conform Tables — LIMS
-- MAGIC Cleansed, typed, and deduplicated tables from LIMS raw layer.
-- MAGIC
-- MAGIC **Tables:**
-- MAGIC - `src_lims_specification` — Specifications (deduplicated, typed)
-- MAGIC - `src_lims_spec_item` — Spec items (deduplicated, typed)
-- MAGIC - `src_lims_spec_limit` — Spec limits (deduplicated, typed)

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

USE SCHEMA l2_1_lims;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### src_lims_specification
-- MAGIC Deduplicated, typed LIMS specifications with business code mappings.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_1_lims.src_lims_specification
(
    source_specification_id     STRING          NOT NULL    COMMENT 'LIMS natural key (from raw layer)',
    source_batch_id             STRING          NOT NULL    COMMENT 'ETL batch ID of latest ingest',
    source_ingestion_timestamp  TIMESTAMP       NOT NULL    COMMENT 'Ingestion timestamp of latest record',
    record_hash                 STRING                      COMMENT 'SHA-256 hash for CDC detection',

    spec_number                 STRING          NOT NULL    COMMENT 'Specification number (cleansed)',
    spec_version                STRING          NOT NULL    COMMENT 'Version — standardized to format x.y',
    spec_title                  STRING                      COMMENT 'Specification title (trimmed)',
    spec_type_code              STRING          NOT NULL    COMMENT 'Mapped spec type: DS|DP|RM|EXCIP|INTERMED|IPC|CCS',
    spec_type_name              STRING                      COMMENT 'Spec type display name',
    product_id_lims             STRING                      COMMENT 'Product ID as-is from LIMS (pre-MDM)',
    product_name                STRING                      COMMENT 'Product name (trimmed)',
    material_id_lims            STRING                      COMMENT 'Material ID as-is from LIMS (pre-MDM)',
    material_name               STRING                      COMMENT 'Material name (trimmed)',
    site_id_lims                STRING                      COMMENT 'Site ID as-is from LIMS (pre-MDM)',
    site_name                   STRING                      COMMENT 'Site name (trimmed)',
    market_region               STRING                      COMMENT 'Market / region (standardized to ISO code)',
    dosage_form                 STRING                      COMMENT 'Dosage form (cleansed)',
    strength                    STRING                      COMMENT 'Strength string',
    status_code                 STRING          NOT NULL    COMMENT 'Mapped status: DRA|APP|SUP|OBS|ARCH',
    status_name                 STRING                      COMMENT 'Status display name',
    effective_start_date        DATE                        COMMENT 'Effective start date (parsed from STRING)',
    effective_end_date          DATE                        COMMENT 'Effective end date (NULL = open-ended)',
    approval_date               DATE                        COMMENT 'Approval date (parsed)',
    approved_by                 STRING                      COMMENT 'Approver (trimmed)',
    ctd_section                 STRING                      COMMENT 'CTD section (standardized)',
    stage_code                  STRING                      COMMENT 'Mapped stage: DEV|CLI|COM',
    compendia_reference         STRING                      COMMENT 'Compendia (USP|EP|JP)',
    supersedes_spec_id          STRING                      COMMENT 'Superseded spec ID (from source)',
    dq_date_parse_error         BOOLEAN                     COMMENT 'TRUE if any date field failed parsing',
    dq_type_code_mapped         BOOLEAN                     COMMENT 'TRUE if spec_type_code was successfully mapped',
    dq_status_code_mapped       BOOLEAN                     COMMENT 'TRUE if status_code was successfully mapped',
    dq_duplicate_flag           BOOLEAN                     COMMENT 'TRUE if multiple raw records existed for this source key',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'L2.1 load timestamp (UTC)',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'TRUE = latest version of this source record'
)
USING DELTA
PARTITIONED BY (spec_type_code)
COMMENT 'L2.1 Source conform: LIMS specifications. Cleansed, typed, deduplicated.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.1',
    'quality.source'                    = 'LIMS',
    'quality.source_raw_table'          = 'l1_raw.raw_lims_specification'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### src_lims_spec_item
-- MAGIC Deduplicated, typed LIMS specification items with category/criticality mappings.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_1_lims.src_lims_spec_item
(
    source_spec_item_id         STRING          NOT NULL    COMMENT 'LIMS spec item natural key',
    source_specification_id     STRING          NOT NULL    COMMENT 'LIMS parent spec ID',
    source_batch_id             STRING          NOT NULL    COMMENT 'ETL batch ID',
    source_ingestion_timestamp  TIMESTAMP       NOT NULL    COMMENT 'Ingestion timestamp',
    record_hash                 STRING                      COMMENT 'SHA-256 for CDC',

    test_code                   STRING                      COMMENT 'Test code (cleansed)',
    test_name                   STRING          NOT NULL    COMMENT 'Test name (trimmed)',
    analyte_code                STRING                      COMMENT 'Analyte code (cleansed)',
    parameter_name              STRING                      COMMENT 'Parameter name (trimmed)',
    test_category_code          STRING                      COMMENT 'Mapped category: PHY|CHE|IMP|MIC|BIO|STER|PACK',
    test_category_name          STRING                      COMMENT 'Category display name',
    test_subcategory            STRING                      COMMENT 'Subcategory (cleansed)',
    uom_code                    STRING                      COMMENT 'Unit code (standardized against dim_uom)',
    criticality_code            STRING                      COMMENT 'Mapped criticality: CQA|CCQA|NCQA|KQA|REPORT',
    sequence_number             INT                         COMMENT 'Sequence order (cast from STRING)',
    reporting_type              STRING                      COMMENT 'Mapped: NUMERIC|PASS_FAIL|TEXT|REPORT_ONLY',
    result_precision            INT                         COMMENT 'Decimal places (cast)',
    is_required                 BOOLEAN                     COMMENT 'Mandatory flag (BOOLEAN)',
    compendia_test_ref          STRING                      COMMENT 'Compendia reference (cleaned)',
    stage_applicability         STRING                      COMMENT 'Mapped: RELEASE|STABILITY|IPC|BOTH',
    test_method_id_lims         STRING                      COMMENT 'LIMS method ID (pre-MDM resolution)',
    dq_category_mapped          BOOLEAN                     COMMENT 'TRUE if category successfully mapped',
    dq_criticality_mapped       BOOLEAN                     COMMENT 'TRUE if criticality successfully mapped',
    dq_type_cast_error          BOOLEAN                     COMMENT 'TRUE if any type cast failed',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'L2.1 load timestamp',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Latest version flag'
)
USING DELTA
PARTITIONED BY (test_category_code)
COMMENT 'L2.1 Source conform: LIMS specification items. Deduplicated, typed.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.1',
    'quality.source'                    = 'LIMS',
    'quality.source_raw_table'          = 'l1_raw.raw_lims_spec_item'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### src_lims_spec_limit
-- MAGIC Deduplicated, typed LIMS specification limits with operator standardization.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_1_lims.src_lims_spec_limit
(
    source_limit_id             STRING          NOT NULL    COMMENT 'LIMS limit natural key',
    source_spec_item_id         STRING          NOT NULL    COMMENT 'LIMS parent spec item ID',
    source_specification_id     STRING          NOT NULL    COMMENT 'LIMS parent spec ID',
    source_batch_id             STRING          NOT NULL    COMMENT 'ETL batch ID',
    source_ingestion_timestamp  TIMESTAMP       NOT NULL    COMMENT 'Ingestion timestamp',
    record_hash                 STRING                      COMMENT 'SHA-256 for CDC',
    limit_type_code             STRING          NOT NULL    COMMENT 'Mapped: AC|NOR|PAR|ALERT|ACTION|IPC_LIMIT|REPORT',
    lower_limit_value           DECIMAL(18, 6)              COMMENT 'Lower limit (cast from LIMS string)',
    upper_limit_value           DECIMAL(18, 6)              COMMENT 'Upper limit (cast from LIMS string)',
    target_value                DECIMAL(18, 6)              COMMENT 'Target value (cast)',
    lower_limit_operator        STRING                      COMMENT 'Standardized lower operator: NLT|GT|GTE|NONE',
    upper_limit_operator        STRING                      COMMENT 'Standardized upper operator: NMT|LT|LTE|NONE',
    limit_text                  STRING                      COMMENT 'Non-numeric limit text (cleansed)',
    limit_description           STRING                      COMMENT 'Full formatted expression',
    uom_code                    STRING                      COMMENT 'Unit code (standardized)',
    limit_basis                 STRING                      COMMENT 'Mapped basis: AS_IS|ANHYDROUS|AS_LABELED|DRIED_BASIS',
    stage_code                  STRING                      COMMENT 'Mapped: RELEASE|STABILITY|IPC|BOTH',
    stability_time_point        STRING                      COMMENT 'Standardized time point: T0|T3M|T6M|T12M|T24M|T36M',
    stability_condition         STRING                      COMMENT 'Standardized condition: 25C60RH|40C75RH|REFRIG',
    effective_start_date        DATE                        COMMENT 'Limit effective start (cast)',
    effective_end_date          DATE                        COMMENT 'Limit effective end (NULL = open)',
    calculation_method          STRING                      COMMENT 'SPC method: 3_SIGMA|CPK|EWMA|CUSUM|MANUAL',
    sample_size                 INT                         COMMENT 'SPC sample size (cast)',
    last_calculated_date        DATE                        COMMENT 'SPC last recalculation date (cast)',
    is_in_filing                BOOLEAN                     COMMENT 'Regulatory filing flag',
    regulatory_basis            STRING                      COMMENT 'Regulatory basis (cleansed)',
    dq_limit_type_mapped        BOOLEAN                     COMMENT 'TRUE if limit_type_code was successfully mapped',
    dq_operator_mapped          BOOLEAN                     COMMENT 'TRUE if operators were successfully parsed',
    dq_numeric_cast_error       BOOLEAN                     COMMENT 'TRUE if any numeric cast failed',
    dq_date_parse_error         BOOLEAN                     COMMENT 'TRUE if any date parse failed',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'L2.1 load timestamp',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Latest version flag'
)
USING DELTA
PARTITIONED BY (limit_type_code)
COMMENT 'L2.1 Source conform: LIMS specification limits. Typed, operator-standardized.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.1',
    'quality.source'                    = 'LIMS',
    'quality.source_raw_table'          = 'l1_raw.raw_lims_spec_limit'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### src_process_recipe
-- MAGIC Cleansed, typed process recipe limits with standardized codes.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_1_lims.src_process_recipe
(
    source_recipe_id            STRING          NOT NULL    COMMENT 'Recipe system natural key',
    source_specification_id     STRING                      COMMENT 'Linked LIMS specification ID',
    source_spec_item_id         STRING                      COMMENT 'Linked LIMS spec item ID',
    source_batch_id             STRING          NOT NULL    COMMENT 'ETL batch ID',
    source_ingestion_timestamp  TIMESTAMP       NOT NULL    COMMENT 'Ingestion timestamp',
    record_hash                 STRING                      COMMENT 'SHA-256 for CDC',

    recipe_name                 STRING          NOT NULL    COMMENT 'Recipe name (trimmed)',
    recipe_version              STRING                      COMMENT 'Recipe version',
    recipe_type                 STRING                      COMMENT 'Mapped: MANUFACTURING|PACKAGING|CLEANING',
    parameter_code              STRING                      COMMENT 'Parameter code (cleansed)',
    parameter_name              STRING          NOT NULL    COMMENT 'Parameter name (trimmed)',

    product_id_recipe           STRING                      COMMENT 'Product ID from recipe system (pre-MDM)',
    product_name                STRING                      COMMENT 'Product name (trimmed)',
    material_id_recipe          STRING                      COMMENT 'Material ID from recipe system (pre-MDM)',
    material_name               STRING                      COMMENT 'Material name (trimmed)',
    site_id_recipe              STRING                      COMMENT 'Site ID from recipe system (pre-MDM)',
    site_name                   STRING                      COMMENT 'Site name (trimmed)',

    limit_type_code             STRING          NOT NULL    COMMENT 'Mapped: NOR|PAR|TARGET|ALERT|ACTION|IPC_LIMIT',
    lower_limit_value           DECIMAL(18, 6)              COMMENT 'Lower limit (cast from string)',
    upper_limit_value           DECIMAL(18, 6)              COMMENT 'Upper limit (cast from string)',
    target_value                DECIMAL(18, 6)              COMMENT 'Target value (cast)',
    uom_code                    STRING                      COMMENT 'Unit code (standardized against dim_uom)',
    limit_basis                 STRING                      COMMENT 'Mapped: AS_IS|ANHYDROUS|AS_LABELED|DRIED_BASIS',
    stage_code                  STRING                      COMMENT 'Mapped: DEV|CLI|COM',

    calculation_method          STRING                      COMMENT 'SPC method: 3_SIGMA|CPK|EWMA|CUSUM|MANUAL',
    sample_size                 INT                         COMMENT 'Sample size (cast)',
    cpk_value                   DECIMAL(8, 4)               COMMENT 'Process capability index (cast)',
    last_calculated_date        DATE                        COMMENT 'SPC recalculation date (cast)',

    effective_start_date        DATE                        COMMENT 'Effective start date (cast)',
    effective_end_date          DATE                        COMMENT 'Effective end date (cast)',

    dq_limit_type_mapped        BOOLEAN                     COMMENT 'TRUE if limit_type_code mapped successfully',
    dq_numeric_cast_error       BOOLEAN                     COMMENT 'TRUE if any numeric cast failed',
    dq_date_parse_error         BOOLEAN                     COMMENT 'TRUE if any date parse failed',
    dq_spec_link_valid          BOOLEAN                     COMMENT 'TRUE if specification_id exists in LIMS source',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'L2.1 load timestamp',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Latest version flag'
)
USING DELTA
PARTITIONED BY (limit_type_code)
COMMENT 'L2.1 Source conform: Process recipe limits. Typed, standardized.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.1',
    'quality.source'                    = 'RECIPE',
    'quality.source_raw_table'          = 'l1_raw.raw_process_recipe'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### src_pdf_specification
-- MAGIC Cleansed, typed specification data from transcribed PDF/SOP documents.
-- MAGIC Denormalized: one row per test-limit extracted from the document.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l2_1_lims.src_pdf_specification
(
    -- Source identity
    source_document_id          STRING          NOT NULL    COMMENT 'Document natural key',
    source_row_key              STRING          NOT NULL    COMMENT 'Unique row key (document_id + test_code + limit_type)',
    source_batch_id             STRING          NOT NULL    COMMENT 'ETL batch ID',
    source_ingestion_timestamp  TIMESTAMP       NOT NULL    COMMENT 'Ingestion timestamp',
    record_hash                 STRING                      COMMENT 'SHA-256 for CDC',

    -- Document provenance
    document_name               STRING          NOT NULL    COMMENT 'Document title (trimmed)',
    document_version            STRING                      COMMENT 'Document revision',
    document_type               STRING                      COMMENT 'Mapped: SOP|SPEC_SHEET|REGULATORY|CERTIFICATE',
    sop_number                  STRING                      COMMENT 'SOP number (trimmed)',
    page_number                 INT                         COMMENT 'Page number (cast)',
    section_reference           STRING                      COMMENT 'Section heading (trimmed)',
    transcription_date          DATE                        COMMENT 'Transcription date (cast)',
    transcribed_by              STRING                      COMMENT 'Transcriber',

    -- Specification header
    spec_number                 STRING                      COMMENT 'Specification number (cleansed)',
    spec_version                STRING                      COMMENT 'Version (standardized)',
    spec_title                  STRING                      COMMENT 'Title (trimmed)',
    spec_type_code              STRING                      COMMENT 'Mapped: DS|DP|RM|EXCIP|INTERMED|IPC|CCS',

    -- Product / Material
    product_id_pdf              STRING                      COMMENT 'Product ID from document (pre-MDM)',
    product_name                STRING                      COMMENT 'Product name (trimmed)',
    material_id_pdf             STRING                      COMMENT 'Material ID from document (pre-MDM)',
    material_name               STRING                      COMMENT 'Material name (trimmed)',
    site_name                   STRING                      COMMENT 'Site name (trimmed)',
    market_region               STRING                      COMMENT 'Market region (standardized)',

    -- Test / Parameter
    test_code                   STRING                      COMMENT 'Test code (cleansed)',
    test_name                   STRING          NOT NULL    COMMENT 'Test name (trimmed)',
    test_category_code          STRING                      COMMENT 'Mapped: PHY|CHE|IMP|MIC|BIO|STER|PACK',
    test_method_reference       STRING                      COMMENT 'Method reference (trimmed)',
    uom_code                    STRING                      COMMENT 'Unit code (standardized)',
    criticality_code            STRING                      COMMENT 'Mapped: CQA|CCQA|NCQA|KQA|REPORT',

    -- Limit
    limit_type_code             STRING          NOT NULL    COMMENT 'Mapped: AC|NOR|PAR|ALERT|ACTION|REPORT',
    lower_limit_value           DECIMAL(18, 6)              COMMENT 'Lower limit (cast)',
    upper_limit_value           DECIMAL(18, 6)              COMMENT 'Upper limit (cast)',
    target_value                DECIMAL(18, 6)              COMMENT 'Target value (cast)',
    limit_text                  STRING                      COMMENT 'Qualitative limit text (trimmed)',
    limit_expression            STRING                      COMMENT 'Full limit expression as written in document',

    -- Regulatory
    ctd_section                 STRING                      COMMENT 'CTD section (standardized)',
    compendia_reference         STRING                      COMMENT 'Compendia reference (trimmed)',
    regulatory_basis            STRING                      COMMENT 'Regulatory basis (trimmed)',
    stage_code                  STRING                      COMMENT 'Mapped: RELEASE|STABILITY|IPC|BOTH',
    stability_condition         STRING                      COMMENT 'Stability condition (standardized)',

    -- Dates
    effective_date              DATE                        COMMENT 'Effective date (cast)',
    approval_date               DATE                        COMMENT 'Approval date (cast)',
    approved_by                 STRING                      COMMENT 'Approver (trimmed)',

    -- DQ flags
    dq_spec_number_present      BOOLEAN                     COMMENT 'TRUE if spec_number was present',
    dq_numeric_cast_error       BOOLEAN                     COMMENT 'TRUE if any numeric cast failed',
    dq_date_parse_error         BOOLEAN                     COMMENT 'TRUE if any date parse failed',
    dq_limit_type_mapped        BOOLEAN                     COMMENT 'TRUE if limit_type_code mapped successfully',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'L2.1 load timestamp',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Latest version flag'
)
USING DELTA
PARTITIONED BY (limit_type_code)
COMMENT 'L2.1 Source conform: Transcribed PDF/SOP specification data. Typed, standardized.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.1',
    'quality.source'                    = 'PDF',
    'quality.source_raw_table'          = 'l1_raw.raw_pdf_specification'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify L2.1 Tables

-- COMMAND ----------

SHOW TABLES IN l2_1_lims;
