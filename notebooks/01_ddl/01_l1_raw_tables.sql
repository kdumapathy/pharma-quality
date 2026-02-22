-- Databricks notebook source
-- MAGIC %md
-- MAGIC # L1 Raw Tables — Ingestion Zone
-- MAGIC Creates the immutable raw landing tables for all source systems.
-- MAGIC All fields stored as STRING to avoid type-cast failures.
-- MAGIC
-- MAGIC **Tables:**
-- MAGIC - `raw_lims_specification` — Specification headers (LIMS)
-- MAGIC - `raw_lims_spec_item` — Specification items / tests (LIMS)
-- MAGIC - `raw_lims_spec_limit` — Specification limits (LIMS)
-- MAGIC - `raw_process_recipe` — Process recipe limits (Recipe system)
-- MAGIC - `raw_pdf_specification` — Transcribed PDF/SOP specifications
-- MAGIC - `raw_vendor_analytical_results` — Vendor/Excel stability analytical results

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

USE SCHEMA l1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### raw_lims_specification
-- MAGIC Specification header data from LIMS. Append-only, source-faithful.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l1_raw.raw_lims_specification
(
    -- Ingestion Metadata
    _ingestion_id               STRING          NOT NULL    COMMENT 'UUID assigned by ingestion pipeline for deduplication',
    _source_system              STRING          NOT NULL    COMMENT 'Source system tag (LIMS)',
    _source_file                STRING                      COMMENT 'Source file or API endpoint path',
    _batch_id                   STRING          NOT NULL    COMMENT 'ETL batch identifier (e.g., date + run number)',
    _ingestion_timestamp        TIMESTAMP       NOT NULL    COMMENT 'UTC timestamp when record was ingested',
    _record_hash                STRING                      COMMENT 'SHA-256 hash of source payload for change detection',

    -- Source Columns
    specification_id            STRING                      COMMENT 'LIMS specification record ID (PK in source)',
    spec_number                 STRING                      COMMENT 'Specification document number',
    spec_version                STRING                      COMMENT 'Version (e.g., 1, 1.0, V1)',
    spec_title                  STRING                      COMMENT 'Specification title',
    spec_type                   STRING                      COMMENT 'Type code from LIMS (may differ from unified codes)',
    product_id                  STRING                      COMMENT 'Product ID from LIMS',
    product_name                STRING                      COMMENT 'Product name as stored in LIMS',
    material_id                 STRING                      COMMENT 'Material / substance ID in LIMS',
    material_name               STRING                      COMMENT 'Material name in LIMS',
    site_id                     STRING                      COMMENT 'Site ID from LIMS',
    site_name                   STRING                      COMMENT 'Site name from LIMS',
    market_region               STRING                      COMMENT 'Market or region code from LIMS',
    dosage_form                 STRING                      COMMENT 'Dosage form (e.g., Tablet, Capsule)',
    strength                    STRING                      COMMENT 'Strength string (e.g., 10 mg)',
    status                      STRING                      COMMENT 'Status in LIMS (e.g., Active, Inactive, Draft)',
    effective_start_date        STRING                      COMMENT 'Effective start date as string (YYYY-MM-DD)',
    effective_end_date          STRING                      COMMENT 'Effective end date as string (or NULL)',
    approval_date               STRING                      COMMENT 'Approval date as string (or NULL)',
    approved_by                 STRING                      COMMENT 'Approver user ID or name',
    ctd_ref                     STRING                      COMMENT 'CTD section reference (free text)',
    stage                       STRING                      COMMENT 'Stage (Development / Clinical / Commercial)',
    superseded_by               STRING                      COMMENT 'Specification ID that supersedes this one',
    compendia                   STRING                      COMMENT 'Compendia reference (USP / EP / JP)',
    created_date                STRING                      COMMENT 'Record creation date in LIMS',
    modified_date               STRING                      COMMENT 'Record last-modified date in LIMS',
    created_by                  STRING                      COMMENT 'User who created the record in LIMS',
    raw_payload                 STRING                      COMMENT 'Full JSON or XML payload from source API (optional)'
)
USING DELTA
PARTITIONED BY (_source_system)
COMMENT 'L1 Raw: LIMS specification header. Immutable append-only. All STRING.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L1',
    'quality.source'                    = 'LIMS',
    'quality.table_type'                = 'raw_ingest',
    'quality.transformation'            = 'none'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### raw_lims_spec_item
-- MAGIC Specification items / test records from LIMS. Append-only.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l1_raw.raw_lims_spec_item
(
    _ingestion_id               STRING          NOT NULL    COMMENT 'UUID for deduplication',
    _source_system              STRING          NOT NULL    COMMENT 'LIMS',
    _batch_id                   STRING          NOT NULL    COMMENT 'ETL batch ID',
    _ingestion_timestamp        TIMESTAMP       NOT NULL    COMMENT 'UTC ingestion time',
    _record_hash                STRING                      COMMENT 'SHA-256 of source payload',

    spec_item_id                STRING                      COMMENT 'LIMS spec item record ID',
    specification_id            STRING                      COMMENT 'Parent spec ID (FK to raw_lims_specification)',
    test_method_id              STRING                      COMMENT 'Method ID from LIMS',
    test_code                   STRING                      COMMENT 'Test code in LIMS',
    test_name                   STRING                      COMMENT 'Test / parameter name',
    analyte_code                STRING                      COMMENT 'Analyte code',
    parameter_name              STRING                      COMMENT 'Parameter name',
    test_category               STRING                      COMMENT 'Category (Physical / Chemical / Microbiological)',
    test_subcategory            STRING                      COMMENT 'Subcategory',
    uom                         STRING                      COMMENT 'Unit of measure string from LIMS',
    criticality                 STRING                      COMMENT 'CQA criticality flag from LIMS',
    sequence_number             STRING                      COMMENT 'Order in specification',
    reporting_type              STRING                      COMMENT 'Numeric / Pass-Fail / Text',
    result_precision            STRING                      COMMENT 'Decimal places (as string)',
    is_required                 STRING                      COMMENT 'TRUE/FALSE/Y/N from LIMS',
    compendia_ref               STRING                      COMMENT 'Compendia test reference',
    stage_applicability         STRING                      COMMENT 'Release / Stability / IPC',
    created_date                STRING                      COMMENT 'Record creation date',
    modified_date               STRING                      COMMENT 'Last modified date'
)
USING DELTA
PARTITIONED BY (_source_system)
COMMENT 'L1 Raw: LIMS specification items. Immutable append-only. All STRING.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L1',
    'quality.source'                    = 'LIMS',
    'quality.table_type'                = 'raw_ingest'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### raw_lims_spec_limit
-- MAGIC Specification limits from LIMS. Includes SPC control limit fields.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l1_raw.raw_lims_spec_limit
(
    _ingestion_id               STRING          NOT NULL    COMMENT 'UUID for deduplication',
    _source_system              STRING          NOT NULL    COMMENT 'LIMS',
    _batch_id                   STRING          NOT NULL    COMMENT 'ETL batch ID',
    _ingestion_timestamp        TIMESTAMP       NOT NULL    COMMENT 'UTC ingestion time',
    _record_hash                STRING                      COMMENT 'SHA-256 of source payload',

    limit_id                    STRING                      COMMENT 'LIMS limit record ID',
    spec_item_id                STRING                      COMMENT 'Parent spec item ID',
    specification_id            STRING                      COMMENT 'Parent spec ID',
    limit_type                  STRING                      COMMENT 'Limit type from LIMS (AC / NOR / PAR / Alert / Action / IPC)',
    comparison_operator         STRING                      COMMENT 'Operator string from LIMS',
    lower_limit                 STRING                      COMMENT 'Lower limit value (numeric as string)',
    upper_limit                 STRING                      COMMENT 'Upper limit value (numeric as string)',
    target_value                STRING                      COMMENT 'Target / nominal value',
    limit_text                  STRING                      COMMENT 'Text / qualitative limit',
    uom                         STRING                      COMMENT 'Unit of measure',
    limit_basis                 STRING                      COMMENT 'Basis (as-is / anhydrous / as-labeled)',
    stage                       STRING                      COMMENT 'Stage (Release / Stability / IPC)',
    stability_time_point        STRING                      COMMENT 'Stability time point (T0 / T6M / T12M)',
    stability_condition         STRING                      COMMENT 'Storage condition (25C60RH / 40C75RH)',
    effective_start_date        STRING                      COMMENT 'Limit effective start date',
    effective_end_date          STRING                      COMMENT 'Limit effective end date',
    calculation_method          STRING                      COMMENT 'SPC method',
    sample_size                 STRING                      COMMENT 'Sample size for SPC',
    last_calculated_date        STRING                      COMMENT 'SPC recalculation date',
    is_in_filing                STRING                      COMMENT 'TRUE/FALSE — appears in regulatory filing',
    regulatory_basis            STRING                      COMMENT 'Regulatory basis (ICH Q6A / USP)',
    created_date                STRING                      COMMENT 'Record creation date',
    modified_date               STRING                      COMMENT 'Last modified date'
)
USING DELTA
PARTITIONED BY (_source_system)
COMMENT 'L1 Raw: LIMS specification limits. Immutable append-only. All STRING.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L1',
    'quality.source'                    = 'LIMS',
    'quality.table_type'                = 'raw_ingest'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### raw_process_recipe
-- MAGIC Process recipe / manufacturing parameter limits from the Recipe Management system.
-- MAGIC Contains NOR, PAR, Target, Alert, and Action limits derived from process capability and validation data.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l1_raw.raw_process_recipe
(
    -- Ingestion Metadata
    _ingestion_id               STRING          NOT NULL    COMMENT 'UUID assigned by ingestion pipeline',
    _source_system              STRING          NOT NULL    COMMENT 'Source system tag (RECIPE)',
    _source_file                STRING                      COMMENT 'Source file or API endpoint',
    _batch_id                   STRING          NOT NULL    COMMENT 'ETL batch identifier',
    _ingestion_timestamp        TIMESTAMP       NOT NULL    COMMENT 'UTC ingestion timestamp',
    _record_hash                STRING                      COMMENT 'SHA-256 hash of source payload',

    -- Recipe Header
    recipe_id                   STRING                      COMMENT 'Recipe record ID (PK in source)',
    recipe_name                 STRING                      COMMENT 'Recipe name',
    recipe_version              STRING                      COMMENT 'Recipe version',
    recipe_type                 STRING                      COMMENT 'Type: MANUFACTURING|PACKAGING|CLEANING',

    -- Product / Material linkage
    product_id                  STRING                      COMMENT 'Product ID in recipe system',
    product_name                STRING                      COMMENT 'Product name',
    material_id                 STRING                      COMMENT 'Material ID in recipe system',
    material_name               STRING                      COMMENT 'Material name',
    site_id                     STRING                      COMMENT 'Site ID',
    site_name                   STRING                      COMMENT 'Site name',

    -- Specification linkage (maps to LIMS spec/item for unified model)
    specification_id            STRING                      COMMENT 'Linked LIMS specification ID',
    spec_item_id                STRING                      COMMENT 'Linked LIMS spec item ID',
    parameter_code              STRING                      COMMENT 'Recipe parameter code',
    parameter_name              STRING                      COMMENT 'Recipe parameter / test name',

    -- Limit values
    limit_type                  STRING                      COMMENT 'Limit type: NOR|PAR|TARGET|ALERT|ACTION|IPC_LIMIT',
    lower_limit                 STRING                      COMMENT 'Lower limit value (numeric as string)',
    upper_limit                 STRING                      COMMENT 'Upper limit value (numeric as string)',
    target_value                STRING                      COMMENT 'Target / nominal value',
    uom                         STRING                      COMMENT 'Unit of measure string',
    limit_basis                 STRING                      COMMENT 'Basis (as-is / anhydrous / dried-basis)',

    -- Context
    stage                       STRING                      COMMENT 'Stage: Development / Clinical / Commercial',

    -- SPC / Process Capability
    calculation_method          STRING                      COMMENT 'SPC method: 3_SIGMA|CPK|EWMA|CUSUM|MANUAL',
    sample_size                 STRING                      COMMENT 'Sample size used for SPC calculation',
    cpk_value                   STRING                      COMMENT 'Process capability index (Cpk)',
    last_calculated_date        STRING                      COMMENT 'Date of last SPC recalculation',

    -- Metadata
    effective_start_date        STRING                      COMMENT 'Effective start date (YYYY-MM-DD)',
    effective_end_date          STRING                      COMMENT 'Effective end date (or NULL)',
    approval_status             STRING                      COMMENT 'Approval status in recipe system',
    approved_by                 STRING                      COMMENT 'Approver',
    created_date                STRING                      COMMENT 'Record creation date',
    modified_date               STRING                      COMMENT 'Last modified date'
)
USING DELTA
PARTITIONED BY (_source_system)
COMMENT 'L1 Raw: Process recipe limits (NOR/PAR/Target). Immutable append-only. All STRING.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L1',
    'quality.source'                    = 'RECIPE',
    'quality.table_type'                = 'raw_ingest',
    'quality.transformation'            = 'none'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### raw_pdf_specification
-- MAGIC Transcribed specification data from SOP / PDF documents.
-- MAGIC Each row represents one test-limit combination extracted from a document page.
-- MAGIC Flat/denormalized structure since the source is a transcribed CSV, not a relational system.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l1_raw.raw_pdf_specification
(
    -- Ingestion Metadata
    _ingestion_id               STRING          NOT NULL    COMMENT 'UUID assigned by ingestion pipeline',
    _source_system              STRING          NOT NULL    COMMENT 'Source system tag (PDF)',
    _source_file                STRING                      COMMENT 'Original PDF file path or name',
    _batch_id                   STRING          NOT NULL    COMMENT 'ETL batch identifier',
    _ingestion_timestamp        TIMESTAMP       NOT NULL    COMMENT 'UTC ingestion timestamp',
    _record_hash                STRING                      COMMENT 'SHA-256 hash of row content',

    -- Document Provenance
    document_id                 STRING                      COMMENT 'Unique document identifier',
    document_name               STRING                      COMMENT 'PDF / SOP document title',
    document_version            STRING                      COMMENT 'Document revision or version',
    document_type               STRING                      COMMENT 'Type: SOP|SPEC_SHEET|REGULATORY|CERTIFICATE',
    sop_number                  STRING                      COMMENT 'SOP document number (e.g., SOP-QC-001)',
    page_number                 STRING                      COMMENT 'Page number in source PDF',
    section_reference           STRING                      COMMENT 'Section heading or reference in the document',
    transcription_date          STRING                      COMMENT 'Date the PDF was transcribed to CSV',
    transcribed_by              STRING                      COMMENT 'User or system that performed transcription',

    -- Specification Header
    spec_number                 STRING                      COMMENT 'Specification number extracted from document',
    spec_version                STRING                      COMMENT 'Specification version',
    spec_title                  STRING                      COMMENT 'Specification title from document',
    spec_type                   STRING                      COMMENT 'Type: Drug Product / Drug Substance / etc.',

    -- Product / Material
    product_id                  STRING                      COMMENT 'Product ID referenced in document',
    product_name                STRING                      COMMENT 'Product name from document',
    material_id                 STRING                      COMMENT 'Material ID referenced in document',
    material_name               STRING                      COMMENT 'Material / substance name',
    site_name                   STRING                      COMMENT 'Manufacturing / testing site from document',
    market_region               STRING                      COMMENT 'Target market or region',

    -- Test / Parameter
    test_code                   STRING                      COMMENT 'Test code from document',
    test_name                   STRING                      COMMENT 'Test / parameter name',
    test_category               STRING                      COMMENT 'Test category (Physical / Chemical / etc.)',
    test_method_reference       STRING                      COMMENT 'Method reference (e.g., USP <621>)',
    uom                         STRING                      COMMENT 'Unit of measure from document',
    criticality                 STRING                      COMMENT 'CQA / CCQA / NCQA from document',

    -- Limit Values (one row per test-limit combination)
    limit_type                  STRING                      COMMENT 'Limit type: AC|NOR|PAR|ALERT|ACTION|REPORT',
    lower_limit                 STRING                      COMMENT 'Lower limit value (as printed in document)',
    upper_limit                 STRING                      COMMENT 'Upper limit value',
    target_value                STRING                      COMMENT 'Target / nominal value',
    limit_text                  STRING                      COMMENT 'Qualitative limit text (e.g., White powder)',
    limit_expression            STRING                      COMMENT 'Full limit as written in document (e.g., NMT 0.15%)',

    -- Regulatory Context
    ctd_section                 STRING                      COMMENT 'CTD section reference from document',
    compendia_reference         STRING                      COMMENT 'Compendia reference (USP / EP / JP)',
    regulatory_basis            STRING                      COMMENT 'Regulatory basis (ICH Q6A / Q3B)',
    stage                       STRING                      COMMENT 'Stage: Release / Stability',
    stability_condition         STRING                      COMMENT 'Stability storage condition',

    -- Status / Dates
    effective_date              STRING                      COMMENT 'Effective date from document',
    approval_date               STRING                      COMMENT 'Approval date from document',
    approved_by                 STRING                      COMMENT 'Approver name from signature block'
)
USING DELTA
PARTITIONED BY (_source_system)
COMMENT 'L1 Raw: Transcribed PDF/SOP specification data. Flat CSV ingest. All STRING.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L1',
    'quality.source'                    = 'PDF',
    'quality.table_type'                = 'raw_ingest',
    'quality.transformation'            = 'none'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### raw_vendor_analytical_results
-- MAGIC Analytical test results from vendor/Excel stability studies.
-- MAGIC Each row represents one test result for a specific batch × test × stability condition × time point.
-- MAGIC Source is typically an Excel workbook exported from a CRO or vendor LIMS.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS l1_raw.raw_vendor_analytical_results
(
    -- Ingestion Metadata
    _ingestion_id               STRING          NOT NULL    COMMENT 'UUID assigned by ingestion pipeline',
    _source_system              STRING          NOT NULL    COMMENT 'Source system tag (VENDOR_EXCEL)',
    _source_file                STRING                      COMMENT 'Source Excel file path or name',
    _batch_id                   STRING          NOT NULL    COMMENT 'ETL batch identifier',
    _ingestion_timestamp        TIMESTAMP       NOT NULL    COMMENT 'UTC ingestion timestamp',
    _record_hash                STRING                      COMMENT 'SHA-256 hash of row content',

    -- Result Identification
    result_id                   STRING                      COMMENT 'Unique result record ID from vendor',
    sample_id                   STRING                      COMMENT 'Sample identifier',

    -- Batch
    batch_number                STRING                      COMMENT 'Manufacturing batch / lot number',
    batch_id                    STRING                      COMMENT 'Batch system ID',
    manufacturing_date          STRING                      COMMENT 'Batch manufacturing date (YYYY-MM-DD)',
    expiry_date                 STRING                      COMMENT 'Batch expiry date (YYYY-MM-DD)',
    batch_size                  STRING                      COMMENT 'Batch size value',
    batch_size_unit             STRING                      COMMENT 'Batch size unit (kg, L, units)',

    -- Product / Material
    product_id                  STRING                      COMMENT 'Product ID from vendor',
    product_name                STRING                      COMMENT 'Product name',
    material_id                 STRING                      COMMENT 'Material / substance ID',
    material_name               STRING                      COMMENT 'Material name',
    dosage_form                 STRING                      COMMENT 'Dosage form (Tablet, Capsule)',
    strength                    STRING                      COMMENT 'Product strength (e.g., 500 mg)',

    -- Specification linkage
    specification_id            STRING                      COMMENT 'Linked specification ID',
    spec_item_id                STRING                      COMMENT 'Linked specification item ID',

    -- Test / Parameter
    test_code                   STRING                      COMMENT 'Test code',
    test_name                   STRING                      COMMENT 'Test / parameter name',
    test_category               STRING                      COMMENT 'Test category (Physical / Chemical / etc.)',
    test_method_id              STRING                      COMMENT 'Test method ID',
    test_method_name            STRING                      COMMENT 'Test method name or reference',

    -- Stability Context
    stability_study_id          STRING                      COMMENT 'Stability study identifier',
    storage_condition           STRING                      COMMENT 'ICH storage condition (e.g., 25C/60%RH, 40C/75%RH)',
    time_point_code             STRING                      COMMENT 'Time point code (T0, T3M, T6M, T12M, T24M, T36M)',
    time_point_months           STRING                      COMMENT 'Time point in months (numeric as string)',
    pull_date                   STRING                      COMMENT 'Sample pull / test date (YYYY-MM-DD)',

    -- Result Values
    result_value                STRING                      COMMENT 'Numeric result value (as string)',
    result_text                 STRING                      COMMENT 'Text result (for non-numeric tests)',
    result_unit                 STRING                      COMMENT 'Unit of measure for result',
    result_status               STRING                      COMMENT 'Result status: PASS|FAIL|OOS|OOT|PENDING|REPORT',

    -- Limits (as reported by vendor)
    reported_lower_limit        STRING                      COMMENT 'Lower limit as reported by vendor',
    reported_upper_limit        STRING                      COMMENT 'Upper limit as reported by vendor',
    reported_target             STRING                      COMMENT 'Target value as reported by vendor',

    -- Instrument
    instrument_id               STRING                      COMMENT 'Instrument / equipment ID',
    instrument_name             STRING                      COMMENT 'Instrument name or type',

    -- Personnel
    analyst_name                STRING                      COMMENT 'Analyst who performed the test',
    reviewer_name               STRING                      COMMENT 'Reviewer who approved the result',

    -- Site / Lab
    site_id                     STRING                      COMMENT 'Testing site ID',
    site_name                   STRING                      COMMENT 'Testing site name',
    lab_id                      STRING                      COMMENT 'Laboratory ID',
    lab_name                    STRING                      COMMENT 'Laboratory name',

    -- Document / Report
    report_id                   STRING                      COMMENT 'Analytical report or CoA ID',
    report_name                 STRING                      COMMENT 'Report name',
    coa_number                  STRING                      COMMENT 'Certificate of Analysis number',

    -- Dates
    test_date                   STRING                      COMMENT 'Date test was performed (YYYY-MM-DD)',
    review_date                 STRING                      COMMENT 'Date result was reviewed (YYYY-MM-DD)',
    approval_date               STRING                      COMMENT 'Date result was approved (YYYY-MM-DD)',

    -- Metadata
    created_date                STRING                      COMMENT 'Record creation date in source',
    modified_date               STRING                      COMMENT 'Record last-modified date in source'
)
USING DELTA
PARTITIONED BY (_source_system)
COMMENT 'L1 Raw: Vendor/Excel analytical test results for stability studies. Immutable append-only. All STRING.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'analytical_results',
    'quality.layer'                     = 'L1',
    'quality.source'                    = 'VENDOR_EXCEL',
    'quality.table_type'                = 'raw_ingest',
    'quality.transformation'            = 'none'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify L1 Tables

-- COMMAND ----------

SHOW TABLES IN l1_raw;
