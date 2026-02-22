-- =============================================================================
-- Table  : src_lims_specification
-- Schema : l2_1_lims
-- Layer  : L2.1 — Source Conform Layer (LIMS-specific)
-- Source : l1_raw.raw_lims_specification
-- Grain  : One row per LIMS specification (latest ingested, deduplicated)
-- Purpose: Cleansed, typed, and LIMS-specific business rules applied.
--          - Deduplication: keep latest record per specification_id
--          - Type casting: STRING → DATE, BOOLEAN, etc.
--          - Source code mapping: LIMS status codes → standard codes
--          - Null handling: standardize empty strings to NULL
--          - No cross-source joins yet (that happens in L2.2)
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l2_1_lims.src_lims_specification
(
    -- Source key (preserved for traceability)
    source_specification_id     STRING          NOT NULL    COMMENT 'LIMS natural key (from raw layer)',
    source_batch_id             STRING          NOT NULL    COMMENT 'ETL batch ID of latest ingest',
    source_ingestion_timestamp  TIMESTAMP       NOT NULL    COMMENT 'Ingestion timestamp of latest record',
    record_hash                 STRING                      COMMENT 'SHA-256 hash for CDC detection',

    -- Typed and cleansed spec fields
    spec_number                 STRING          NOT NULL    COMMENT 'Specification number (cleansed)',
    spec_version                STRING          NOT NULL    COMMENT 'Version — standardized to format x.y (e.g., V1 → 1.0)',
    spec_title                  STRING                      COMMENT 'Specification title (trimmed)',

    -- Type-mapped: LIMS type codes → L2.2 standard codes
    -- Mapping: Tablet Spec → DP, API Spec → DS, Raw Material → RM, etc.
    spec_type_code              STRING          NOT NULL    COMMENT 'Mapped spec type: DS|DP|RM|EXCIP|INTERMED|IPC|CCS',
    spec_type_name              STRING                      COMMENT 'Spec type display name',

    -- Product / material (raw LIMS IDs preserved — MDM resolution in L2.2)
    product_id_lims             STRING                      COMMENT 'Product ID as-is from LIMS (pre-MDM)',
    product_name                STRING                      COMMENT 'Product name (trimmed)',
    material_id_lims            STRING                      COMMENT 'Material ID as-is from LIMS (pre-MDM)',
    material_name               STRING                      COMMENT 'Material name (trimmed)',
    site_id_lims                STRING                      COMMENT 'Site ID as-is from LIMS (pre-MDM)',
    site_name                   STRING                      COMMENT 'Site name (trimmed)',
    market_region               STRING                      COMMENT 'Market / region (standardized to ISO code)',
    dosage_form                 STRING                      COMMENT 'Dosage form (cleansed)',
    strength                    STRING                      COMMENT 'Strength string',

    -- Status mapping: LIMS statuses → standard status codes
    -- Mapping: Active → APP, Inactive → OBS, Draft → DRA, Obsolete → OBS, etc.
    status_code                 STRING          NOT NULL    COMMENT 'Mapped status: DRA|APP|SUP|OBS|ARCH',
    status_name                 STRING                      COMMENT 'Status display name',

    -- Typed dates (NULL if unparseable — flagged in data quality column)
    effective_start_date        DATE                        COMMENT 'Effective start date (parsed from STRING)',
    effective_end_date          DATE                        COMMENT 'Effective end date (NULL = open-ended)',
    approval_date               DATE                        COMMENT 'Approval date (parsed)',
    approved_by                 STRING                      COMMENT 'Approver (trimmed)',

    -- Regulatory attributes
    ctd_section                 STRING                      COMMENT 'CTD section (standardized, e.g., 3.2.S.4.1)',
    stage_code                  STRING                      COMMENT 'Mapped stage: DEV|CLI|COM',
    compendia_reference         STRING                      COMMENT 'Compendia (USP|EP|JP)',
    supersedes_spec_id          STRING                      COMMENT 'Superseded spec ID (from source)',

    -- Data quality flags
    dq_date_parse_error         BOOLEAN                     COMMENT 'TRUE if any date field failed parsing',
    dq_type_code_mapped         BOOLEAN                     COMMENT 'TRUE if spec_type_code was successfully mapped',
    dq_status_code_mapped       BOOLEAN                     COMMENT 'TRUE if status_code was successfully mapped',
    dq_duplicate_flag           BOOLEAN                     COMMENT 'TRUE if multiple raw records existed for this source key',

    -- Audit
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'L2.1 load timestamp (UTC)',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'TRUE = latest version of this source record'
)
USING DELTA
PARTITIONED BY (spec_type_code)
COMMENT 'L2.1 Source conform: LIMS specifications. Cleansed, typed, deduplicated. LIMS-specific type/status mappings applied. Source IDs preserved for L2.2 MDM resolution. No cross-source joins.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.1',
    'quality.source'                    = 'LIMS',
    'quality.source_raw_table'          = 'l1_raw.raw_lims_specification'
);

-- =============================================================================
-- L2.1 Transformation Logic (conceptual — implement as Databricks notebook/job)
-- =============================================================================
-- INSERT OVERWRITE l2_1_lims.src_lims_specification
-- WITH deduplicated AS (
--     SELECT *,
--         ROW_NUMBER() OVER (
--             PARTITION BY specification_id
--             ORDER BY _ingestion_timestamp DESC
--         ) AS rn
--     FROM l1_raw.raw_lims_specification
--     WHERE _source_system = 'LIMS'
-- )
-- SELECT
--     specification_id                                AS source_specification_id,
--     _batch_id                                       AS source_batch_id,
--     _ingestion_timestamp                            AS source_ingestion_timestamp,
--     _record_hash                                    AS record_hash,
--     TRIM(spec_number)                               AS spec_number,
--     -- Normalize version: V1 → 1.0, 1 → 1.0
--     CASE
--         WHEN spec_version RLIKE '^V?[0-9]+$'
--         THEN CONCAT(REGEXP_EXTRACT(spec_version, '[0-9]+'), '.0')
--         ELSE TRIM(spec_version)
--     END                                             AS spec_version,
--     TRIM(spec_title)                                AS spec_title,
--     -- LIMS type code → standard
--     CASE TRIM(UPPER(spec_type))
--         WHEN 'DRUG SUBSTANCE'   THEN 'DS'
--         WHEN 'API'              THEN 'DS'
--         WHEN 'DRUG PRODUCT'     THEN 'DP'
--         WHEN 'FINISHED PRODUCT' THEN 'DP'
--         WHEN 'RAW MATERIAL'     THEN 'RM'
--         WHEN 'EXCIPIENT'        THEN 'EXCIP'
--         WHEN 'INTERMEDIATE'     THEN 'INTERMED'
--         WHEN 'IN-PROCESS'       THEN 'IPC'
--         ELSE 'DP'  -- default fallback, flag with dq_type_code_mapped = FALSE
--     END                                             AS spec_type_code,
--     -- ... (status mapping, date casting, etc.)
--     current_timestamp()                             AS load_timestamp,
--     TRUE                                            AS is_current
-- FROM deduplicated
-- WHERE rn = 1;
