-- =============================================================================
-- Table  : src_lims_spec_item
-- Schema : l2_1_lims
-- Layer  : L2.1 — Source Conform Layer (LIMS-specific)
-- Source : l1_raw.raw_lims_spec_item
-- Grain  : One row per LIMS spec item (latest, deduplicated)
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l2_1_lims.src_lims_spec_item
(
    source_spec_item_id         STRING          NOT NULL    COMMENT 'LIMS spec item natural key',
    source_specification_id     STRING          NOT NULL    COMMENT 'LIMS parent spec ID',
    source_batch_id             STRING          NOT NULL    COMMENT 'ETL batch ID',
    source_ingestion_timestamp  TIMESTAMP       NOT NULL    COMMENT 'Ingestion timestamp',
    record_hash                 STRING                      COMMENT 'SHA-256 for CDC',

    test_code                   STRING                      COMMENT 'Test code (cleansed)',
    test_name                   STRING          NOT NULL    COMMENT 'Test name (trimmed)',
    analyte_code                STRING                      COMMENT 'Analyte code (cleansed — ref ERD)',
    parameter_name              STRING                      COMMENT 'Parameter name (trimmed — ref ERD)',
    test_category_code          STRING                      COMMENT 'Mapped category: PHY|CHE|IMP|MIC|BIO|STER|PACK',
    test_category_name          STRING                      COMMENT 'Category display name',
    test_subcategory            STRING                      COMMENT 'Subcategory (cleansed)',
    uom_code                    STRING                      COMMENT 'Unit code (standardized against dim_uom)',
    criticality_code            STRING                      COMMENT 'Mapped criticality: CQA|CCQA|NCQA|KQA|REPORT (ref ERD)',
    sequence_number             INT                         COMMENT 'Sequence order (cast from STRING)',
    reporting_type              STRING                      COMMENT 'Mapped: NUMERIC|PASS_FAIL|TEXT|REPORT_ONLY',
    result_precision            INT                         COMMENT 'Decimal places (cast)',
    is_required                 BOOLEAN                     COMMENT 'Mandatory flag (Y/N/TRUE/FALSE → BOOLEAN)',
    compendia_test_ref          STRING                      COMMENT 'Compendia reference (cleaned)',
    stage_applicability         STRING                      COMMENT 'Mapped: RELEASE|STABILITY|IPC|BOTH',
    test_method_id_lims         STRING                      COMMENT 'LIMS method ID (pre-MDM resolution)',

    -- Data quality
    dq_category_mapped          BOOLEAN                     COMMENT 'TRUE if category successfully mapped',
    dq_criticality_mapped       BOOLEAN                     COMMENT 'TRUE if criticality successfully mapped',
    dq_type_cast_error          BOOLEAN                     COMMENT 'TRUE if any type cast failed',

    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'L2.1 load timestamp',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Latest version flag'
)
USING DELTA
PARTITIONED BY (test_category_code)
COMMENT 'L2.1 Source conform: LIMS specification items. Includes analyte_code, criticality mapping per ref ERD. Deduplicated, typed.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.1',
    'quality.source'                    = 'LIMS',
    'quality.source_raw_table'          = 'l1_raw.raw_lims_spec_item'
);
