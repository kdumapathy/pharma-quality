-- =============================================================================
-- Table  : raw_lims_spec_item
-- Schema : l1_raw
-- Layer  : L1 — Raw Layer
-- Source : LIMS — Specification Item / Test records
-- Grain  : One row per ingest event (append-only)
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l1_raw.raw_lims_spec_item
(
    -- Ingestion metadata
    _ingestion_id               STRING          NOT NULL    COMMENT 'UUID for deduplication',
    _source_system              STRING          NOT NULL    COMMENT 'LIMS',
    _batch_id                   STRING          NOT NULL    COMMENT 'ETL batch ID',
    _ingestion_timestamp        TIMESTAMP       NOT NULL    COMMENT 'UTC ingestion time',
    _record_hash                STRING                      COMMENT 'SHA-256 of source payload',

    -- Source columns (all STRING)
    spec_item_id                STRING                      COMMENT 'LIMS spec item record ID',
    specification_id            STRING                      COMMENT 'Parent spec ID (FK to raw_lims_specification)',
    test_method_id              STRING                      COMMENT 'Method ID from LIMS',
    test_code                   STRING                      COMMENT 'Test code in LIMS',
    test_name                   STRING                      COMMENT 'Test / parameter name',
    analyte_code                STRING                      COMMENT 'Analyte code (ref ERD: analyte_code)',
    parameter_name              STRING                      COMMENT 'Parameter name (ref ERD: parameter_name)',
    test_category               STRING                      COMMENT 'Category (Physical / Chemical / Microbiological)',
    test_subcategory            STRING                      COMMENT 'Subcategory',
    uom                         STRING                      COMMENT 'Unit of measure string from LIMS',
    criticality                 STRING                      COMMENT 'CQA criticality flag from LIMS (ref ERD: criticality)',
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
COMMENT 'L1 Raw: LIMS specification items. Immutable append-only. All STRING. Includes analyte_code and criticality from reference ERD.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L1',
    'quality.source'                    = 'LIMS',
    'quality.table_type'                = 'raw_ingest'
);
