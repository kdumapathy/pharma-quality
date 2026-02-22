-- =============================================================================
-- Table  : raw_lims_specification
-- Schema : l1_raw
-- Layer  : L1 — Raw Layer (Immutable ingestion zone)
-- Source : LIMS (Laboratory Information Management System)
-- Grain  : One row per ingest event (append-only; exact copy of source)
-- Purpose: Immutable landing zone for specification header data from LIMS.
--          No transformations applied. Schema reflects source system columns
--          as-is. All fields stored as STRING to avoid type-cast failures.
--          Duplicate source records may exist (idempotent ingestion).
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l1_raw.raw_lims_specification
(
    -- -------------------------------------------------------------------------
    -- Ingestion Metadata (added by ETL pipeline, not from source)
    -- -------------------------------------------------------------------------
    _ingestion_id               STRING          NOT NULL    COMMENT 'UUID assigned by ingestion pipeline for deduplication',
    _source_system              STRING          NOT NULL    COMMENT 'Source system tag (LIMS)',
    _source_file                STRING                      COMMENT 'Source file or API endpoint path',
    _batch_id                   STRING          NOT NULL    COMMENT 'ETL batch identifier (e.g., date + run number)',
    _ingestion_timestamp        TIMESTAMP       NOT NULL    COMMENT 'UTC timestamp when record was ingested',
    _record_hash                STRING                      COMMENT 'SHA-256 hash of source payload for change detection',

    -- -------------------------------------------------------------------------
    -- Source Columns — LIMS Specification Header (stored as STRING)
    -- -------------------------------------------------------------------------
    specification_id            STRING                      COMMENT 'LIMS specification record ID (PK in source)',
    spec_number                 STRING                      COMMENT 'Specification document number',
    spec_version                STRING                      COMMENT 'Version (e.g., 1, 1.0, V1)',
    spec_title                  STRING                      COMMENT 'Specification title',
    spec_type                   STRING                      COMMENT 'Type code from LIMS (may differ from unified codes)',
    product_id                  STRING                      COMMENT 'Product ID from LIMS (foreign key to LIMS product table)',
    product_name                STRING                      COMMENT 'Product name as stored in LIMS',
    material_id                 STRING                      COMMENT 'Material / substance ID in LIMS',
    material_name               STRING                      COMMENT 'Material name in LIMS',
    site_id                     STRING                      COMMENT 'Site ID from LIMS',
    site_name                   STRING                      COMMENT 'Site name from LIMS',
    market_region               STRING                      COMMENT 'Market or region code from LIMS (may be non-standard)',
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
COMMENT 'L1 Raw: LIMS specification header. Immutable append-only. Exact copy of source, all STRING. No business transformations. Supports CDC via _record_hash and _batch_id.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L1',
    'quality.source'                    = 'LIMS',
    'quality.table_type'                = 'raw_ingest',
    'quality.transformation'            = 'none'
);
