-- =============================================================================
-- Table  : raw_lims_spec_limit
-- Schema : l1_raw
-- Layer  : L1 — Raw Layer
-- Source : LIMS — Specification Limits (maps to SPECIFICATION_LIMIT_SET
--          and CONTROL_LIMITS in reference ERD)
-- Grain  : One row per ingest event (append-only)
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l1_raw.raw_lims_spec_limit
(
    -- Ingestion metadata
    _ingestion_id               STRING          NOT NULL    COMMENT 'UUID for deduplication',
    _source_system              STRING          NOT NULL    COMMENT 'LIMS',
    _batch_id                   STRING          NOT NULL    COMMENT 'ETL batch ID',
    _ingestion_timestamp        TIMESTAMP       NOT NULL    COMMENT 'UTC ingestion time',
    _record_hash                STRING                      COMMENT 'SHA-256 of source payload',

    -- Source columns (all STRING)
    limit_id                    STRING                      COMMENT 'LIMS limit record ID',
    spec_item_id                STRING                      COMMENT 'Parent spec item ID',
    specification_id            STRING                      COMMENT 'Parent spec ID',
    limit_type                  STRING                      COMMENT 'Limit type from LIMS (AC / NOR / PAR / Alert / Action / IPC)',
    comparison_operator         STRING                      COMMENT 'Operator string from LIMS (e.g., NLT, NMT, Between)',
    lower_limit                 STRING                      COMMENT 'Lower limit value (numeric as string)',
    upper_limit                 STRING                      COMMENT 'Upper limit value (numeric as string)',
    target_value                STRING                      COMMENT 'Target / nominal value',
    limit_text                  STRING                      COMMENT 'Text / qualitative limit',
    uom                         STRING                      COMMENT 'Unit of measure',
    limit_basis                 STRING                      COMMENT 'Basis (as-is / anhydrous / as-labeled)',
    stage                       STRING                      COMMENT 'Stage (Release / Stability / IPC)',
    stability_time_point        STRING                      COMMENT 'Stability time point (T0 / T6M / T12M)',
    stability_condition         STRING                      COMMENT 'Storage condition (25C60RH / 40C75RH)',
    effective_start_date        STRING                      COMMENT 'Limit effective start date (ref ERD: SPECIFICATION_LIMIT_SET)',
    effective_end_date          STRING                      COMMENT 'Limit effective end date (ref ERD: SPECIFICATION_LIMIT_SET)',
    -- Control Limits SPC fields (populated for Alert / Action type limits)
    calculation_method          STRING                      COMMENT 'SPC method (ref ERD: CONTROL_LIMITS.calculation_method)',
    sample_size                 STRING                      COMMENT 'Sample size for SPC (ref ERD: CONTROL_LIMITS.sample_size)',
    last_calculated_date        STRING                      COMMENT 'SPC recalculation date (ref ERD: CONTROL_LIMITS.last_calculated_date)',
    is_in_filing                STRING                      COMMENT 'TRUE/FALSE — appears in regulatory filing',
    regulatory_basis            STRING                      COMMENT 'Regulatory basis (ICH Q6A / USP)',
    created_date                STRING                      COMMENT 'Record creation date',
    modified_date               STRING                      COMMENT 'Last modified date'
)
USING DELTA
PARTITIONED BY (_source_system)
COMMENT 'L1 Raw: LIMS specification limits. Merges SPECIFICATION_LIMIT_SET and CONTROL_LIMITS from ref ERD. Immutable append-only. All STRING.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L1',
    'quality.source'                    = 'LIMS',
    'quality.table_type'                = 'raw_ingest'
);
