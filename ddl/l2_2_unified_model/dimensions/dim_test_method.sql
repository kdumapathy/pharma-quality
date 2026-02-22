-- =============================================================================
-- Table  : dim_test_method
-- Schema : l2_2_spec_unified
-- Layer  : L2.2 — Unified Data Model (Business Conform Layer)
-- Domain : Pharmaceutical Quality — Specifications
-- Grain  : One row per test method version
-- Purpose: Analytical test methods linked to specification items. Captures
--          method identity, type, analytical technique, and validation status.
--          Supports CTD 3.2.S.4.2 / 3.2.P.5.2 (Analytical Procedures) and
--          3.2.S.4.3 / 3.2.P.5.3 (Validation of Analytical Procedures).
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_test_method
(
    -- -------------------------------------------------------------------------
    -- Keys
    -- -------------------------------------------------------------------------
    test_method_key             BIGINT          NOT NULL    GENERATED ALWAYS AS IDENTITY
                                                            COMMENT 'Surrogate primary key (system-generated)',

    test_method_id              STRING          NOT NULL    COMMENT 'Natural / business key from source system',

    -- -------------------------------------------------------------------------
    -- Method Identity
    -- -------------------------------------------------------------------------
    test_method_number          STRING                      COMMENT 'Method document number (e.g., TM-HPLC-001, AM-KF-005)',
    test_method_name            STRING          NOT NULL    COMMENT 'Method name (e.g., Assay by HPLC, Water Content by KF)',
    test_method_version         STRING                      COMMENT 'Method version (e.g., 1.0, 2.3)',

    -- -------------------------------------------------------------------------
    -- Method Type
    -- method_type_code values:
    --   COMP     = Compendial (USP, EP, JP, BP — used as-is)
    --   COMP_MOD = Compendial Modified (compendia method with modifications)
    --   INHOUSE  = In-house developed method
    --   TRANSFER = Transferred from another site/lab
    -- -------------------------------------------------------------------------
    method_type_code            STRING                      COMMENT 'Method type: COMP|COMP_MOD|INHOUSE|TRANSFER',
    method_type_name            STRING                      COMMENT 'Method type display name',

    -- -------------------------------------------------------------------------
    -- Analytical Technique
    -- analytical_technique values (non-exhaustive):
    --   HPLC, UPLC, GC, GC-MS, ICP-MS, ICP-OES, KF (Karl Fischer),
    --   UV-VIS, FTIR, NIR, NMR, CE (Capillary Electrophoresis),
    --   DISSOLUTION, HARDNESS, DISINTEGRATION, VISUAL, PSD (Particle Size),
    --   TOC, BIOASSAY, ELISA, PCR, LAL (Limulus Amebocyte Lysate)
    -- -------------------------------------------------------------------------
    analytical_technique        STRING                      COMMENT 'Primary analytical technique (e.g., HPLC, GC, KF, UV-VIS, LAL)',
    instrument_type             STRING                      COMMENT 'Instrument or equipment type',

    -- -------------------------------------------------------------------------
    -- Compendia Reference
    -- -------------------------------------------------------------------------
    compendia_reference         STRING                      COMMENT 'Official compendia reference (e.g., USP <621>, EP 2.2.29, USP <711>)',

    -- -------------------------------------------------------------------------
    -- Validation / Qualification Status
    -- validation_status values:
    --   VALIDATED  = Full ICH Q2(R1) method validation completed
    --   QUALIFIED  = Method qualification (transfer) completed
    --   VERIFIED   = Compendial method verification completed (USP <1226>)
    --   WAIVED     = Validation formally waived (e.g., compendial, simple tests)
    --   EXEMPT     = Regulatory exemption granted
    --   IN_PROG    = Validation in progress
    -- -------------------------------------------------------------------------
    validation_status           STRING                      COMMENT 'Validation status: VALIDATED|QUALIFIED|VERIFIED|WAIVED|EXEMPT|IN_PROG',
    validation_date             DATE                        COMMENT 'Date of method validation completion or most recent revalidation',

    -- -------------------------------------------------------------------------
    -- Source System Provenance
    -- -------------------------------------------------------------------------
    source_system_code          STRING          NOT NULL    COMMENT 'Source system identifier: LIMS|SAP|VAULT|MANUAL',
    source_system_id            STRING                      COMMENT 'Original record identifier in source system',

    -- -------------------------------------------------------------------------
    -- SCD Tracking
    -- -------------------------------------------------------------------------
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'ETL batch load timestamp (UTC)',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'TRUE = current/active row; FALSE = historical row'
)
USING DELTA
COMMENT 'L2.2 Test method dimension. One row per analytical method version. Supports CTD sections 3.2.S.4.2/4.3 and 3.2.P.5.2/5.3 (Analytical Procedures and Validation).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.grain'                     = 'test_method_version'
);

-- OPTIMIZE l2_2_spec_unified.dim_test_method ZORDER BY (test_method_number, test_method_version);
