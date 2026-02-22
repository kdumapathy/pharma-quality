-- =============================================================================
-- Table  : dim_specification_item
-- Schema : l2_2_spec_unified
-- Layer  : L2.2 — Unified Data Model (Business Conform Layer)
-- Domain : Pharmaceutical Quality — Specifications
-- Grain  : One row per test / item per specification version
-- Ref    : specification_data_model_30-jan.html → SPECIFICATION_ITEM
-- Changes: v2 — added analyte_code (ref ERD field), criticality (CQA flag).
-- Purpose: Individual test/item rows within a specification. Captures the test
--          identity, category, method linkage, and reporting attributes.
--          Limit values are stored separately in fact_specification_limit.
-- CTD    : Individual rows correspond to rows in CTD 3.2.S.4.1 / 3.2.P.5.1
--          specification tables.
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_specification_item
(
    -- -------------------------------------------------------------------------
    -- Keys
    -- -------------------------------------------------------------------------
    spec_item_key               BIGINT          NOT NULL    GENERATED ALWAYS AS IDENTITY
                                                            COMMENT 'Surrogate primary key (system-generated)',

    spec_item_id                STRING          NOT NULL    COMMENT 'Natural / business key from source system',

    -- -------------------------------------------------------------------------
    -- Parent Specification
    -- -------------------------------------------------------------------------
    spec_key                    BIGINT          NOT NULL    COMMENT 'FK to dim_specification (parent specification)',

    -- -------------------------------------------------------------------------
    -- Method & Unit Linkage
    -- -------------------------------------------------------------------------
    test_method_key             BIGINT                      COMMENT 'FK to dim_test_method (analytical method used)',
    uom_key                     BIGINT                      COMMENT 'FK to dim_uom (primary result unit of measure)',

    -- -------------------------------------------------------------------------
    -- Test Identity
    -- -------------------------------------------------------------------------
    test_code                   STRING                      COMMENT 'Internal test/item code (e.g., ASS-001, DIS-002)',
    test_name                   STRING          NOT NULL    COMMENT 'Test name as appears in specification (e.g., Assay, Appearance, Dissolution)',
    test_description            STRING                      COMMENT 'Detailed description of the test parameter',

    -- -------------------------------------------------------------------------
    -- Test Category
    -- test_category_code values:
    --   PHY  = Physical (Appearance, pH, Viscosity, Particle Size)
    --   CHE  = Chemical (Assay, Related Substances, Water Content)
    --   IMP  = Impurity (Organic, Inorganic, Residual Solvents, Elemental)
    --   MIC  = Microbiological (Bioburden, Microbial Limits, TAMC, TYMC)
    --   BIO  = Biological (Potency, Sterility, Endotoxin, Pyrogen)
    --   STER = Sterility
    --   PACK = Packaging / Container Closure
    -- -------------------------------------------------------------------------
    test_category_code          STRING                      COMMENT 'Category code: PHY|CHE|IMP|MIC|BIO|STER|PACK',
    test_category_name          STRING                      COMMENT 'Category display name',
    test_subcategory            STRING                      COMMENT 'Subcategory (e.g., Related Substances, Residual Solvents, Heavy Metals, Dissolution)',

    -- -------------------------------------------------------------------------
    -- Analyte & Criticality (from ref ERD: SPECIFICATION_ITEM)
    -- analyte_code: identifies the specific substance/analyte being measured.
    --   For multi-analyte tests (e.g., Related Substances), each impurity has
    --   its own spec_item row with a distinct analyte_code.
    --   Examples: ATV (Atorvastatin), IMP-A (Impurity A), TOTAL-IMP (Total)
    --
    -- criticality: Quality attribute classification per ICH Q8 / Q9 / Q10:
    --   CQA    = Critical Quality Attribute (directly impacts safety/efficacy)
    --   CCQA   = Critical Clinical Quality Attribute
    --   NCQA   = Non-Critical Quality Attribute
    --   KQA    = Key Quality Attribute (important but not critical)
    --   REPORT = Report-only; no criticality designation
    -- -------------------------------------------------------------------------
    analyte_code                STRING                      COMMENT 'Specific analyte / substance code being measured (ref ERD: analyte_code)',
    criticality                 STRING                      COMMENT 'Quality attribute criticality: CQA|CCQA|NCQA|KQA|REPORT (ref ERD: criticality)',

    -- -------------------------------------------------------------------------
    -- Ordering
    -- -------------------------------------------------------------------------
    sequence_number             INT                         COMMENT 'Display / reporting sequence order within the specification',

    -- -------------------------------------------------------------------------
    -- Test Properties
    -- -------------------------------------------------------------------------
    is_required                 BOOLEAN                     COMMENT 'TRUE = mandatory test; FALSE = conditional or optional test',

    -- reporting_type values:
    --   NUMERIC     = Numeric result with defined limit
    --   PASS_FAIL   = Binary pass/fail result
    --   TEXT        = Text-based conformance result
    --   REPORT_ONLY = Result reported for information only, no limit
    reporting_type              STRING          NOT NULL    COMMENT 'Result type: NUMERIC|PASS_FAIL|TEXT|REPORT_ONLY',

    result_precision            INT                         COMMENT 'Decimal places for numeric result reporting',

    -- -------------------------------------------------------------------------
    -- Compendia Reference
    -- -------------------------------------------------------------------------
    compendia_test_ref          STRING                      COMMENT 'Official compendia test reference (e.g., USP <711> Dissolution, EP 2.9.3)',
    is_compendial               BOOLEAN                     COMMENT 'TRUE if the test and/or limit is sourced from official pharmacopoeia',

    -- -------------------------------------------------------------------------
    -- Stage Applicability
    -- stage_applicability values:
    --   RELEASE    = Applied at batch release only
    --   STABILITY  = Applied during stability studies only
    --   IPC        = In-process control during manufacturing
    --   BOTH       = Applied at release and stability
    -- -------------------------------------------------------------------------
    stage_applicability         STRING                      COMMENT 'When this test applies: RELEASE|STABILITY|IPC|BOTH',

    -- -------------------------------------------------------------------------
    -- Source System Provenance
    -- -------------------------------------------------------------------------
    source_system_code          STRING          NOT NULL    COMMENT 'Source system identifier: LIMS|SAP|VAULT|MANUAL',
    source_system_id            STRING                      COMMENT 'Original record identifier in source system',

    -- -------------------------------------------------------------------------
    -- SCD Type 2 Tracking
    -- -------------------------------------------------------------------------
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'ETL batch load timestamp (UTC)',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'TRUE = current/active row; FALSE = historical row (SCD2)'
)
USING DELTA
PARTITIONED BY (test_category_code)
COMMENT 'L2.2 Specification item / test dimension. One row per test per specification version. Captures test identity, category, method, and reporting attributes. Limit values stored separately in fact_specification_limit.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.scd_type'                  = '2',
    'quality.grain'                     = 'test_per_specification_version'
);

-- OPTIMIZE l2_2_spec_unified.dim_specification_item ZORDER BY (spec_key, test_code);

-- -------------------------------------------------------------------------
-- TEST_CATEGORY_CODE Reference Values
-- -------------------------------------------------------------------------
-- PHY    Physical
-- CHE    Chemical
-- IMP    Impurity (organic, inorganic, elemental, residual solvent)
-- MIC    Microbiological
-- BIO    Biological
-- STER   Sterility
-- PACK   Packaging / Container Closure

-- -------------------------------------------------------------------------
-- Commonly Observed Tests by Category
-- -------------------------------------------------------------------------
-- PHY: Appearance, pH, Viscosity, Specific Gravity, Osmolality,
--      Particle Size, Reconstitution Time, Color, Clarity
-- CHE: Assay (HPLC), Water Content (KF), Loss on Drying, Residue on Ignition,
--      Identification (IR/HPLC/UV), Uniformity of Dosage Units
-- IMP: Related Substances, Degradation Products, Residual Solvents,
--      Heavy Metals, Elemental Impurities (ICH Q3D)
-- MIC: Total Aerobic Microbial Count (TAMC), Total Yeast & Mold Count (TYMC),
--      Bile-tolerant gram-negative bacteria, Staphylococcus aureus
-- BIO: Sterility, Bacterial Endotoxins (LAL), Potency (Bioassay)
-- PACK: Container Closure Integrity, Extractables & Leachables
