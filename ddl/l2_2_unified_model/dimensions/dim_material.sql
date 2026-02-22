-- =============================================================================
-- Table  : dim_material
-- Schema : l2_2_spec_unified
-- Layer  : L2.2 — Unified Data Model (Business Conform Layer)
-- Domain : Pharmaceutical Quality — Specifications
-- Grain  : One row per material / substance (MDM-mastered)
-- Purpose: Master Data Management (MDM) material dimension. Represents the
--          substance or material governed by a specification (API, excipient,
--          intermediate, raw material, drug product, packaging component).
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_material
(
    -- -------------------------------------------------------------------------
    -- Keys
    -- -------------------------------------------------------------------------
    material_key                BIGINT          NOT NULL    GENERATED ALWAYS AS IDENTITY
                                                            COMMENT 'Surrogate primary key (system-generated)',

    material_id                 STRING          NOT NULL    COMMENT 'MDM golden record natural key',

    -- -------------------------------------------------------------------------
    -- Material Identity
    -- -------------------------------------------------------------------------
    material_code               STRING                      COMMENT 'Internal material number (e.g., SAP Material Number, LIMS Material ID)',
    material_name               STRING          NOT NULL    COMMENT 'Material name (e.g., Atorvastatin Calcium, Microcrystalline Cellulose)',

    -- -------------------------------------------------------------------------
    -- Material Type
    -- material_type_code values:
    --   API      = Active Pharmaceutical Ingredient
    --   EXCIP    = Excipient
    --   INTERMED = Intermediate (synthesis step product)
    --   DRUG_PROD= Drug Product (finished dosage form)
    --   RM       = Raw Material (non-excipient starting material)
    --   PACK     = Packaging Component (primary/secondary)
    --   REAGENT  = Reagent / reference standard
    -- -------------------------------------------------------------------------
    material_type_code          STRING                      COMMENT 'Material type: API|EXCIP|INTERMED|DRUG_PROD|RM|PACK|REAGENT',
    material_type_name          STRING                      COMMENT 'Material type display name',

    -- -------------------------------------------------------------------------
    -- Chemical Identity (primarily for APIs and intermediates)
    -- -------------------------------------------------------------------------
    cas_number                  STRING                      COMMENT 'CAS Registry Number',
    molecular_formula           STRING                      COMMENT 'Molecular formula (e.g., C22H24F3NO2S·Ca)',
    molecular_weight            DECIMAL(10, 4)              COMMENT 'Molecular weight (g/mol)',
    structural_formula          STRING                      COMMENT 'SMILES or InChI structural formula (for APIs)',

    -- -------------------------------------------------------------------------
    -- Pharmacopoeia Grade
    -- pharmacopoeia_grade values:
    --   USP = United States Pharmacopeia
    --   EP  = European Pharmacopoeia
    --   JP  = Japanese Pharmacopoeia
    --   BP  = British Pharmacopeia
    --   ACS = American Chemical Society (reagent grade)
    --   NF  = National Formulary (excipients)
    --   FCC = Food Chemicals Codex
    --   INHOUSE = Internal grade without compendia listing
    -- -------------------------------------------------------------------------
    pharmacopoeia_grade         STRING                      COMMENT 'Pharmacopoeial grade: USP|EP|JP|BP|NF|ACS|FCC|INHOUSE',

    -- -------------------------------------------------------------------------
    -- Manufacturer (primary/preferred supplier)
    -- -------------------------------------------------------------------------
    manufacturer_name           STRING                      COMMENT 'Approved manufacturer / supplier name',
    manufacturer_code           STRING                      COMMENT 'Internal supplier/vendor code',

    -- -------------------------------------------------------------------------
    -- Source / MDM
    -- -------------------------------------------------------------------------
    source_system_code          STRING          NOT NULL    COMMENT 'Source: MDM|SAP|LIMS|MANUAL',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'ETL batch load timestamp (UTC)',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'TRUE = current/active record'
)
USING DELTA
COMMENT 'L2.2 Material / substance dimension. MDM-mastered. One row per material (API, excipient, intermediate, raw material, drug product, packaging). Linked to specifications via dim_specification.material_key.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'mdm_dimension',
    'quality.grain'                     = 'material'
);
