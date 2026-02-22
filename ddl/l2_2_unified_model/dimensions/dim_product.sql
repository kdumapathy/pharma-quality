-- =============================================================================
-- Table  : dim_product
-- Schema : l2_2_spec_unified
-- Layer  : L2.2 — Unified Data Model (Business Conform Layer)
-- Domain : Pharmaceutical Quality — Specifications
-- Grain  : One row per pharmaceutical product (MDM-mastered)
-- Purpose: Master Data Management (MDM) product dimension. Links specification
--          records to the drug product they govern. Sourced from corporate MDM.
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_product
(
    -- -------------------------------------------------------------------------
    -- Keys
    -- -------------------------------------------------------------------------
    product_key                 BIGINT          NOT NULL    GENERATED ALWAYS AS IDENTITY
                                                            COMMENT 'Surrogate primary key (system-generated)',

    product_id                  STRING          NOT NULL    COMMENT 'MDM golden record natural key',

    -- -------------------------------------------------------------------------
    -- Product Identity
    -- -------------------------------------------------------------------------
    product_code                STRING                      COMMENT 'Internal product code (may differ per system)',
    product_name                STRING          NOT NULL    COMMENT 'Drug product name (e.g., Atorvastatin Calcium Tablets 10mg)',

    -- -------------------------------------------------------------------------
    -- INN / Brand
    -- -------------------------------------------------------------------------
    inn_name                    STRING                      COMMENT 'International Nonproprietary Name (INN) / USAN',
    brand_name                  STRING                      COMMENT 'Commercial / trade name',

    -- -------------------------------------------------------------------------
    -- Pharmaceutical Form
    -- dosage_form_code values (non-exhaustive):
    --   TAB = Tablet, CAP = Capsule, INJ = Injection, SOL = Solution,
    --   SUS = Suspension, CRM = Cream, OIN = Ointment, GEL = Gel,
    --   PWD = Powder for Reconstitution, PATCH = Transdermal Patch,
    --   INH = Inhalation, SUPP = Suppository, LYOPH = Lyophilisate
    -- -------------------------------------------------------------------------
    dosage_form_code            STRING                      COMMENT 'Dosage form code: TAB|CAP|INJ|SOL|SUS|CRM|etc.',
    dosage_form_name            STRING                      COMMENT 'Dosage form display name',

    -- -------------------------------------------------------------------------
    -- Route of Administration
    -- route_of_administration values:
    --   ORAL, IV (Intravenous), IM (Intramuscular), SC (Subcutaneous),
    --   TOPICAL, INHAL (Inhalation), NASAL, OPHTHALMIC, OTIC, RECTAL, VAGINAL
    -- -------------------------------------------------------------------------
    route_of_administration     STRING                      COMMENT 'Route of administration: ORAL|IV|IM|SC|TOPICAL|INHAL|etc.',

    -- -------------------------------------------------------------------------
    -- Strength
    -- -------------------------------------------------------------------------
    strength                    STRING                      COMMENT 'Strength as text (e.g., 10 mg, 250 mg/5 mL, 0.5% w/w)',
    strength_value              DECIMAL(12, 4)              COMMENT 'Numeric strength value',
    strength_uom                STRING                      COMMENT 'Strength unit of measure (mg, mg/mL, %, IU)',

    -- -------------------------------------------------------------------------
    -- Therapeutic Classification
    -- -------------------------------------------------------------------------
    therapeutic_area            STRING                      COMMENT 'Therapeutic area (e.g., Oncology, Cardiology, CNS, Anti-Infective)',
    atc_code                    STRING                      COMMENT 'WHO ATC classification code',

    -- -------------------------------------------------------------------------
    -- Regulatory Registration
    -- -------------------------------------------------------------------------
    nda_number                  STRING                      COMMENT 'NDA / ANDA / BLA / MAA application number',
    product_status              STRING                      COMMENT 'Regulatory status: APPROVED|CLINICAL|WITHDRAWN|DISCONTINUED',

    -- -------------------------------------------------------------------------
    -- Source / MDM
    -- -------------------------------------------------------------------------
    source_system_code          STRING          NOT NULL    COMMENT 'Source: MDM|SAP|VAULT',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'ETL batch load timestamp (UTC)',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'TRUE = current/active record'
)
USING DELTA
COMMENT 'L2.2 Pharmaceutical product dimension. MDM-mastered. One row per drug product. Links specification records to the drug product they govern.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'mdm_dimension',
    'quality.grain'                     = 'product'
);
