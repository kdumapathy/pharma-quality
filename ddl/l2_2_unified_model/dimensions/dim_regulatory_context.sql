-- =============================================================================
-- Table  : dim_regulatory_context
-- Schema : l2_2_spec_unified
-- Layer  : L2.2 — Unified Data Model (Business Conform Layer)
-- Domain : Pharmaceutical Quality — Specifications
-- Grain  : One row per regulatory region / submission type combination
-- Purpose: Reference dimension capturing the regulatory filing context for each
--          specification. Enables filtering and grouping of specifications by
--          region, regulatory body, submission type, and applicable guidelines.
--          Directly supports CTD Module 3 structure alignment.
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_regulatory_context
(
    -- -------------------------------------------------------------------------
    -- Keys
    -- -------------------------------------------------------------------------
    regulatory_context_key      BIGINT          NOT NULL    GENERATED ALWAYS AS IDENTITY
                                                            COMMENT 'Surrogate primary key (system-generated)',

    regulatory_context_code     STRING          NOT NULL    COMMENT 'Composite business code (e.g., US-NDA, EU-MAA, JP-JNDA, GLOBAL-CTD)',

    -- -------------------------------------------------------------------------
    -- Geographic Region
    -- region_code values:
    --   US     = United States
    --   EU     = European Union
    --   JP     = Japan
    --   CA     = Canada
    --   CH     = Switzerland
    --   AU     = Australia
    --   ROW    = Rest of World
    --   GLOBAL = All regions (ICH harmonized context)
    -- -------------------------------------------------------------------------
    region_code                 STRING                      COMMENT 'Region code: US|EU|JP|CA|CH|AU|ROW|GLOBAL',
    region_name                 STRING                      COMMENT 'Region full name',

    -- -------------------------------------------------------------------------
    -- Regulatory Body
    -- -------------------------------------------------------------------------
    regulatory_body             STRING                      COMMENT 'Regulatory authority (e.g., FDA, EMA, PMDA, Health Canada, TGA)',

    -- -------------------------------------------------------------------------
    -- Submission Type
    -- submission_type values:
    --   NDA    = New Drug Application (US FDA, small molecule)
    --   ANDA   = Abbreviated NDA (US FDA, generic)
    --   BLA    = Biologics License Application (US FDA, biologics)
    --   MAA    = Marketing Authorisation Application (EU EMA)
    --   JNDA   = Japanese NDA (PMDA)
    --   CTD    = Common Technical Document (ICH harmonized)
    --   IND    = Investigational New Drug Application
    --   IMPD   = Investigational Medicinal Product Dossier (EU clinical)
    -- -------------------------------------------------------------------------
    submission_type             STRING                      COMMENT 'Submission type: NDA|ANDA|BLA|MAA|JNDA|CTD|IND|IMPD',

    -- -------------------------------------------------------------------------
    -- Applicable Guidelines
    -- -------------------------------------------------------------------------
    guideline_code              STRING                      COMMENT 'Primary applicable guideline code (e.g., ICH Q6A, ICH Q6B, USP, EP)',
    guideline_name              STRING                      COMMENT 'Primary applicable guideline full name',

    -- -------------------------------------------------------------------------
    -- CTD Structure Mapping
    -- -------------------------------------------------------------------------
    ctd_module                  STRING                      COMMENT 'CTD module (typically Module 3 for quality)',
    ctd_section                 STRING                      COMMENT 'Default CTD section for this context (e.g., 3.2.S.4.1, 3.2.P.5.1)',

    -- -------------------------------------------------------------------------
    -- Audit
    -- -------------------------------------------------------------------------
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'ETL batch load timestamp (UTC)'
)
USING DELTA
COMMENT 'L2.2 Regulatory context reference dimension. Classifies specifications by filing region, regulatory body, submission type, and applicable ICH/compendia guidelines. Supports multi-regional CTD alignment.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'reference',
    'quality.grain'                     = 'regulatory_context'
);

-- =============================================================================
-- SEED DATA — Common Regulatory Contexts
-- =============================================================================

INSERT INTO l2_2_spec_unified.dim_regulatory_context
    (regulatory_context_code, region_code, region_name, regulatory_body,
     submission_type, guideline_code, guideline_name, ctd_module, ctd_section, load_timestamp)
VALUES
    ('US-NDA',      'US', 'United States',      'FDA',           'NDA',   'ICH Q6A', 'ICH Q6A: Specifications for New Drug Substances and Products (Chemical)',     '3', '3.2.S.4.1', current_timestamp()),
    ('US-ANDA',     'US', 'United States',      'FDA',           'ANDA',  'ICH Q6A', 'ICH Q6A: Specifications for New Drug Substances and Products (Chemical)',     '3', '3.2.S.4.1', current_timestamp()),
    ('US-BLA',      'US', 'United States',      'FDA',           'BLA',   'ICH Q6B', 'ICH Q6B: Specifications for Biotechnological/Biological Products',            '3', '3.2.S.4.1', current_timestamp()),
    ('EU-MAA',      'EU', 'European Union',     'EMA',           'MAA',   'ICH Q6A', 'ICH Q6A: Specifications for New Drug Substances and Products (Chemical)',     '3', '3.2.S.4.1', current_timestamp()),
    ('JP-JNDA',     'JP', 'Japan',              'PMDA',          'JNDA',  'ICH Q6A', 'ICH Q6A: Specifications for New Drug Substances and Products (Chemical)',     '3', '3.2.S.4.1', current_timestamp()),
    ('CA-NDS',      'CA', 'Canada',             'Health Canada', 'NDS',   'ICH Q6A', 'ICH Q6A: Specifications for New Drug Substances and Products (Chemical)',     '3', '3.2.S.4.1', current_timestamp()),
    ('GLOBAL-CTD',  'GLOBAL', 'Global (ICH)',   'ICH',           'CTD',   'ICH Q6A', 'ICH Q6A: Specifications for New Drug Substances and Products (Chemical)',     '3', '3.2.S.4.1', current_timestamp()),
    ('US-IND',      'US', 'United States',      'FDA',           'IND',   'ICH Q6A', 'ICH Q6A (Development stage application)',                                    '3', '3.2.S.4.1', current_timestamp()),
    ('EU-IMPD',     'EU', 'European Union',     'EMA',           'IMPD',  'ICH Q6A', 'ICH Q6A (Clinical stage application)',                                       '3', '3.2.S.4.1', current_timestamp());
