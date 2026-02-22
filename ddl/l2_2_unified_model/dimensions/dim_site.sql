-- =============================================================================
-- Table  : dim_site
-- Schema : l2_2_spec_unified
-- Layer  : L2.2 — Unified Data Model
-- Source : specification_data_model_30-jan.html → SITE entity
-- Grain  : One row per manufacturing / testing site (MDM-mastered)
-- Purpose: Captures the physical site at which a specification is applicable
--          (manufacturing, QC testing, packaging). Added from the reference
--          ERD: SITE ||--o{ SPECIFICATION_HEADER : applicable_at
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_site
(
    -- -------------------------------------------------------------------------
    -- Keys
    -- -------------------------------------------------------------------------
    site_key                    BIGINT          NOT NULL    GENERATED ALWAYS AS IDENTITY
                                                            COMMENT 'Surrogate primary key',
    site_id                     STRING          NOT NULL    COMMENT 'Natural / business key from source (MDM)',

    -- -------------------------------------------------------------------------
    -- Site Identity
    -- -------------------------------------------------------------------------
    site_code                   STRING          NOT NULL    COMMENT 'Short site code used in specifications (e.g., NJ01, UK02, IN03)',
    site_name                   STRING          NOT NULL    COMMENT 'Full site name',

    -- site_type values:
    --   MANUFACTURING  = Primary drug manufacturing site
    --   QC_TESTING     = Quality control / analytical testing lab
    --   PACKAGING      = Secondary packaging site
    --   STORAGE        = Warehouse / distribution center
    --   CRO            = Contract Research Organisation
    --   CMO            = Contract Manufacturing Organisation
    site_type                   STRING                      COMMENT 'Site function: MANUFACTURING|QC_TESTING|PACKAGING|STORAGE|CRO|CMO',

    -- -------------------------------------------------------------------------
    -- Location
    -- -------------------------------------------------------------------------
    address_line                STRING                      COMMENT 'Street address',
    city                        STRING                      COMMENT 'City',
    state_province              STRING                      COMMENT 'State or province',
    country_code                STRING                      COMMENT 'ISO 3166-1 alpha-2 country code (e.g., US, GB, DE, IN)',
    country_name                STRING                      COMMENT 'Country full name',

    -- -------------------------------------------------------------------------
    -- Regulatory Attribution
    -- regulatory_region: which regulatory authority governs this site
    -- -------------------------------------------------------------------------
    regulatory_region           STRING                      COMMENT 'Primary regulatory authority region: FDA|EMA|PMDA|CDSCO|TGA|ANVISA',

    -- -------------------------------------------------------------------------
    -- GMP Status (critical for regulatory use)
    -- gmp_status values:
    --   APPROVED        = Current valid GMP approval
    --   PENDING         = GMP application / renewal pending
    --   WARNING_LETTER  = FDA Warning Letter issued
    --   IMPORT_ALERT    = FDA Import Alert active
    --   RESTRICTED      = Conditional / restricted approval
    --   SUSPENDED       = GMP approval suspended
    --   DECOMMISSIONED  = Site closed / decommissioned
    -- -------------------------------------------------------------------------
    gmp_status                  STRING                      COMMENT 'GMP compliance status: APPROVED|PENDING|WARNING_LETTER|IMPORT_ALERT|RESTRICTED|SUSPENDED|DECOMMISSIONED',
    gmp_status_date             DATE                        COMMENT 'Date of most recent GMP status change',
    last_inspection_date        DATE                        COMMENT 'Date of most recent regulatory inspection',
    last_inspection_outcome     STRING                      COMMENT 'Outcome of last inspection: SATISFACTORY|VAI|OAI|NAI',

    -- -------------------------------------------------------------------------
    -- Source / MDM
    -- -------------------------------------------------------------------------
    source_system_code          STRING          NOT NULL    COMMENT 'Source: MDM|SAP|ERP|MANUAL',
    source_system_id            STRING                      COMMENT 'Original record ID in source system',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'ETL batch load timestamp (UTC)',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'TRUE = current active record'
)
USING DELTA
COMMENT 'L2.2 Manufacturing and testing site dimension. MDM-mastered. Maps to SITE entity from reference ERD (specification_data_model_30-jan). One row per site. Linked to dim_specification via site_key FK.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'mdm_dimension',
    'quality.grain'                     = 'site',
    'quality.source_model'              = 'SITE (specification_data_model_30-jan.html)'
);

-- -------------------------------------------------------------------------
-- Inspection Outcome Codes (CDER / CBER convention):
--   NAI = No Action Indicated    (best outcome)
--   VAI = Voluntary Action Indicated
--   OAI = Official Action Indicated  (worst — leads to warning letter)
-- -------------------------------------------------------------------------
