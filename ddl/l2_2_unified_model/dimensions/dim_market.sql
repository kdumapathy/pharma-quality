-- =============================================================================
-- Table  : dim_market
-- Schema : l2_2_spec_unified
-- Layer  : L2.2 — Unified Data Model
-- Source : specification_data_model_30-jan.html → MARKET entity
-- Grain  : One row per market / country (MDM-mastered)
-- Purpose: Captures the market (country) for which a specification is
--          registered and applicable. Enables country-level multi-regional
--          specification management and CTD filing tracking.
--          Relationship: MARKET ||--o{ SPECIFICATION_HEADER : applicable_for
--
--          Note: Complements (not replaces) dim_regulatory_context.
--          dim_market = country-level (where product is sold/registered)
--          dim_regulatory_context = filing context (NDA, MAA, submission type)
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_market
(
    -- -------------------------------------------------------------------------
    -- Keys
    -- -------------------------------------------------------------------------
    market_key                  BIGINT          NOT NULL    GENERATED ALWAYS AS IDENTITY
                                                            COMMENT 'Surrogate primary key',
    market_id                   STRING          NOT NULL    COMMENT 'Natural / business key from source (MDM)',

    -- -------------------------------------------------------------------------
    -- Country / Market Identity
    -- -------------------------------------------------------------------------
    country_code                STRING          NOT NULL    COMMENT 'ISO 3166-1 alpha-2 country code (e.g., US, GB, DE, JP, IN)',
    country_name                STRING          NOT NULL    COMMENT 'Country full name',

    -- region_code groups countries for reporting and regulatory alignment
    -- region_code values: US, EU, JP, CA, AU, CH, IN, CN, ROW, GLOBAL
    region_code                 STRING                      COMMENT 'Reporting region: US|EU|JP|CA|AU|CH|IN|CN|ROW|GLOBAL',
    region_name                 STRING                      COMMENT 'Region display name',

    -- -------------------------------------------------------------------------
    -- Regulatory Authority
    -- -------------------------------------------------------------------------
    regulatory_body             STRING                      COMMENT 'National regulatory authority (e.g., FDA, EMA, PMDA, CDSCO, TGA, Swissmedic)',

    -- -------------------------------------------------------------------------
    -- Marketing Authorisation
    -- market_status values:
    --   APPROVED    = Marketing authorisation granted
    --   PENDING     = Application under review
    --   FILED       = Dossier filed, not yet approved
    --   WITHDRAWN   = Authorisation withdrawn
    --   NEVER_FILED = Product not filed in this market
    -- -------------------------------------------------------------------------
    market_status               STRING                      COMMENT 'MA status: APPROVED|PENDING|FILED|WITHDRAWN|NEVER_FILED',
    marketing_auth_number       STRING                      COMMENT 'Marketing authorisation / registration number',
    marketing_auth_date         DATE                        COMMENT 'Date marketing authorisation was granted',
    marketing_auth_expiry_date  DATE                        COMMENT 'MA renewal / expiry date (where applicable)',

    -- -------------------------------------------------------------------------
    -- Pharmacopoeia Applicable
    -- Which compendia applies in this market (for compendia-based specs)
    -- -------------------------------------------------------------------------
    primary_pharmacopoeia       STRING                      COMMENT 'Applicable pharmacopoeia: USP|EP|JP|BP|IP|CP|TGA',

    -- -------------------------------------------------------------------------
    -- Source / MDM
    -- -------------------------------------------------------------------------
    source_system_code          STRING          NOT NULL    COMMENT 'Source: MDM|SAP|REGULATORY_DB|MANUAL',
    source_system_id            STRING                      COMMENT 'Original record ID in source system',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'ETL batch load timestamp (UTC)',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'TRUE = current active record'
)
USING DELTA
COMMENT 'L2.2 Market / country dimension. MDM-mastered. Maps to MARKET entity from reference ERD (specification_data_model_30-jan). One row per country/market. Linked to dim_specification via market_key FK. Complements dim_regulatory_context (filing context).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'mdm_dimension',
    'quality.grain'                     = 'market_country',
    'quality.source_model'              = 'MARKET (specification_data_model_30-jan.html)'
);
