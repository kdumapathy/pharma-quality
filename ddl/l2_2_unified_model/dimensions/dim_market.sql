CREATE TABLE IF NOT EXISTS l2_2_unified_model.dim_market
(
    market_key                  BIGINT          NOT NULL    COMMENT 'Surrogate key',
    market_code                 STRING          NOT NULL    COMMENT 'Market code (ISO alpha-2 or region code)',
    market_name                 STRING          NOT NULL    COMMENT 'Market name',
    country_code                STRING                      COMMENT 'ISO 3166-1 alpha-2 country code',
    country_name                STRING                      COMMENT 'Country name',
    region_code                 STRING          NOT NULL    COMMENT 'Region: US|EU|JP|CN|ROW',
    region_name                 STRING          NOT NULL    COMMENT 'Region display name',
    regulatory_authority        STRING                      COMMENT 'Primary regulatory authority (FDA, EMA, PMDA, NMPA)',
    primary_pharmacopoeia       STRING                      COMMENT 'Primary pharmacopoeia: USP|EP|JP|BP|IP',
    market_status               STRING                      COMMENT 'MA status: APPROVED|PENDING|FILED|WITHDRAWN|NEVER_FILED',
    marketing_auth_number       STRING                      COMMENT 'Marketing authorization / registration number',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 MDM: Market/country dimension with marketing authorization.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'mdm_dimension'
);
