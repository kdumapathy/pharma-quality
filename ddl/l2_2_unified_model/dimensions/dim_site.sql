CREATE TABLE IF NOT EXISTS l2_2_unified_model.dim_site
(
    site_key                    BIGINT          NOT NULL    COMMENT 'Surrogate key',
    site_id                     STRING          NOT NULL    COMMENT 'MDM-resolved site ID',
    site_code                   STRING                      COMMENT 'Short site code (e.g., SPR-01, MIL-02)',
    site_name                   STRING          NOT NULL    COMMENT 'Site name',
    site_type                   STRING                      COMMENT 'Type: MANUFACTURING|QC_TESTING|PACKAGING|CMO|CRO|DISTRIBUTION',
    address_line                STRING                      COMMENT 'Street address',
    city                        STRING                      COMMENT 'City',
    state_province              STRING                      COMMENT 'State or province',
    country_code                STRING                      COMMENT 'ISO 3166-1 alpha-2 country code (US, DE, JP, IN)',
    country_name                STRING                      COMMENT 'Country name',
    regulatory_region           STRING                      COMMENT 'Regulatory authority region: FDA|EMA|PMDA|CDSCO|TGA|ANVISA',
    gmp_status                  STRING                      COMMENT 'GMP status: APPROVED|PENDING|WARNING_LETTER|IMPORT_ALERT',
    gmp_certificate_number      STRING                      COMMENT 'GMP certificate or manufacturing license number',
    fda_fei_number              STRING                      COMMENT 'FDA Facility Establishment Identifier (FEI)',
    last_inspection_date        DATE                        COMMENT 'Most recent regulatory inspection date',
    last_inspection_outcome     STRING                      COMMENT 'Outcome: NAI|VAI|OAI (FDA) or SATISFACTORY|NON_COMPLIANT',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 MDM: Manufacturing/testing site dimension with GMP status.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'mdm_dimension'
);
