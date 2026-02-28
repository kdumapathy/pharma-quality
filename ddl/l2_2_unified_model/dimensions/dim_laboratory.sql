CREATE TABLE IF NOT EXISTS l2_2_unified_model.dim_laboratory
(
    laboratory_key              BIGINT          NOT NULL    COMMENT 'Surrogate key',
    laboratory_id               STRING          NOT NULL    COMMENT 'Laboratory ID',
    laboratory_name             STRING          NOT NULL    COMMENT 'Laboratory name',
    laboratory_type             STRING                      COMMENT 'Type: QC|R_AND_D|STABILITY|MICROBIOLOGY|CRO|CONTRACT',
    site_key                    BIGINT                      COMMENT 'FK to dim_site (parent site)',
    accreditation_status        STRING                      COMMENT 'Status: ISO_17025|GLP|GMP_COMPLIANT|PENDING',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 Dimension: QC / analytical laboratory.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'analytical_results',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'dimension'
);
