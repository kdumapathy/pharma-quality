CREATE OR REPLACE TABLE l2_2_unified_model.dim_batch
(
    batch_key                   BIGINT          NOT NULL    COMMENT 'Surrogate key',
    batch_number                STRING          NOT NULL    COMMENT 'Manufacturing batch / lot number',
    batch_system_id             STRING                      COMMENT 'Batch system ID (ERP/MES)',
    product_key                 BIGINT                      COMMENT 'FK to dim_product',
    site_key                    BIGINT                      COMMENT 'FK to dim_site (manufacturing site)',
    batch_type                  STRING                      COMMENT 'Type: DEVELOPMENT|PILOT|EXHIBIT|REGISTRATION|COMMERCIAL|VALIDATION|SCALE_UP',
    manufacturing_date          DATE                        COMMENT 'Manufacturing / completion date',
    expiry_date                 DATE                        COMMENT 'Expiry date based on stability data',
    retest_date                 DATE                        COMMENT 'Retest date (for drug substance)',
    batch_size                  DECIMAL(18, 4)              COMMENT 'Batch size value',
    batch_size_unit             STRING                      COMMENT 'Batch size unit (kg, L, units, doses)',
    yield_pct                   DECIMAL(8, 4)               COMMENT 'Batch yield percentage',
    batch_status                STRING                      COMMENT 'Disposition: RELEASED|QUARANTINE|REJECTED|RECALLED|PENDING',
    disposition_date            DATE                        COMMENT 'QA disposition date',
    packaging_configuration     STRING                      COMMENT 'Primary packaging configuration',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 Dimension: Manufacturing batch / lot with GMP disposition.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'analytical_results',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'dimension'
);
