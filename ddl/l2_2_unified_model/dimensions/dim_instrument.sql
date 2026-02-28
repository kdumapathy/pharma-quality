CREATE TABLE IF NOT EXISTS l2_2_unified_model.dim_instrument
(
    instrument_key              BIGINT          NOT NULL    COMMENT 'Surrogate key',
    instrument_id               STRING          NOT NULL    COMMENT 'Instrument / equipment ID',
    instrument_name             STRING          NOT NULL    COMMENT 'Instrument name or model',
    instrument_type             STRING                      COMMENT 'Type: HPLC|GC|UV_VIS|IR|DISSOLUTION|BALANCE|PH_METER|KF|PSD|OTHER',
    serial_number               STRING                      COMMENT 'Manufacturer serial number',
    manufacturer                STRING                      COMMENT 'Instrument manufacturer (Agilent, Waters, Shimadzu)',
    qualification_status        STRING                      COMMENT 'IQ/OQ/PQ status: QUALIFIED|PENDING_OQ|PENDING_PQ|DECOMMISSIONED',
    calibration_due_date        DATE                        COMMENT 'Next calibration due date',
    location                    STRING                      COMMENT 'Lab / room location',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 Dimension: Analytical instrument / equipment with IQ/OQ/PQ status.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'analytical_results',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'dimension'
);
