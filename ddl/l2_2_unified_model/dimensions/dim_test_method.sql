CREATE TABLE IF NOT EXISTS l2_2_unified_model.dim_test_method
(
    test_method_key             BIGINT          NOT NULL    COMMENT 'Surrogate key',
    test_method_id              STRING          NOT NULL    COMMENT 'MDM-resolved method ID',
    test_method_name            STRING          NOT NULL    COMMENT 'Method name (e.g., Assay by HPLC)',
    test_method_number          STRING                      COMMENT 'Method document number (e.g., TM-HPLC-001)',
    test_method_version         STRING                      COMMENT 'Method version',
    method_type                 STRING                      COMMENT 'Type: COMPENDIAL|COMPENDIAL_MODIFIED|IN_HOUSE|TRANSFER',
    analytical_technique        STRING                      COMMENT 'Technique: HPLC|GC|UV_VIS|IR|KF|DISSOLUTION|HARDNESS|PSD|LAL',
    compendia_reference         STRING                      COMMENT 'Compendia reference (USP <621>, EP 2.2.29)',
    detection_limit             DECIMAL(18, 6)              COMMENT 'Limit of Detection (LOD) per ICH Q2',
    quantitation_limit          DECIMAL(18, 6)              COMMENT 'Limit of Quantitation (LOQ) per ICH Q2',
    validation_status           STRING                      COMMENT 'Status: VALIDATED|VERIFIED|QUALIFIED|TRANSFER_COMPLETE|IN_PROGRESS',
    validation_date             DATE                        COMMENT 'ICH Q2(R1) validation completion date',
    is_validated                BOOLEAN                     COMMENT 'Validation status flag',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    effective_from              DATE                        COMMENT 'Method effective start date',
    effective_to                DATE                        COMMENT 'Method effective end date',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 MDM: Analytical test method dimension per ICH Q2(R1).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'mdm_dimension'
);
