CREATE OR REPLACE TABLE l2_2_unified_model.dim_material
(
    material_key                BIGINT          NOT NULL    COMMENT 'Surrogate key',
    material_id                 STRING          NOT NULL    COMMENT 'MDM-resolved material ID',
    material_name               STRING          NOT NULL    COMMENT 'Material name (INN or chemical name)',
    material_type_code          STRING                      COMMENT 'Type: API|EXCIPIENT|INTERMEDIATE|PACKAGING|REFERENCE_STD|RAW_MATERIAL',
    material_type_name          STRING                      COMMENT 'Material type display name',
    cas_number                  STRING                      COMMENT 'CAS Registry Number (e.g., 103-90-2)',
    molecular_formula           STRING                      COMMENT 'Molecular formula (e.g., C8H9NO2)',
    molecular_weight            DECIMAL(10, 4)              COMMENT 'Molecular weight in g/mol',
    inn_name                    STRING                      COMMENT 'INN (International Nonproprietary Name)',
    compendial_name             STRING                      COMMENT 'Compendial name (USP/EP/JP monograph name)',
    pharmacopoeia_grade         STRING                      COMMENT 'Grade: USP|EP|JP|NF|ACS|REAGENT|IN_HOUSE',
    grade                       STRING                      COMMENT 'Material grade (pharmaceutical, analytical, etc.)',
    supplier_name               STRING                      COMMENT 'Qualified supplier / manufacturer name',
    retest_period_months        INT                         COMMENT 'Retest period in months (ICH Q1A)',
    storage_requirements        STRING                      COMMENT 'Storage conditions for material',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 MDM: Material/substance master dimension (CMC chemistry).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'mdm_dimension'
);
