CREATE OR REPLACE TABLE l3_data_product.obt_acceptance_criteria
(
    obt_ac_key                  BIGINT          NOT NULL    COMMENT 'Surrogate key',

    -- Specification
    spec_number                 STRING          NOT NULL    COMMENT 'Specification number',
    spec_version                STRING          NOT NULL    COMMENT 'Specification version',
    spec_type_code              STRING          NOT NULL    COMMENT 'Type code',
    status_code                 STRING          NOT NULL    COMMENT 'Status code',
    stage_code                  STRING                      COMMENT 'Stage code',

    -- Product & Material
    product_name                STRING                      COMMENT 'Product name',
    material_name               STRING                      COMMENT 'Material name',
    dosage_form                 STRING                      COMMENT 'Dosage form',
    strength                    STRING                      COMMENT 'Strength',

    -- Test / Item
    test_name                   STRING          NOT NULL    COMMENT 'Test name',
    test_code                   STRING                      COMMENT 'Test code',
    test_category_code          STRING                      COMMENT 'Test category',
    criticality                 STRING                      COMMENT 'CQA classification',
    uom_code                    STRING                      COMMENT 'Unit of measure',
    sequence_number             INT                         COMMENT 'Display order',
    reporting_type              STRING                      COMMENT 'Reporting type',
    is_required                 BOOLEAN                     COMMENT 'Mandatory flag',

    -- AC limits (pivoted)
    ac_lower_limit              DECIMAL(18, 6)              COMMENT 'Acceptance criteria lower limit',
    ac_upper_limit              DECIMAL(18, 6)              COMMENT 'Acceptance criteria upper limit',
    ac_target_value             DECIMAL(18, 6)              COMMENT 'AC target value',
    ac_width                    DECIMAL(18, 6)              COMMENT 'AC range width (upper - lower)',

    -- NOR limits (pivoted)
    nor_lower_limit             DECIMAL(18, 6)              COMMENT 'Normal operating range lower limit',
    nor_upper_limit             DECIMAL(18, 6)              COMMENT 'Normal operating range upper limit',
    nor_target_value            DECIMAL(18, 6)              COMMENT 'NOR target value',
    nor_width                   DECIMAL(18, 6)              COMMENT 'NOR range width',

    -- PAR limits (pivoted)
    par_lower_limit             DECIMAL(18, 6)              COMMENT 'Proven acceptable range lower limit',
    par_upper_limit             DECIMAL(18, 6)              COMMENT 'Proven acceptable range upper limit',
    par_target_value            DECIMAL(18, 6)              COMMENT 'PAR target value',
    par_width                   DECIMAL(18, 6)              COMMENT 'PAR range width',

    -- Hierarchy metrics
    nor_tightness_pct           DECIMAL(8, 4)               COMMENT 'NOR width / AC width x 100 (tightness %)',
    par_vs_ac_factor            DECIMAL(8, 4)               COMMENT 'PAR width / AC width (ratio)',
    is_hierarchy_valid          BOOLEAN                     COMMENT 'TRUE if PAR >= AC >= NOR holds',

    -- Metadata
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Current version flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (spec_type_code)
COMMENT 'L3 OBT: Acceptance criteria with pivoted limits and hierarchy metrics. Grain = spec x item.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L3',
    'quality.table_type'                = 'obt'
);
