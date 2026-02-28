CREATE OR REPLACE TABLE l2_2_unified_model.dspec_specification
(
    dspec_key                   BIGINT          NOT NULL    COMMENT 'Surrogate key',
    spec_key                    BIGINT          NOT NULL    COMMENT 'FK to dim_specification',
    spec_item_key               BIGINT          NOT NULL    COMMENT 'FK to dim_specification_item',

    -- Specification header (denormalized)
    spec_number                 STRING          NOT NULL    COMMENT 'Specification number',
    spec_version                STRING          NOT NULL    COMMENT 'Specification version',
    spec_title                  STRING                      COMMENT 'Specification title',
    spec_type_code              STRING          NOT NULL    COMMENT 'Type code (DS|DP|RM|EXCIP|INTERMED|IPC|CCS)',
    spec_type_name              STRING                      COMMENT 'Type name',
    product_name                STRING                      COMMENT 'Drug product name',
    material_name               STRING                      COMMENT 'Material / substance name',
    site_name                   STRING                      COMMENT 'Manufacturing / testing site',
    market_name                 STRING                      COMMENT 'Target market',
    status_code                 STRING          NOT NULL    COMMENT 'Status code',
    stage_code                  STRING                      COMMENT 'Stage code',
    strength                    STRING                      COMMENT 'Product strength',

    -- Item (denormalized)
    test_name                   STRING          NOT NULL    COMMENT 'Test / parameter name',
    test_code                   STRING                      COMMENT 'Test code',
    test_category_code          STRING                      COMMENT 'Test category',
    criticality_code            STRING                      COMMENT 'CQA criticality (CQA|CCQA|NCQA|KQA|REPORT)',
    uom_code                    STRING                      COMMENT 'Unit of measure code',
    sequence_number             INT                         COMMENT 'Display order',
    reporting_type              STRING                      COMMENT 'Reporting type',
    is_required                 BOOLEAN                     COMMENT 'Mandatory flag',

    -- Pivoted limit columns — Acceptance Criteria (ICH Q6A)
    ac_lower_limit              DECIMAL(18, 6)              COMMENT 'AC lower limit',
    ac_upper_limit              DECIMAL(18, 6)              COMMENT 'AC upper limit',
    ac_target_value             DECIMAL(18, 6)              COMMENT 'AC target value',
    ac_limit_description        STRING                      COMMENT 'AC limit expression (CTD-ready)',

    -- Pivoted limit columns — Normal Operating Range (ICH Q8)
    nor_lower_limit             DECIMAL(18, 6)              COMMENT 'NOR lower limit',
    nor_upper_limit             DECIMAL(18, 6)              COMMENT 'NOR upper limit',
    nor_target_value            DECIMAL(18, 6)              COMMENT 'NOR target value',
    nor_limit_description       STRING                      COMMENT 'NOR limit expression',

    -- Pivoted limit columns — Proven Acceptable Range (ICH Q8 Design Space)
    par_lower_limit             DECIMAL(18, 6)              COMMENT 'PAR lower limit',
    par_upper_limit             DECIMAL(18, 6)              COMMENT 'PAR upper limit',
    par_target_value            DECIMAL(18, 6)              COMMENT 'PAR target value',
    par_limit_description       STRING                      COMMENT 'PAR limit expression',

    -- Pivoted limit columns — Alert (SPC)
    alert_lower_limit           DECIMAL(18, 6)              COMMENT 'Alert lower limit',
    alert_upper_limit           DECIMAL(18, 6)              COMMENT 'Alert upper limit',
    alert_limit_description     STRING                      COMMENT 'Alert limit expression',

    -- Pivoted limit columns — Action (SPC)
    action_lower_limit          DECIMAL(18, 6)              COMMENT 'Action lower limit',
    action_upper_limit          DECIMAL(18, 6)              COMMENT 'Action upper limit',
    action_limit_description    STRING                      COMMENT 'Action limit expression',

    -- Hierarchy validation (ICH Q8: PAR >= AC >= NOR)
    is_hierarchy_valid          BOOLEAN                     COMMENT 'TRUE if PAR >= AC >= NOR holds per ICH Q8',

    -- Metadata
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Current version flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (spec_type_code)
COMMENT 'L2.2 Denormalized: Wide specification with pivoted limit columns per ICH Q8 hierarchy.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'denormalized'
);
