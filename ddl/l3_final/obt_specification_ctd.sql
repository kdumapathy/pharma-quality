CREATE OR REPLACE TABLE l3_data_product.obt_specification_ctd
(
    obt_ctd_key                 BIGINT          NOT NULL    COMMENT 'Surrogate key',

    -- Specification header
    spec_key                    BIGINT          NOT NULL    COMMENT 'FK to L2.2 dim_specification',
    spec_number                 STRING          NOT NULL    COMMENT 'Specification number',
    spec_version                STRING          NOT NULL    COMMENT 'Specification version',
    spec_title                  STRING                      COMMENT 'Specification title',
    spec_type_code              STRING          NOT NULL    COMMENT 'Type: DS|DP|RM|EXCIP|INTERMED|IPC|CCS',
    spec_type_name              STRING                      COMMENT 'Type display name',
    status_code                 STRING          NOT NULL    COMMENT 'Status: DRA|APP|SUP|OBS|ARCH',
    stage_code                  STRING                      COMMENT 'Stage: DEV|CLI|COM',
    stage_name                  STRING                      COMMENT 'Stage display name',

    -- Product (PQ/CMC)
    product_name                STRING                      COMMENT 'Product name',
    inn_name                    STRING                      COMMENT 'International Nonproprietary Name (WHO INN)',
    brand_name                  STRING                      COMMENT 'Brand / trade name',
    dosage_form_code            STRING                      COMMENT 'Dosage form code',
    dosage_form_name            STRING                      COMMENT 'Dosage form display name',
    route_of_administration     STRING                      COMMENT 'Route of administration',
    strength                    STRING                      COMMENT 'Strength string',
    nda_number                  STRING                      COMMENT 'NDA/ANDA/BLA/MAA registration number',

    -- Material (CMC)
    material_name               STRING                      COMMENT 'Material name',
    material_type_code          STRING                      COMMENT 'Material type code',
    cas_number                  STRING                      COMMENT 'CAS Registry Number',

    -- Site (GMP)
    site_code                   STRING                      COMMENT 'Site code',
    site_name                   STRING                      COMMENT 'Site name',
    site_regulatory_region      STRING                      COMMENT 'Regulatory region',

    -- Market
    region_code                 STRING                      COMMENT 'Market region code',
    market_country_code         STRING                      COMMENT 'Market country code',
    market_country_name         STRING                      COMMENT 'Market country name',
    market_status               STRING                      COMMENT 'Marketing authorization status',

    -- Test / Item
    test_name                   STRING          NOT NULL    COMMENT 'Test name',
    test_code                   STRING                      COMMENT 'Test code',
    test_category_code          STRING                      COMMENT 'Category: PHY|CHE|IMP|MIC|BIO|STER|PACK',
    test_category_name          STRING                      COMMENT 'Category display name',
    criticality                 STRING                      COMMENT 'CQA classification: CQA|CCQA|NCQA|KQA|REPORT',
    sequence_number             INT                         COMMENT 'Display order',
    is_required                 BOOLEAN                     COMMENT 'Mandatory flag',
    reporting_type              STRING                      COMMENT 'NUMERIC|PASS_FAIL|TEXT|REPORT_ONLY',
    compendia_reference         STRING                      COMMENT 'Compendia reference for specification',
    compendia_test_ref          STRING                      COMMENT 'Compendia test reference (USP <621>, EP 2.9.3)',

    -- Method
    test_method_name            STRING                      COMMENT 'Test method name',
    test_method_number          STRING                      COMMENT 'Method document number',

    -- Limit
    limit_type_code             STRING                      COMMENT 'Limit type code',
    limit_type_name             STRING                      COMMENT 'Limit type name',
    lower_limit_value           DECIMAL(18, 6)              COMMENT 'Lower limit',
    upper_limit_value           DECIMAL(18, 6)              COMMENT 'Upper limit',
    target_value                DECIMAL(18, 6)              COMMENT 'Target value',
    limit_description           STRING                      COMMENT 'Formatted limit expression',
    limit_text                  STRING                      COMMENT 'Non-numeric limit text',
    uom_code                    STRING                      COMMENT 'Unit of measure',
    uom_name                    STRING                      COMMENT 'Unit display name',
    limit_basis                 STRING                      COMMENT 'Basis: AS_IS|ANHYDROUS|AS_LABELED|DRIED_BASIS',

    -- Stability context
    stability_time_point        STRING                      COMMENT 'Time point: T0|T3M|T6M|T12M|T24M|T36M',
    stability_condition         STRING                      COMMENT 'Condition: 25C60RH|40C75RH|REFRIG',

    -- Regulatory
    ctd_section                 STRING                      COMMENT 'CTD section reference',
    is_in_filing                BOOLEAN                     COMMENT 'Appears in regulatory filing',
    regulatory_basis            STRING                      COMMENT 'Regulatory basis',

    -- Dates
    effective_start_date        DATE                        COMMENT 'Limit effective start date',
    effective_end_date          DATE                        COMMENT 'Limit effective end date',
    approval_date               DATE                        COMMENT 'Specification approval date',
    approver_name               STRING                      COMMENT 'Approving authority name',

    -- Metadata
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Current version flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (spec_type_code)
COMMENT 'L3 OBT: CTD-ready specification data product. Grain = spec x item x limit type x stage.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L3',
    'quality.table_type'                = 'obt'
);
