CREATE OR REPLACE TABLE l2_2_unified_model.dim_specification_item
(
    spec_item_key               BIGINT          NOT NULL    COMMENT 'Surrogate key',
    source_spec_item_id         STRING          NOT NULL    COMMENT 'Source system natural key',
    spec_key                    BIGINT          NOT NULL    COMMENT 'FK to dim_specification',
    test_method_key             BIGINT                      COMMENT 'FK to dim_test_method',
    uom_key                     INT                         COMMENT 'FK to dim_uom',
    test_code                   STRING                      COMMENT 'Test code (e.g., ASSAY, DISSO, IMPD)',
    test_name                   STRING          NOT NULL    COMMENT 'Test / parameter name (Assay, Dissolution, etc.)',
    analyte_code                STRING                      COMMENT 'Specific analyte code (APAP, IMP-A, TOTAL-IMP)',
    test_category_code          STRING                      COMMENT 'Category: PHY|CHE|IMP|MIC|BIO|STER|PACK',
    test_category_name          STRING                      COMMENT 'Category display name',
    test_subcategory            STRING                      COMMENT 'Subcategory (Related Substances, Residual Solvents, etc.)',
    criticality                 STRING                      COMMENT 'CQA classification per ICH Q8/Q9: CQA|CCQA|NCQA|KQA|REPORT',
    sequence_number             INT                         COMMENT 'Display order in specification',
    is_required                 BOOLEAN                     COMMENT 'TRUE = mandatory test per specification',
    is_compendial               BOOLEAN                     COMMENT 'TRUE = from official pharmacopoeia (USP/EP/JP)',
    is_stability_indicating     BOOLEAN                     COMMENT 'TRUE = stability-indicating method per ICH Q1A',
    reporting_type              STRING                      COMMENT 'Type: NUMERIC|PASS_FAIL|TEXT|REPORT_ONLY',
    result_precision            INT                         COMMENT 'Decimal places for numeric results',
    compendia_test_ref          STRING                      COMMENT 'Compendia test reference (USP <621>, EP 2.9.3)',
    stage_applicability         STRING                      COMMENT 'Stage: RELEASE|STABILITY|IPC|BOTH',
    reporting_threshold         DECIMAL(18, 6)              COMMENT 'ICH Q3A/Q3B reporting threshold (%)',
    identification_threshold    DECIMAL(18, 6)              COMMENT 'ICH Q3A/Q3B identification threshold (%)',
    qualification_threshold     DECIMAL(18, 6)              COMMENT 'ICH Q3A/Q3B qualification threshold (%)',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'SCD2 current flag',
    valid_from                  TIMESTAMP       NOT NULL    COMMENT 'SCD2 valid from',
    valid_to                    TIMESTAMP                   COMMENT 'SCD2 valid to',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (test_category_code)
COMMENT 'L2.2 Conformed: Specification item/test dimension per ICH Q6A with CQA classification (SCD2).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'conformed_dimension'
);
