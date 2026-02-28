CREATE OR REPLACE TABLE l2_2_unified_model.dim_specification
(
    spec_key                    BIGINT          NOT NULL    COMMENT 'Surrogate key',
    source_specification_id     STRING          NOT NULL    COMMENT 'Source system natural key',
    spec_number                 STRING          NOT NULL    COMMENT 'Specification document number (e.g., QC-SPEC-2026-0001)',
    spec_version                STRING          NOT NULL    COMMENT 'Specification version (e.g., 1.0, 2.1)',
    spec_title                  STRING                      COMMENT 'Specification title',
    spec_type_code              STRING          NOT NULL    COMMENT 'Type: DS|DP|RM|EXCIP|INTERMED|IPC|CCS',
    spec_type_name              STRING                      COMMENT 'Type display name (Drug Substance, Drug Product, etc.)',
    product_key                 BIGINT                      COMMENT 'FK to dim_product',
    material_key                BIGINT                      COMMENT 'FK to dim_material',
    site_key                    BIGINT                      COMMENT 'FK to dim_site',
    market_key                  BIGINT                      COMMENT 'FK to dim_market',
    status_code                 STRING          NOT NULL    COMMENT 'Status: DRA|APP|SUP|OBS|ARCH',
    status_name                 STRING                      COMMENT 'Status display name (Draft, Approved, Superseded, etc.)',
    stage_code                  STRING                      COMMENT 'Stage: DEV|CLI|COM (Development, Clinical, Commercial)',
    stage_name                  STRING                      COMMENT 'Stage display name',
    ctd_section                 STRING                      COMMENT 'CTD section reference (3.2.S.4.1, 3.2.P.5.1)',
    compendia_reference         STRING                      COMMENT 'Compendia: USP|EP|JP|BP',
    effective_start_date        DATE                        COMMENT 'Specification effective start date',
    effective_end_date          DATE                        COMMENT 'Specification effective end date (NULL = open-ended)',
    effective_start_date_key    INT                         COMMENT 'FK to dim_date (start)',
    effective_end_date_key      INT                         COMMENT 'FK to dim_date (end)',
    approval_date               DATE                        COMMENT 'Formal approval date',
    approval_date_key           INT                         COMMENT 'FK to dim_date (approval)',
    approver_name               STRING                      COMMENT 'Approving authority name',
    approved_by                 STRING                      COMMENT 'Approver user ID',
    supersedes_spec_key         BIGINT                      COMMENT 'Self-FK to superseded specification',
    supersedes_spec_id          STRING                      COMMENT 'Superseded specification source ID',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'SCD2 current flag',
    valid_from                  TIMESTAMP       NOT NULL    COMMENT 'SCD2 valid from',
    valid_to                    TIMESTAMP                   COMMENT 'SCD2 valid to (NULL = current)',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
PARTITIONED BY (spec_type_code)
COMMENT 'L2.2 Conformed: Specification header dimension per ICH Q6A/Q6B (SCD2).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'conformed_dimension'
);
