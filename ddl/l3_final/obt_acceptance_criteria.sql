-- =============================================================================
-- Table  : obt_acceptance_criteria
-- Schema : l3_spec_products
-- Layer  : L3 — Final Data Product Layer
-- Domain : Pharmaceutical Quality — Specifications
-- Grain  : One row per test per specification (release acceptance criteria only;
--          stability acceptance criteria in separate stability_time_point rows)
-- Purpose: Focused OBT for Acceptance Criteria analysis and reporting:
--            - Specification comparison (across product versions, regions, stages)
--            - Limit trending (how AC has changed over spec lifecycle)
--            - Compendia compliance gap analysis
--            - CTD 3.2.P.5.6 / 3.2.S.4.4 specification justification support
--            - Regulatory intelligence: filed vs intended limits comparison
--          This table is simpler than obt_specification_ctd — optimized for
--          fast AC queries without the full spec context overhead.
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l3_spec_products.obt_acceptance_criteria
(
    -- =========================================================================
    -- IDENTIFICATION
    -- =========================================================================
    spec_number                 STRING          NOT NULL    COMMENT 'Specification document number',
    spec_version                STRING          NOT NULL    COMMENT 'Specification version',
    spec_type_code              STRING          NOT NULL    COMMENT 'DS|DP|RM|EXCIP|INTERMED|IPC|CCS',
    spec_type_name              STRING                      COMMENT 'Type display name',
    stage_code                  STRING          NOT NULL    COMMENT 'DEV|CLI|COM',
    ctd_section                 STRING                      COMMENT 'CTD section reference',
    effective_date              DATE                        COMMENT 'Specification effective date',

    -- =========================================================================
    -- PRODUCT / MATERIAL
    -- =========================================================================
    product_name                STRING                      COMMENT 'Drug product name',
    inn_name                    STRING                      COMMENT 'INN / USAN name',
    material_name               STRING                      COMMENT 'Drug substance / material name',
    dosage_form_name            STRING                      COMMENT 'Dosage form',
    strength                    STRING                      COMMENT 'Product strength',
    nda_number                  STRING                      COMMENT 'NDA / ANDA / BLA / MAA number',

    -- =========================================================================
    -- REGULATORY CONTEXT
    -- =========================================================================
    region_code                 STRING                      COMMENT 'US|EU|JP|GLOBAL',
    regulatory_body             STRING                      COMMENT 'FDA|EMA|PMDA|etc.',

    -- =========================================================================
    -- TEST / PARAMETER
    -- =========================================================================
    sequence_number             INT                         COMMENT 'Test display order',
    test_name                   STRING          NOT NULL    COMMENT 'Test name',
    test_category_code          STRING                      COMMENT 'PHY|CHE|IMP|MIC|BIO|STER',
    test_category_name          STRING                      COMMENT 'Category display name',
    test_subcategory            STRING                      COMMENT 'Subcategory (e.g., Related Substances)',
    test_method_number          STRING                      COMMENT 'Analytical method number',
    compendia_test_ref          STRING                      COMMENT 'Compendia reference (USP <711>, EP 2.9.3)',
    result_uom_code             STRING                      COMMENT 'Result unit code',

    -- =========================================================================
    -- ACCEPTANCE CRITERIA
    -- =========================================================================
    ac_lower_limit              DECIMAL(18, 6)              COMMENT 'Lower bound',
    ac_upper_limit              DECIMAL(18, 6)              COMMENT 'Upper bound',
    ac_target                   DECIMAL(18, 6)              COMMENT 'Nominal / target value',
    ac_lower_operator           STRING                      COMMENT 'NLT|GT|GTE|NONE',
    ac_upper_operator           STRING                      COMMENT 'NMT|LT|LTE|NONE',
    ac_limit_text               STRING                      COMMENT 'Non-numeric limit expression',
    ac_limit_description        STRING                      COMMENT 'Full CTD-formatted limit expression',
    ac_limit_basis              STRING                      COMMENT 'AS_IS|ANHYDROUS|AS_LABELED|DRIED_BASIS',
    ac_uom_code                 STRING                      COMMENT 'Limit unit code (may differ from result unit)',
    ac_stage                    STRING                      COMMENT 'RELEASE|STABILITY|BOTH',
    stability_time_point        STRING                      COMMENT 'T0|T3M|T6M|T12M|T24M|T36M (NULL for release)',
    stability_condition         STRING                      COMMENT '25C60RH|40C75RH|REFRIG|etc. (NULL for release)',
    ac_regulatory_basis         STRING                      COMMENT 'ICH Q6A|USP <xxx>|EP x.x.x',
    is_compendial               BOOLEAN                     COMMENT 'TRUE = sourced from official pharmacopoeia',

    -- =========================================================================
    -- CONTEXT: NOR AND PAR (for comparison and justification)
    -- =========================================================================
    nor_lower_limit             DECIMAL(18, 6)              COMMENT 'NOR lower bound (internal reference)',
    nor_upper_limit             DECIMAL(18, 6)              COMMENT 'NOR upper bound (internal reference)',
    par_lower_limit             DECIMAL(18, 6)              COMMENT 'PAR lower bound (design space reference)',
    par_upper_limit             DECIMAL(18, 6)              COMMENT 'PAR upper bound (design space reference)',

    -- =========================================================================
    -- LIMIT WIDTH METRICS (computed — useful for comparison / analytics)
    -- =========================================================================
    ac_width                    DECIMAL(18, 6)              COMMENT 'AC range width: ac_upper_limit - ac_lower_limit',
    nor_width                   DECIMAL(18, 6)              COMMENT 'NOR range width: nor_upper_limit - nor_lower_limit',
    par_width                   DECIMAL(18, 6)              COMMENT 'PAR range width: par_upper_limit - par_lower_limit',
    nor_tightness_pct           DECIMAL(8, 4)               COMMENT 'NOR width / AC width * 100 (lower = tighter NOR)',
    par_vs_ac_factor            DECIMAL(8, 4)               COMMENT 'PAR width / AC width (how much wider PAR is vs AC)',

    -- =========================================================================
    -- DATA QUALITY
    -- =========================================================================
    is_hierarchy_valid          BOOLEAN                     COMMENT 'PAR >= AC >= NOR hierarchy flag',

    -- =========================================================================
    -- AUDIT
    -- =========================================================================
    source_system_code          STRING                      COMMENT 'Originating source system',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'ETL load timestamp (UTC)'
)
USING DELTA
PARTITIONED BY (spec_type_code, stage_code)
COMMENT 'L3 Acceptance Criteria OBT. One row per test per specification per stability time point. Focused on regulatory Acceptance Criteria with NOR/PAR context and computed width metrics. Supports CTD 3.2.P.5.6 specification justification and cross-version limit comparison.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L3',
    'quality.table_type'                = 'obt',
    'quality.grain'                     = 'acceptance_criteria_per_spec_item_per_timepoint',
    'quality.refresh_strategy'          = 'full_overwrite',
    'quality.primary_consumer'          = 'spec_review,CTD_3.2.P.5.6,limit_comparison'
);

-- OPTIMIZE l3_spec_products.obt_acceptance_criteria ZORDER BY (spec_number, test_name, stability_time_point);
