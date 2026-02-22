-- =============================================================================
-- Table  : obt_specification_ctd
-- Schema : l3_spec_products
-- Layer  : L3 — Final Data Product Layer
-- Domain : Pharmaceutical Quality — Specifications
-- Grain  : One row per specification item per stability time point
--          (release specs = one row per test;
--           stability specs = one row per test per time point per condition)
-- Changes: v2 — added site (site_code, site_name, site_regulatory_region) and
--               market (market_country_code, market_country_name, market_status)
--               from dim_site / dim_market; added analyte_code and criticality
--               from dim_specification_item; renamed effective_date →
--               effective_start_date (aligned with dim_specification v2).
-- Purpose: PRIMARY FINAL DATA PRODUCT for regulatory submission (CTD filing).
--          One Big Table (OBT) fully denormalized, flattened, and CTD-aligned.
--          Integrates ALL specification, item, limit, product, material, method,
--          and regulatory context data into a single queryable surface.
--
--          Consumer use cases:
--            1. CTD Module 3 specification table generation (3.2.S.4.1, 3.2.P.5.1)
--            2. Regulatory submission portals (Veeva Vault, Documentum, EURS)
--            3. BI dashboards (Power BI, Tableau, Databricks SQL)
--            4. Spec comparison across product versions and regions
--            5. Gap analysis vs compendia and ICH guidelines
--            6. API exposure to downstream quality systems
--
--          Scope filter (applied during population):
--            - status_code = 'APP' (Approved specifications only)
--            - is_current = TRUE
--            - is_in_filing = TRUE (regulatory limits only)
--            - limit_type_code = 'AC' (Acceptance Criteria for CTD)
--            - NOR and PAR included as additional context columns
--
--          Refresh strategy: Full overwrite on each ETL cycle
--          Partition: spec_type_code, stage_code
--          Source: l2_2_spec_unified.dspec_specification
-- CTD    : Directly maps to 3.2.S.4.1 (DS) and 3.2.P.5.1 (DP) tables
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l3_spec_products.obt_specification_ctd
(
    -- =========================================================================
    -- BLOCK 1: SPECIFICATION HEADER
    -- =========================================================================
    spec_key                    BIGINT                      COMMENT 'Surrogate key from L2.2 dim_specification',
    spec_number                 STRING          NOT NULL    COMMENT 'Specification document number (e.g., SP-DS-2024-001)',
    spec_version                STRING          NOT NULL    COMMENT 'Specification version',
    spec_title                  STRING                      COMMENT 'Full specification document title',
    spec_type_code              STRING          NOT NULL    COMMENT 'DS|DP|RM|EXCIP|INTERMED|IPC|CCS',
    spec_type_name              STRING                      COMMENT 'Type display name',
    ctd_section                 STRING                      COMMENT 'CTD section (3.2.S.4.1 / 3.2.P.5.1)',
    stage_code                  STRING          NOT NULL    COMMENT 'DEV|CLI|COM',
    stage_name                  STRING                      COMMENT 'Development|Clinical|Commercial',
    status_code                 STRING          NOT NULL    COMMENT 'APP|DRA|SUP|OBS (only APP in this OBT)',
    effective_start_date        DATE                        COMMENT 'Specification effective date (ref ERD: effective_start_date)',
    effective_end_date          DATE                        COMMENT 'Specification expiry date — NULL = open-ended',
    approval_date               DATE                        COMMENT 'Specification approval date',
    approver_name               STRING                      COMMENT 'Approving authority name',

    -- Site of manufacture / testing (from dim_site)
    site_code                   STRING                      COMMENT 'Manufacturing / testing site code (from dim_site)',
    site_name                   STRING                      COMMENT 'Manufacturing / testing site name (from dim_site)',
    site_regulatory_region      STRING                      COMMENT 'GxP regulatory region: FDA|EMA|PMDA|ANVISA (from dim_site)',

    -- Target market / country (from dim_market)
    market_country_code         STRING                      COMMENT 'ISO 3166-1 country code of target market (from dim_market)',
    market_country_name         STRING                      COMMENT 'Country name of target market (from dim_market)',
    market_status               STRING                      COMMENT 'Marketing authorisation status: APPROVED|PENDING|FILED|WITHDRAWN',

    compendia_reference         STRING                      COMMENT 'Pharmacopoeia basis: USP|EP|JP|BP',

    -- =========================================================================
    -- BLOCK 2: PRODUCT INFORMATION
    -- =========================================================================
    product_name                STRING                      COMMENT 'Drug product name',
    inn_name                    STRING                      COMMENT 'International Nonproprietary Name (INN/USAN)',
    brand_name                  STRING                      COMMENT 'Commercial / trade name',
    dosage_form_code            STRING                      COMMENT 'TAB|CAP|INJ|SOL|SUS|CRM|etc.',
    dosage_form_name            STRING                      COMMENT 'Dosage form display name',
    route_of_administration     STRING                      COMMENT 'ORAL|IV|IM|SC|TOPICAL|INHAL|etc.',
    strength                    STRING                      COMMENT 'Product strength (e.g., 10 mg, 250 mg/5 mL)',
    nda_number                  STRING                      COMMENT 'NDA/ANDA/BLA/MAA application number',

    -- =========================================================================
    -- BLOCK 3: DRUG SUBSTANCE / MATERIAL INFORMATION
    -- =========================================================================
    material_name               STRING                      COMMENT 'Material name (API, excipient, intermediate)',
    material_type_code          STRING                      COMMENT 'API|EXCIP|INTERMED|DRUG_PROD|RM|PACK',
    cas_number                  STRING                      COMMENT 'CAS Registry Number',
    molecular_formula           STRING                      COMMENT 'Molecular formula',
    molecular_weight            DECIMAL(10, 4)              COMMENT 'Molecular weight (g/mol)',
    pharmacopoeia_grade         STRING                      COMMENT 'USP|EP|JP|BP|NF',

    -- =========================================================================
    -- BLOCK 4: REGULATORY CONTEXT
    -- =========================================================================
    regulatory_context_code     STRING                      COMMENT 'US-NDA|EU-MAA|JP-JNDA|GLOBAL-CTD',
    region_code                 STRING                      COMMENT 'US|EU|JP|CA|GLOBAL',
    region_name                 STRING                      COMMENT 'Region display name',
    regulatory_body             STRING                      COMMENT 'FDA|EMA|PMDA|Health Canada|etc.',
    submission_type             STRING                      COMMENT 'NDA|ANDA|BLA|MAA|JNDA|CTD',
    guideline_code              STRING                      COMMENT 'ICH Q6A|ICH Q6B|USP|EP',

    -- =========================================================================
    -- BLOCK 5: TEST / SPECIFICATION ITEM
    -- =========================================================================
    spec_item_key               BIGINT                      COMMENT 'Surrogate key from L2.2 dim_specification_item',
    sequence_number             INT                         COMMENT 'Test display order in specification',
    test_code                   STRING                      COMMENT 'Internal test code',
    test_name                   STRING          NOT NULL    COMMENT 'Test name as it appears in specification',
    test_description            STRING                      COMMENT 'Detailed test description',
    test_category_code          STRING                      COMMENT 'PHY|CHE|IMP|MIC|BIO|STER|PACK',
    test_category_name          STRING                      COMMENT 'Test category display name',
    test_subcategory            STRING                      COMMENT 'e.g., Related Substances, Residual Solvents',
    analyte_code                STRING                      COMMENT 'Specific analyte / substance identifier in multi-analyte tests',
    criticality                 STRING                      COMMENT 'ICH Q8 quality attribute criticality: CQA|CCQA|NCQA|KQA|REPORT',
    is_required                 BOOLEAN                     COMMENT 'TRUE = mandatory test',
    is_compendial               BOOLEAN                     COMMENT 'TRUE = test from official pharmacopoeia',
    reporting_type              STRING                      COMMENT 'NUMERIC|PASS_FAIL|TEXT|REPORT_ONLY',
    result_precision            INT                         COMMENT 'Decimal places for numeric result',
    result_uom_code             STRING                      COMMENT 'Primary result unit code',
    result_uom_name             STRING                      COMMENT 'Primary result unit name',
    stage_applicability         STRING                      COMMENT 'RELEASE|STABILITY|IPC|BOTH',

    -- =========================================================================
    -- BLOCK 6: ANALYTICAL METHOD
    -- =========================================================================
    test_method_number          STRING                      COMMENT 'Analytical method document number',
    test_method_name            STRING                      COMMENT 'Analytical method name',
    test_method_version         STRING                      COMMENT 'Analytical method version',
    method_type_code            STRING                      COMMENT 'COMP|COMP_MOD|INHOUSE|TRANSFER',
    analytical_technique        STRING                      COMMENT 'HPLC|GC|KF|UV-VIS|LAL|BIOASSAY|etc.',
    compendia_test_ref          STRING                      COMMENT 'Compendia test reference (e.g., USP <711>)',
    validation_status           STRING                      COMMENT 'VALIDATED|QUALIFIED|VERIFIED|WAIVED',

    -- =========================================================================
    -- BLOCK 7: ACCEPTANCE CRITERIA (Regulatory Limits — CTD Filing)
    -- These are the limits that appear in the CTD specification tables.
    -- =========================================================================
    ac_lower_limit              DECIMAL(18, 6)              COMMENT 'Acceptance criteria lower bound (numeric)',
    ac_upper_limit              DECIMAL(18, 6)              COMMENT 'Acceptance criteria upper bound (numeric)',
    ac_target                   DECIMAL(18, 6)              COMMENT 'Acceptance criteria nominal / target value',
    ac_lower_operator           STRING                      COMMENT 'Lower bound operator: NLT|GT|GTE|NONE',
    ac_upper_operator           STRING                      COMMENT 'Upper bound operator: NMT|LT|LTE|NONE',
    ac_limit_text               STRING                      COMMENT 'Non-numeric limit (e.g., "Passes test", "Clear, colorless")',
    ac_limit_description        STRING                      COMMENT 'Full formatted limit expression for CTD table (human-readable)',
    ac_limit_basis              STRING                      COMMENT 'AS_IS|ANHYDROUS|AS_LABELED|DRIED_BASIS',
    ac_uom_code                 STRING                      COMMENT 'Acceptance criteria unit code',
    ac_regulatory_basis         STRING                      COMMENT 'ICH Q6A|USP <xxx>|EP x.x.x|etc.',

    -- =========================================================================
    -- BLOCK 8: STABILITY-SPECIFIC ATTRIBUTES
    -- Populated only when stage_applicability includes STABILITY
    -- =========================================================================
    stability_time_point        STRING                      COMMENT 'Stability test time point: T0|T3M|T6M|T12M|T18M|T24M|T36M',
    stability_condition         STRING                      COMMENT 'ICH storage condition: 25C60RH|30C65RH|40C75RH|REFRIG|FREEZE',
    stability_condition_label   STRING                      COMMENT 'Human-readable condition: 25°C/60%RH|30°C/65%RH|40°C/75%RH|2-8°C|-20°C',
    is_release_spec             BOOLEAN                     COMMENT 'TRUE = this row represents a release specification limit',
    is_stability_spec           BOOLEAN                     COMMENT 'TRUE = this row represents a stability specification limit',

    -- =========================================================================
    -- BLOCK 9: INTERNAL LIMITS (Context — not in CTD filing)
    -- Included for completeness; filter out for pure CTD output
    -- =========================================================================
    nor_lower_limit             DECIMAL(18, 6)              COMMENT 'Normal Operating Range lower bound (internal)',
    nor_upper_limit             DECIMAL(18, 6)              COMMENT 'Normal Operating Range upper bound (internal)',
    nor_limit_description       STRING                      COMMENT 'NOR formatted expression',
    par_lower_limit             DECIMAL(18, 6)              COMMENT 'Proven Acceptable Range lower bound',
    par_upper_limit             DECIMAL(18, 6)              COMMENT 'Proven Acceptable Range upper bound',
    par_limit_description       STRING                      COMMENT 'PAR formatted expression',

    -- =========================================================================
    -- BLOCK 10: DATA QUALITY & LINEAGE
    -- =========================================================================
    is_hierarchy_valid          BOOLEAN                     COMMENT 'TRUE = PAR >= AC >= NOR hierarchy validated',
    source_spec_key             BIGINT                      COMMENT 'Traceability: source spec_key from L2.2',
    source_spec_item_key        BIGINT                      COMMENT 'Traceability: source spec_item_key from L2.2',
    source_system_code          STRING                      COMMENT 'Originating source system: LIMS|SAP|VAULT|MANUAL',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'ETL load timestamp (UTC)',
    data_product_version        STRING                      COMMENT 'Version of this data product (semantic version, e.g., 1.0.0)'
)
USING DELTA
PARTITIONED BY (spec_type_code, stage_code)
COMMENT 'L3 Final data product. One Big Table (OBT) for CTD regulatory filing. Fully denormalized, approved specifications only, Acceptance Criteria (is_in_filing=TRUE) with NOR/PAR context. Maps directly to CTD 3.2.S.4.1 (DS) and 3.2.P.5.1 (DP). One row per spec item per stability time point.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L3',
    'quality.table_type'                = 'obt',
    'quality.grain'                     = 'spec_item_per_stability_time_point',
    'quality.refresh_strategy'          = 'full_overwrite',
    'quality.ctd_sections'              = '3.2.S.4.1,3.2.P.5.1',
    'quality.scope_filter'             = 'status_code=APP AND is_current=TRUE AND ac_is_in_filing=TRUE',
    'quality.primary_consumer'          = 'CTD_regulatory_filing'
);

-- OPTIMIZE l3_spec_products.obt_specification_ctd ZORDER BY (spec_number, test_name);

-- =============================================================================
-- POPULATION QUERY — from L2.2 dspec_specification
-- =============================================================================
-- v2: added site, market, analyte_code, criticality, effective_start/end_date.
--
-- INSERT OVERWRITE l3_spec_products.obt_specification_ctd
-- SELECT
--     spec_key,
--     spec_number,
--     spec_version,
--     spec_title,
--     spec_type_code,
--     spec_type_name,
--     ctd_section,
--     stage_code,
--     stage_name,
--     status_code,
--     effective_start_date,
--     effective_end_date,
--     approval_date,
--     approver_name,
--     site_code,
--     site_name,
--     site_regulatory_region,
--     market_country_code,
--     market_country_name,
--     market_status,
--     compendia_reference,
--     product_name,
--     inn_name,
--     brand_name,
--     dosage_form_code,
--     dosage_form_name,
--     route_of_administration,
--     strength,
--     nda_number,
--     material_name,
--     material_type_code,
--     cas_number,
--     NULL AS molecular_formula,   -- add from dim_material if needed
--     molecular_weight,
--     NULL AS pharmacopoeia_grade,
--     regulatory_context_code,
--     region_code,
--     NULL AS region_name,
--     regulatory_body,
--     submission_type,
--     NULL AS guideline_code,
--     spec_item_key,
--     sequence_number,
--     test_code,
--     test_name,
--     test_description,
--     test_category_code,
--     test_category_name,
--     test_subcategory,
--     analyte_code,
--     criticality,
--     is_required,
--     is_compendial,
--     reporting_type,
--     result_precision,
--     uom_code              AS result_uom_code,
--     uom_name              AS result_uom_name,
--     stage_applicability,
--     test_method_number,
--     test_method_name,
--     test_method_version,
--     NULL AS method_type_code,
--     analytical_technique,
--     compendia_test_ref,
--     NULL AS validation_status,
--     ac_lower_limit,
--     ac_upper_limit,
--     ac_target,
--     ac_lower_operator,
--     ac_upper_operator,
--     ac_limit_text,
--     ac_limit_description,
--     ac_limit_basis,
--     ac_uom_code,
--     ac_regulatory_basis,
--     ac_stability_time_point   AS stability_time_point,
--     ac_stability_condition    AS stability_condition,
--     CASE ac_stability_condition
--         WHEN '25C60RH'    THEN '25°C/60%RH (Long-term)'
--         WHEN '30C65RH'    THEN '30°C/65%RH (Intermediate)'
--         WHEN '40C75RH'    THEN '40°C/75%RH (Accelerated)'
--         WHEN 'REFRIG'     THEN '2-8°C (Refrigerated)'
--         WHEN 'FREEZE'     THEN '-20°C (Frozen)'
--         WHEN 'DEEPFREEZE' THEN '-80°C (Deep Freeze)'
--         WHEN 'PHOTO'      THEN 'Photostability'
--         ELSE ac_stability_condition
--     END                        AS stability_condition_label,
--     (ac_stage = 'RELEASE' OR ac_stage = 'BOTH')   AS is_release_spec,
--     (ac_stage = 'STABILITY' OR ac_stage = 'BOTH') AS is_stability_spec,
--     nor_lower_limit,
--     nor_upper_limit,
--     nor_limit_description,
--     par_lower_limit,
--     par_upper_limit,
--     par_limit_description,
--     is_hierarchy_valid,
--     spec_key                   AS source_spec_key,
--     spec_item_key              AS source_spec_item_key,
--     'LIMS'                     AS source_system_code,    -- replace with actual
--     current_timestamp()        AS load_timestamp,
--     '2.0.0'                    AS data_product_version
-- FROM l2_2_spec_unified.dspec_specification
-- WHERE status_code = 'APP'
--   AND is_current = TRUE
--   AND ac_is_in_filing = TRUE;
