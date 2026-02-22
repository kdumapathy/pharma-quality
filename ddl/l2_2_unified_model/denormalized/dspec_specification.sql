-- =============================================================================
-- Table  : dspec_specification
-- Schema : l2_2_spec_unified
-- Layer  : L2.2 — Unified Data Model (Business Conform Layer) — Denormalized
-- Domain : Pharmaceutical Quality — Specifications
-- Grain  : One row per specification item with all limit types pivoted as columns
-- Purpose: Semi-denormalized specification table combining:
--            - Specification header (from dim_specification)
--            - Item / test attributes (from dim_specification_item)
--            - All limit types pivoted horizontally:
--                Acceptance Criteria (AC)
--                Normal Operating Range (NOR)
--                Proven Acceptable Range (PAR)
--                Alert Limits (with SPC fields)
--                Action Limits (with SPC fields)
--          This table serves as the primary analytical layer for:
--            - Specification review and approval workflows
--            - Quality dashboard and trending
--            - Intermediate preparation for L3 CTD output
--            - Cross-limit comparison reports
--          Refresh strategy: Full overwrite on each ETL cycle (not incremental)
--          to ensure pivot accuracy.
-- CTD    : Used as source for CTD 3.2.S.4.1 / 3.2.P.5.1 tabular output
-- Changes: v2 — added site (GMP status, regulatory region) and market (country,
--               MA status) from dim_site / dim_market FKs on dim_specification;
--               added analyte_code, criticality from dim_specification_item;
--               added SPC fields (calculation_method, sample_size,
--               last_calculated_date) for Alert and Action limits;
--               renamed effective_date → effective_start_date, added
--               effective_end_date to align with dim_specification v2.
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dspec_specification
(
    -- =========================================================================
    -- SECTION A: SPECIFICATION HEADER (from dim_specification)
    -- =========================================================================
    spec_key                    BIGINT          NOT NULL    COMMENT 'Surrogate key of specification version',
    spec_number                 STRING          NOT NULL    COMMENT 'Specification document number (e.g., SP-DS-2024-001)',
    spec_version                STRING          NOT NULL    COMMENT 'Specification version string',
    spec_title                  STRING                      COMMENT 'Full specification document title',
    spec_type_code              STRING          NOT NULL    COMMENT 'Type: DS|DP|RM|EXCIP|INTERMED|IPC|CCS',
    spec_type_name              STRING                      COMMENT 'Type display name',
    ctd_section                 STRING                      COMMENT 'CTD section (e.g., 3.2.S.4.1, 3.2.P.5.1)',
    stage_code                  STRING                      COMMENT 'Lifecycle stage: DEV|CLI|COM',
    stage_name                  STRING                      COMMENT 'Stage display name',
    status_code                 STRING          NOT NULL    COMMENT 'Status: DRA|APP|SUP|OBS|ARCH',
    status_name                 STRING                      COMMENT 'Status display name',
    effective_start_date        DATE                        COMMENT 'Specification effective date (ref ERD: effective_start_date)',
    effective_end_date          DATE                        COMMENT 'Specification expiry date — NULL = open-ended (ref ERD: effective_end_date)',
    approval_date               DATE                        COMMENT 'Specification approval date',
    approver_name               STRING                      COMMENT 'Approving authority name',

    -- Site (from dim_site via dim_specification.site_key)
    site_code                   STRING                      COMMENT 'Manufacturing / testing site code (from dim_site)',
    site_name                   STRING                      COMMENT 'Manufacturing / testing site name (from dim_site)',
    site_regulatory_region      STRING                      COMMENT 'GxP regulatory region at site: FDA|EMA|PMDA|ANVISA (from dim_site)',
    gmp_status                  STRING                      COMMENT 'Site GMP status: APPROVED|WARNING_LETTER|IMPORT_ALERT (from dim_site)',

    -- Market (from dim_market via dim_specification.market_key)
    market_country_code         STRING                      COMMENT 'ISO 3166-1 country code of target market (from dim_market)',
    market_country_name         STRING                      COMMENT 'Country name of target market (from dim_market)',
    market_status               STRING                      COMMENT 'Marketing authorisation status: APPROVED|PENDING|FILED|WITHDRAWN (from dim_market)',

    compendia_reference         STRING                      COMMENT 'Pharmacopoeia basis: USP|EP|JP|BP',
    supersedes_spec_id          STRING                      COMMENT 'Natural key of prior specification version',

    -- =========================================================================
    -- SECTION B: PRODUCT & MATERIAL (from dim_product, dim_material)
    -- =========================================================================
    product_name                STRING                      COMMENT 'Drug product name (from dim_product)',
    inn_name                    STRING                      COMMENT 'International Nonproprietary Name',
    brand_name                  STRING                      COMMENT 'Commercial / trade name',
    dosage_form_code            STRING                      COMMENT 'Dosage form code: TAB|CAP|INJ|SOL|etc.',
    dosage_form_name            STRING                      COMMENT 'Dosage form display name',
    route_of_administration     STRING                      COMMENT 'Route: ORAL|IV|IM|SC|TOPICAL|etc.',
    strength                    STRING                      COMMENT 'Product strength string (e.g., 10 mg)',
    nda_number                  STRING                      COMMENT 'NDA / ANDA / BLA / MAA application number',
    material_name               STRING                      COMMENT 'Material / substance name (from dim_material)',
    material_type_code          STRING                      COMMENT 'Material type: API|EXCIP|INTERMED|etc.',
    cas_number                  STRING                      COMMENT 'CAS Registry Number (primarily for DS)',
    molecular_weight            DECIMAL(10, 4)              COMMENT 'Molecular weight g/mol (primarily for DS)',

    -- =========================================================================
    -- SECTION C: REGULATORY CONTEXT (from dim_regulatory_context)
    -- =========================================================================
    regulatory_context_code     STRING                      COMMENT 'Regulatory context code (e.g., US-NDA, EU-MAA)',
    region_code                 STRING                      COMMENT 'Region: US|EU|JP|GLOBAL',
    regulatory_body             STRING                      COMMENT 'FDA|EMA|PMDA|etc.',
    submission_type             STRING                      COMMENT 'NDA|ANDA|BLA|MAA|JNDA|CTD',

    -- =========================================================================
    -- SECTION D: SPECIFICATION ITEM / TEST (from dim_specification_item)
    -- =========================================================================
    spec_item_key               BIGINT          NOT NULL    COMMENT 'Surrogate key of test item',
    sequence_number             INT                         COMMENT 'Test display order within specification',
    test_code                   STRING                      COMMENT 'Internal test code (e.g., ASS-001)',
    test_name                   STRING          NOT NULL    COMMENT 'Test name (e.g., Assay, Appearance, Dissolution)',
    test_description            STRING                      COMMENT 'Detailed test description',
    test_category_code          STRING                      COMMENT 'Category: PHY|CHE|IMP|MIC|BIO|STER',
    test_category_name          STRING                      COMMENT 'Category display name',
    test_subcategory            STRING                      COMMENT 'Subcategory (e.g., Related Substances)',
    analyte_code                STRING                      COMMENT 'Specific analyte / substance identifier in multi-analyte tests (ref ERD: analyte_code)',
    criticality                 STRING                      COMMENT 'ICH Q8 quality attribute criticality: CQA|CCQA|NCQA|KQA|REPORT (ref ERD: criticality)',
    test_method_number          STRING                      COMMENT 'Analytical method number',
    test_method_name            STRING                      COMMENT 'Analytical method name',
    test_method_version         STRING                      COMMENT 'Analytical method version',
    analytical_technique        STRING                      COMMENT 'Technique: HPLC|GC|KF|UV-VIS|etc.',
    compendia_test_ref          STRING                      COMMENT 'Compendia reference (e.g., USP <711>)',
    uom_code                    STRING                      COMMENT 'Primary result unit code',
    uom_name                    STRING                      COMMENT 'Primary result unit name',
    reporting_type              STRING                      COMMENT 'NUMERIC|PASS_FAIL|TEXT|REPORT_ONLY',
    result_precision            INT                         COMMENT 'Decimal places for reporting',
    stage_applicability         STRING                      COMMENT 'RELEASE|STABILITY|IPC|BOTH',
    is_required                 BOOLEAN                     COMMENT 'TRUE = mandatory test',
    is_compendial               BOOLEAN                     COMMENT 'TRUE = compendia test',

    -- =========================================================================
    -- SECTION E: ACCEPTANCE CRITERIA (AC) — REGULATORY LIMITS
    -- These limits appear in the regulatory filing (is_in_filing = TRUE)
    -- Sourced from fact_specification_limit WHERE limit_type_code = 'AC'
    -- =========================================================================
    ac_lower_limit              DECIMAL(18, 6)              COMMENT 'AC lower bound numeric value',
    ac_upper_limit              DECIMAL(18, 6)              COMMENT 'AC upper bound numeric value',
    ac_target                   DECIMAL(18, 6)              COMMENT 'AC nominal / target value',
    ac_lower_operator           STRING                      COMMENT 'AC lower operator: NLT|GT|GTE|NONE',
    ac_upper_operator           STRING                      COMMENT 'AC upper operator: NMT|LT|LTE|NONE',
    ac_limit_text               STRING                      COMMENT 'AC text/qualitative limit expression',
    ac_limit_description        STRING                      COMMENT 'AC full formatted expression (CTD-ready)',
    ac_limit_basis              STRING                      COMMENT 'AC basis: AS_IS|ANHYDROUS|AS_LABELED|DRIED_BASIS',
    ac_stage                    STRING                      COMMENT 'AC stage: RELEASE|STABILITY|BOTH',
    ac_stability_time_point     STRING                      COMMENT 'AC stability time point: T0|T6M|T12M|T24M|T36M',
    ac_stability_condition      STRING                      COMMENT 'AC stability condition: 25C60RH|40C75RH|REFRIG',
    ac_regulatory_basis         STRING                      COMMENT 'AC regulatory basis: ICH Q6A|USP <xxx>|EP x.x.x',
    ac_is_in_filing             BOOLEAN                     COMMENT 'TRUE = this AC appears in regulatory filing',
    ac_uom_code                 STRING                      COMMENT 'AC unit code (may differ from result unit)',

    -- =========================================================================
    -- SECTION F: NORMAL OPERATING RANGE (NOR) — INTERNAL OPERATIONAL LIMITS
    -- Internal tighter range; not in regulatory filing
    -- Sourced from fact_specification_limit WHERE limit_type_code = 'NOR'
    -- =========================================================================
    nor_lower_limit             DECIMAL(18, 6)              COMMENT 'NOR lower bound numeric value',
    nor_upper_limit             DECIMAL(18, 6)              COMMENT 'NOR upper bound numeric value',
    nor_target                  DECIMAL(18, 6)              COMMENT 'NOR nominal / target value',
    nor_lower_operator          STRING                      COMMENT 'NOR lower operator: NLT|GT|GTE|NONE',
    nor_upper_operator          STRING                      COMMENT 'NOR upper operator: NMT|LT|LTE|NONE',
    nor_limit_description       STRING                      COMMENT 'NOR full formatted expression',
    nor_uom_code                STRING                      COMMENT 'NOR unit code',

    -- =========================================================================
    -- SECTION G: PROVEN ACCEPTABLE RANGE (PAR) — DESIGN SPACE
    -- Broader validated range; may appear in CTD design space (ICH Q8)
    -- Sourced from fact_specification_limit WHERE limit_type_code = 'PAR'
    -- =========================================================================
    par_lower_limit             DECIMAL(18, 6)              COMMENT 'PAR lower bound numeric value',
    par_upper_limit             DECIMAL(18, 6)              COMMENT 'PAR upper bound numeric value',
    par_target                  DECIMAL(18, 6)              COMMENT 'PAR nominal / target value',
    par_lower_operator          STRING                      COMMENT 'PAR lower operator: NLT|GT|GTE|NONE',
    par_upper_operator          STRING                      COMMENT 'PAR upper operator: NMT|LT|LTE|NONE',
    par_limit_description       STRING                      COMMENT 'PAR full formatted expression',
    par_uom_code                STRING                      COMMENT 'PAR unit code',

    -- =========================================================================
    -- SECTION H: ALERT LIMITS (Internal SPC — not in regulatory filing)
    -- Sourced from fact_specification_limit WHERE limit_type_code = 'ALERT'
    -- SPC fields populated from CONTROL_LIMITS (ref ERD)
    -- =========================================================================
    alert_lower_limit           DECIMAL(18, 6)              COMMENT 'Alert limit lower bound',
    alert_upper_limit           DECIMAL(18, 6)              COMMENT 'Alert limit upper bound',
    alert_limit_description     STRING                      COMMENT 'Alert limit full formatted expression',
    alert_calculation_method    STRING                      COMMENT 'SPC method used to derive alert limits: 3_SIGMA|CPK|EWMA|CUSUM|MANUAL',
    alert_sample_size           INT                         COMMENT 'Number of historical data points used for alert limit calculation',
    alert_last_calculated_date  DATE                        COMMENT 'Date alert limits were last recalculated from historical data',

    -- =========================================================================
    -- SECTION I: ACTION LIMITS (Internal SPC — not in regulatory filing)
    -- Sourced from fact_specification_limit WHERE limit_type_code = 'ACTION'
    -- SPC fields populated from CONTROL_LIMITS (ref ERD)
    -- =========================================================================
    action_lower_limit          DECIMAL(18, 6)              COMMENT 'Action limit lower bound',
    action_upper_limit          DECIMAL(18, 6)              COMMENT 'Action limit upper bound',
    action_limit_description    STRING                      COMMENT 'Action limit full formatted expression',
    action_calculation_method   STRING                      COMMENT 'SPC method used to derive action limits: 3_SIGMA|CPK|EWMA|CUSUM|MANUAL',
    action_sample_size          INT                         COMMENT 'Number of historical data points used for action limit calculation',
    action_last_calculated_date DATE                        COMMENT 'Date action limits were last recalculated from historical data',

    -- =========================================================================
    -- SECTION J: LIMIT HIERARCHY VALIDATION FLAGS (computed)
    -- Business rule: PAR >= AC >= NOR (lower bound direction reversed: PAR_lower <= AC_lower <= NOR_lower)
    -- =========================================================================
    is_hierarchy_valid          BOOLEAN                     COMMENT 'TRUE if PAR >= AC >= NOR hierarchy is satisfied for both bounds',
    hierarchy_violation_notes   STRING                      COMMENT 'Description of any hierarchy violation detected',

    -- =========================================================================
    -- SECTION K: METADATA
    -- =========================================================================
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'ETL refresh / load timestamp (UTC)',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'TRUE = current active row (filters to current spec version and limits)'
)
USING DELTA
PARTITIONED BY (spec_type_code, stage_code)
COMMENT 'L2.2 Semi-denormalized specification table. One row per specification item with all limit types (AC, NOR, PAR, Alert, Action) pivoted as columns. Combines header, item, method, product, material, and limit data. Optimized for specification review, CTD preparation, and quality dashboards.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'denormalized',
    'quality.grain'                     = 'spec_item_with_all_limits_pivoted',
    'quality.refresh_strategy'          = 'full_overwrite',
    'quality.ctd_sections'             = '3.2.S.4.1,3.2.P.5.1'
);

-- OPTIMIZE l2_2_spec_unified.dspec_specification ZORDER BY (spec_number, test_code);

-- =============================================================================
-- POPULATION QUERY — Pivot from Normalized Fact to Denormalized Table
-- =============================================================================
-- This query populates dspec_specification from the star schema.
-- Run as OVERWRITE to ensure consistency.
-- v2: added dim_site, dim_market joins; analyte_code, criticality; SPC fields.
--
-- INSERT OVERWRITE l2_2_spec_unified.dspec_specification
-- SELECT
--     -- Section A: Spec Header
--     s.spec_key,
--     s.spec_number,
--     s.spec_version,
--     s.spec_title,
--     s.spec_type_code,
--     s.spec_type_name,
--     s.ctd_section,
--     s.stage_code,
--     s.stage_name,
--     s.status_code,
--     s.status_name,
--     s.effective_start_date,
--     s.effective_end_date,
--     s.approval_date,
--     s.approver_name,
--     st.site_code,
--     st.site_name,
--     st.regulatory_region                                        AS site_regulatory_region,
--     st.gmp_status,
--     mk.country_code                                             AS market_country_code,
--     mk.country_name                                             AS market_country_name,
--     mk.market_status,
--     s.compendia_reference,
--     s.supersedes_spec_id,
--
--     -- Section B: Product & Material
--     p.product_name,
--     p.inn_name,
--     p.brand_name,
--     p.dosage_form_code,
--     p.dosage_form_name,
--     p.route_of_administration,
--     p.strength,
--     p.nda_number,
--     m.material_name,
--     m.material_type_code,
--     m.cas_number,
--     m.molecular_weight,
--
--     -- Section C: Regulatory Context
--     rc.regulatory_context_code,
--     rc.region_code,
--     rc.regulatory_body,
--     rc.submission_type,
--
--     -- Section D: Item
--     i.spec_item_key,
--     i.sequence_number,
--     i.test_code,
--     i.test_name,
--     i.test_description,
--     i.test_category_code,
--     i.test_category_name,
--     i.test_subcategory,
--     i.analyte_code,
--     i.criticality,
--     tm.test_method_number,
--     tm.test_method_name,
--     tm.test_method_version,
--     tm.analytical_technique,
--     i.compendia_test_ref,
--     u.uom_code,
--     u.uom_name,
--     i.reporting_type,
--     i.result_precision,
--     i.stage_applicability,
--     i.is_required,
--     i.is_compendial,
--
--     -- Section E: Acceptance Criteria (pivoted)
--     MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.lower_limit_value    END) AS ac_lower_limit,
--     MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.upper_limit_value    END) AS ac_upper_limit,
--     MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.target_value         END) AS ac_target,
--     MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.lower_limit_operator END) AS ac_lower_operator,
--     MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.upper_limit_operator END) AS ac_upper_operator,
--     MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.limit_text           END) AS ac_limit_text,
--     MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.limit_description    END) AS ac_limit_description,
--     MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.limit_basis          END) AS ac_limit_basis,
--     MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.stage_code           END) AS ac_stage,
--     MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.stability_time_point END) AS ac_stability_time_point,
--     MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.stability_condition  END) AS ac_stability_condition,
--     MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.regulatory_basis     END) AS ac_regulatory_basis,
--     MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.is_in_filing         END) AS ac_is_in_filing,
--     MAX(CASE WHEN lt.limit_type_code = 'AC' THEN fu.uom_code            END) AS ac_uom_code,
--
--     -- Section F: NOR (pivoted)
--     MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value    END) AS nor_lower_limit,
--     MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value    END) AS nor_upper_limit,
--     MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.target_value         END) AS nor_target,
--     MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_operator END) AS nor_lower_operator,
--     MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_operator END) AS nor_upper_operator,
--     MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.limit_description    END) AS nor_limit_description,
--     MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN fu.uom_code            END) AS nor_uom_code,
--
--     -- Section G: PAR (pivoted)
--     MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value    END) AS par_lower_limit,
--     MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value    END) AS par_upper_limit,
--     MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.target_value         END) AS par_target,
--     MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_operator END) AS par_lower_operator,
--     MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_operator END) AS par_upper_operator,
--     MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.limit_description    END) AS par_limit_description,
--     MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN fu.uom_code            END) AS par_uom_code,
--
--     -- Section H: Alert (pivoted + SPC fields)
--     MAX(CASE WHEN lt.limit_type_code = 'ALERT' THEN f.lower_limit_value      END) AS alert_lower_limit,
--     MAX(CASE WHEN lt.limit_type_code = 'ALERT' THEN f.upper_limit_value      END) AS alert_upper_limit,
--     MAX(CASE WHEN lt.limit_type_code = 'ALERT' THEN f.limit_description      END) AS alert_limit_description,
--     MAX(CASE WHEN lt.limit_type_code = 'ALERT' THEN f.calculation_method     END) AS alert_calculation_method,
--     MAX(CASE WHEN lt.limit_type_code = 'ALERT' THEN f.sample_size            END) AS alert_sample_size,
--     MAX(CASE WHEN lt.limit_type_code = 'ALERT' THEN f.last_calculated_date   END) AS alert_last_calculated_date,
--
--     -- Section I: Action (pivoted + SPC fields)
--     MAX(CASE WHEN lt.limit_type_code = 'ACTION' THEN f.lower_limit_value     END) AS action_lower_limit,
--     MAX(CASE WHEN lt.limit_type_code = 'ACTION' THEN f.upper_limit_value     END) AS action_upper_limit,
--     MAX(CASE WHEN lt.limit_type_code = 'ACTION' THEN f.limit_description     END) AS action_limit_description,
--     MAX(CASE WHEN lt.limit_type_code = 'ACTION' THEN f.calculation_method    END) AS action_calculation_method,
--     MAX(CASE WHEN lt.limit_type_code = 'ACTION' THEN f.sample_size           END) AS action_sample_size,
--     MAX(CASE WHEN lt.limit_type_code = 'ACTION' THEN f.last_calculated_date  END) AS action_last_calculated_date,
--
--     -- Section J: Hierarchy Validation
--     CASE WHEN
--         -- Lower bounds: PAR_lower <= AC_lower <= NOR_lower
--         (MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value END) <=
--          MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END) OR
--          MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value END) IS NULL)
--         AND
--         (MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END) <=
--          MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value END) OR
--          MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value END) IS NULL)
--         AND
--         -- Upper bounds: PAR_upper >= AC_upper >= NOR_upper
--         (MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value END) >=
--          MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END) OR
--          MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value END) IS NULL)
--         AND
--         (MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END) >=
--          MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value END) OR
--          MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value END) IS NULL)
--     THEN TRUE ELSE FALSE END                                    AS is_hierarchy_valid,
--     NULL                                                        AS hierarchy_violation_notes,
--
--     -- Section K: Metadata
--     current_timestamp()                                         AS load_timestamp,
--     TRUE                                                        AS is_current
--
-- FROM l2_2_spec_unified.dim_specification s
-- JOIN l2_2_spec_unified.dim_specification_item i
--     ON s.spec_key = i.spec_key AND i.is_current = TRUE
-- LEFT JOIN l2_2_spec_unified.fact_specification_limit f
--     ON i.spec_item_key = f.spec_item_key AND f.is_current = TRUE
-- LEFT JOIN l2_2_spec_unified.dim_limit_type lt
--     ON f.limit_type_key = lt.limit_type_key
-- LEFT JOIN l2_2_spec_unified.dim_product p
--     ON s.product_key = p.product_key AND p.is_current = TRUE
-- LEFT JOIN l2_2_spec_unified.dim_material m
--     ON s.material_key = m.material_key AND m.is_current = TRUE
-- LEFT JOIN l2_2_spec_unified.dim_regulatory_context rc
--     ON s.regulatory_context_key = rc.regulatory_context_key
-- LEFT JOIN l2_2_spec_unified.dim_site st
--     ON s.site_key = st.site_key AND st.is_current = TRUE
-- LEFT JOIN l2_2_spec_unified.dim_market mk
--     ON s.market_key = mk.market_key AND mk.is_current = TRUE
-- LEFT JOIN l2_2_spec_unified.dim_test_method tm
--     ON i.test_method_key = tm.test_method_key AND tm.is_current = TRUE
-- LEFT JOIN l2_2_spec_unified.dim_uom u
--     ON i.uom_key = u.uom_key
-- LEFT JOIN l2_2_spec_unified.dim_uom fu
--     ON f.uom_key = fu.uom_key
-- WHERE s.is_current = TRUE
-- GROUP BY
--     s.spec_key, s.spec_number, s.spec_version, s.spec_title,
--     s.spec_type_code, s.spec_type_name, s.ctd_section,
--     s.stage_code, s.stage_name, s.status_code, s.status_name,
--     s.effective_start_date, s.effective_end_date, s.approval_date, s.approver_name,
--     st.site_code, st.site_name, st.regulatory_region, st.gmp_status,
--     mk.country_code, mk.country_name, mk.market_status,
--     s.compendia_reference, s.supersedes_spec_id,
--     p.product_name, p.inn_name, p.brand_name, p.dosage_form_code,
--     p.dosage_form_name, p.route_of_administration, p.strength, p.nda_number,
--     m.material_name, m.material_type_code, m.cas_number, m.molecular_weight,
--     rc.regulatory_context_code, rc.region_code, rc.regulatory_body, rc.submission_type,
--     i.spec_item_key, i.sequence_number, i.test_code, i.test_name, i.test_description,
--     i.test_category_code, i.test_category_name, i.test_subcategory,
--     i.analyte_code, i.criticality,
--     tm.test_method_number, tm.test_method_name, tm.test_method_version,
--     tm.analytical_technique, i.compendia_test_ref,
--     u.uom_code, u.uom_name, i.reporting_type, i.result_precision,
--     i.stage_applicability, i.is_required, i.is_compendial;
