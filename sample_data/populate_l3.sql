-- =============================================================================
-- L3 OBT Population — obt_specification_ctd
-- =============================================================================
-- Source  : l2_2_spec_unified star schema (dim_* + fact_specification_limit)
-- Target  : l3_spec_products.obt_specification_ctd
-- Grain   : One row per spec_item (RELEASE) / per spec_item+time_point (STABILITY)
-- Filter  : status_code = APP, is_current = TRUE, AC with is_in_filing = TRUE
-- Strategy: TRUNCATE + INSERT (full overwrite — idempotent, safe to re-run)
-- =============================================================================

USE CATALOG pharma_quality;

-- Full overwrite — safe to re-run
TRUNCATE TABLE l3_spec_products.obt_specification_ctd;

INSERT INTO l3_spec_products.obt_specification_ctd
SELECT
    -- -------------------------------------------------------------------------
    -- Block 1: Specification Header
    -- -------------------------------------------------------------------------
    s.spec_key,
    s.spec_number,
    s.spec_version,
    s.spec_title,
    s.spec_type_code,
    s.spec_type_name,
    s.ctd_section,
    s.stage_code,           -- lifecycle stage: DEV|CLI|COM
    s.stage_name,
    s.status_code,
    s.effective_start_date,
    s.effective_end_date,
    s.approval_date,
    s.approver_name,

    -- Site (from dim_site)
    st.site_code,
    st.site_name,
    st.regulatory_region        AS site_regulatory_region,

    -- Market (from dim_market)
    mk.country_code             AS market_country_code,
    mk.country_name             AS market_country_name,
    mk.market_status,

    s.compendia_reference,

    -- -------------------------------------------------------------------------
    -- Block 2: Product
    -- -------------------------------------------------------------------------
    p.product_name,
    p.inn_name,
    p.brand_name,
    p.dosage_form_code,
    p.dosage_form_name,
    p.route_of_administration,
    p.strength,
    p.nda_number,

    -- -------------------------------------------------------------------------
    -- Block 3: Material
    -- -------------------------------------------------------------------------
    m.material_name,
    m.material_type_code,
    m.cas_number,
    m.molecular_formula,
    m.molecular_weight,
    m.pharmacopoeia_grade,

    -- -------------------------------------------------------------------------
    -- Block 4: Regulatory Context
    -- -------------------------------------------------------------------------
    rc.regulatory_context_code,
    rc.region_code,
    rc.region_name,
    rc.regulatory_body,
    rc.submission_type,
    rc.guideline_code,

    -- -------------------------------------------------------------------------
    -- Block 5: Test / Specification Item
    -- -------------------------------------------------------------------------
    i.spec_item_key,
    i.sequence_number,
    i.test_code,
    i.test_name,
    i.test_description,
    i.test_category_code,
    i.test_category_name,
    i.test_subcategory,
    i.analyte_code,
    i.criticality,
    i.is_required,
    i.is_compendial,
    i.reporting_type,
    i.result_precision,
    u.uom_code                  AS result_uom_code,
    u.uom_name                  AS result_uom_name,
    i.stage_applicability,

    -- -------------------------------------------------------------------------
    -- Block 6: Analytical Method
    -- -------------------------------------------------------------------------
    tm.test_method_number,
    tm.test_method_name,
    tm.test_method_version,
    tm.method_type_code,
    tm.analytical_technique,
    i.compendia_test_ref,
    tm.validation_status,

    -- -------------------------------------------------------------------------
    -- Block 7: Acceptance Criteria — pivoted from fact_specification_limit
    -- -------------------------------------------------------------------------
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.lower_limit_value   END) AS ac_lower_limit,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.upper_limit_value   END) AS ac_upper_limit,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.target_value        END) AS ac_target,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.lower_limit_operator END) AS ac_lower_operator,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.upper_limit_operator END) AS ac_upper_operator,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.limit_text          END) AS ac_limit_text,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.limit_description   END) AS ac_limit_description,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.limit_basis         END) AS ac_limit_basis,
    u.uom_code                                                               AS ac_uom_code,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.regulatory_basis    END) AS ac_regulatory_basis,

    -- -------------------------------------------------------------------------
    -- Block 8: Stability
    -- -------------------------------------------------------------------------
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.stability_time_point END) AS stability_time_point,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.stability_condition  END) AS stability_condition,
    CASE MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.stability_condition END)
        WHEN '25C60RH'    THEN '25°C/60%RH (Long-term)'
        WHEN '30C65RH'    THEN '30°C/65%RH (Intermediate)'
        WHEN '40C75RH'    THEN '40°C/75%RH (Accelerated)'
        WHEN 'REFRIG'     THEN '2-8°C (Refrigerated)'
        WHEN 'FREEZE'     THEN '-20°C (Frozen)'
        WHEN 'DEEPFREEZE' THEN '-80°C (Deep Freeze)'
        WHEN 'PHOTO'      THEN 'Photostability'
        ELSE NULL
    END                                                                      AS stability_condition_label,
    (i.stage_applicability IN ('RELEASE', 'BOTH'))                           AS is_release_spec,
    (i.stage_applicability IN ('STABILITY', 'BOTH'))                         AS is_stability_spec,

    -- -------------------------------------------------------------------------
    -- Block 9: Internal Limits (NOR / PAR — context only, not in CTD filing)
    -- -------------------------------------------------------------------------
    MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value  END) AS nor_lower_limit,
    MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value  END) AS nor_upper_limit,
    MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.limit_description  END) AS nor_limit_description,
    MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value  END) AS par_lower_limit,
    MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value  END) AS par_upper_limit,
    MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.limit_description  END) AS par_limit_description,

    -- -------------------------------------------------------------------------
    -- Block 10: Hierarchy Validation & Lineage
    -- PAR >= AC >= NOR (NULL = not defined, treated as satisfied)
    -- -------------------------------------------------------------------------
    CASE WHEN
        (MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value END) IS NULL
         OR MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value END) <=
            MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END))
        AND
        (MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END) IS NULL
         OR MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value END) IS NULL
         OR MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END) <=
            MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value END))
        AND
        (MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value END) IS NULL
         OR MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value END) >=
            MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END))
        AND
        (MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END) IS NULL
         OR MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value END) IS NULL
         OR MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END) >=
            MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value END))
    THEN TRUE ELSE FALSE END                                                 AS is_hierarchy_valid,

    s.spec_key                  AS source_spec_key,
    i.spec_item_key             AS source_spec_item_key,
    MAX(f.source_system_code)   AS source_system_code,
    current_timestamp()         AS load_timestamp,
    '2.0.0'                     AS data_product_version

FROM l2_2_spec_unified.dim_specification s
JOIN l2_2_spec_unified.dim_specification_item i
    ON s.spec_key = i.spec_key AND i.is_current = TRUE
JOIN l2_2_spec_unified.fact_specification_limit f
    ON i.spec_item_key = f.spec_item_key AND f.is_current = TRUE
JOIN l2_2_spec_unified.dim_limit_type lt
    ON f.limit_type_key = lt.limit_type_key
LEFT JOIN l2_2_spec_unified.dim_site st
    ON s.site_key = st.site_key AND st.is_current = TRUE
LEFT JOIN l2_2_spec_unified.dim_market mk
    ON s.market_key = mk.market_key AND mk.is_current = TRUE
LEFT JOIN l2_2_spec_unified.dim_product p
    ON s.product_key = p.product_key AND p.is_current = TRUE
LEFT JOIN l2_2_spec_unified.dim_material m
    ON s.material_key = m.material_key AND m.is_current = TRUE
LEFT JOIN l2_2_spec_unified.dim_regulatory_context rc
    ON s.regulatory_context_key = rc.regulatory_context_key
LEFT JOIN l2_2_spec_unified.dim_test_method tm
    ON i.test_method_key = tm.test_method_key AND tm.is_current = TRUE
LEFT JOIN l2_2_spec_unified.dim_uom u
    ON i.uom_key = u.uom_key
WHERE s.status_code = 'APP'
  AND s.is_current = TRUE
  AND EXISTS (
      SELECT 1
      FROM l2_2_spec_unified.fact_specification_limit fac
      JOIN l2_2_spec_unified.dim_limit_type lac ON fac.limit_type_key = lac.limit_type_key
      WHERE fac.spec_item_key = i.spec_item_key
        AND lac.limit_type_code = 'AC'
        AND fac.is_in_filing = TRUE
        AND fac.is_current = TRUE
  )
GROUP BY
    s.spec_key, s.spec_number, s.spec_version, s.spec_title,
    s.spec_type_code, s.spec_type_name, s.ctd_section,
    s.stage_code, s.stage_name, s.status_code,
    s.effective_start_date, s.effective_end_date, s.approval_date, s.approver_name,
    st.site_code, st.site_name, st.regulatory_region,
    mk.country_code, mk.country_name, mk.market_status,
    s.compendia_reference,
    p.product_name, p.inn_name, p.brand_name,
    p.dosage_form_code, p.dosage_form_name, p.route_of_administration,
    p.strength, p.nda_number,
    m.material_name, m.material_type_code, m.cas_number,
    m.molecular_formula, m.molecular_weight, m.pharmacopoeia_grade,
    rc.regulatory_context_code, rc.region_code, rc.region_name,
    rc.regulatory_body, rc.submission_type, rc.guideline_code,
    i.spec_item_key, i.sequence_number, i.test_code, i.test_name,
    i.test_description, i.test_category_code, i.test_category_name, i.test_subcategory,
    i.analyte_code, i.criticality, i.is_required, i.is_compendial,
    i.reporting_type, i.result_precision, i.stage_applicability,
    tm.test_method_number, tm.test_method_name, tm.test_method_version,
    tm.method_type_code, tm.analytical_technique, i.compendia_test_ref,
    tm.validation_status,
    u.uom_code, u.uom_name;

-- =============================================================================
-- V4: L3 OBT — CTD-ready final output (3.2.P.5.1 — Drug Product)
-- =============================================================================
SELECT
    spec_number,
    spec_version,
    ctd_section,
    site_name                               AS manufacturing_site,
    site_regulatory_region                  AS regulatory_region,
    market_country_name                     AS market,
    inn_name,
    strength,
    dosage_form_name,
    nda_number,
    regulatory_context_code,
    submission_type,
    sequence_number                         AS seq,
    test_name,
    analyte_code,
    criticality,
    test_category_name,
    analytical_technique,
    validation_status                       AS method_validation,
    ac_lower_operator,
    ac_lower_limit,
    ac_upper_limit,
    ac_upper_operator,
    ac_limit_text,
    ac_limit_description                    AS acceptance_criteria,
    ac_uom_code                             AS uom,
    ac_regulatory_basis,
    nor_lower_limit,
    nor_upper_limit,
    nor_limit_description                   AS nor_description,
    par_lower_limit,
    par_upper_limit,
    par_limit_description                   AS par_description,
    is_hierarchy_valid,
    is_release_spec,
    data_product_version
FROM l3_spec_products.obt_specification_ctd
WHERE spec_type_code = 'DP'
ORDER BY sequence_number;

-- =============================================================================
-- obt_acceptance_criteria population
-- Source: obt_specification_ctd (already loaded above — avoids re-joining L2.2)
-- Adds computed width metrics: ac_width, nor_width, par_width,
--   nor_tightness_pct (NOR_width / AC_width * 100),
--   par_vs_ac_factor  (PAR_width / AC_width)
-- =============================================================================

TRUNCATE TABLE l3_spec_products.obt_acceptance_criteria;

INSERT INTO l3_spec_products.obt_acceptance_criteria
SELECT
    spec_number,
    spec_version,
    spec_type_code,
    spec_type_name,
    stage_code,
    ctd_section,
    effective_start_date                                AS effective_date,
    product_name,
    inn_name,
    material_name,
    dosage_form_name,
    strength,
    nda_number,
    region_code,
    regulatory_body,
    sequence_number,
    test_name,
    test_category_code,
    test_category_name,
    test_subcategory,
    test_method_number,
    compendia_test_ref,
    result_uom_code,
    ac_lower_limit,
    ac_upper_limit,
    ac_target,
    ac_lower_operator,
    ac_upper_operator,
    ac_limit_text,
    ac_limit_description,
    ac_limit_basis,
    ac_uom_code,
    CASE
        WHEN is_release_spec AND is_stability_spec THEN 'BOTH'
        WHEN is_stability_spec                     THEN 'STABILITY'
        ELSE                                            'RELEASE'
    END                                                 AS ac_stage,
    stability_time_point,
    stability_condition,
    ac_regulatory_basis,
    is_compendial,
    nor_lower_limit,
    nor_upper_limit,
    par_lower_limit,
    par_upper_limit,
    -- Computed: range widths
    CASE WHEN ac_upper_limit  IS NOT NULL AND ac_lower_limit  IS NOT NULL
         THEN ac_upper_limit  - ac_lower_limit  END     AS ac_width,
    CASE WHEN nor_upper_limit IS NOT NULL AND nor_lower_limit IS NOT NULL
         THEN nor_upper_limit - nor_lower_limit END     AS nor_width,
    CASE WHEN par_upper_limit IS NOT NULL AND par_lower_limit IS NOT NULL
         THEN par_upper_limit - par_lower_limit END     AS par_width,
    -- NOR tightness: how narrow NOR is relative to AC (%)
    CASE WHEN ac_upper_limit  IS NOT NULL AND ac_lower_limit  IS NOT NULL
              AND nor_upper_limit IS NOT NULL AND nor_lower_limit IS NOT NULL
              AND (ac_upper_limit - ac_lower_limit) > 0
         THEN ROUND((nor_upper_limit - nor_lower_limit)
                    / (ac_upper_limit - ac_lower_limit) * 100, 4)
         END                                            AS nor_tightness_pct,
    -- PAR vs AC factor: how many times wider PAR is vs AC
    CASE WHEN ac_upper_limit  IS NOT NULL AND ac_lower_limit  IS NOT NULL
              AND par_upper_limit IS NOT NULL AND par_lower_limit IS NOT NULL
              AND (ac_upper_limit - ac_lower_limit) > 0
         THEN ROUND((par_upper_limit - par_lower_limit)
                    / (ac_upper_limit - ac_lower_limit), 4)
         END                                            AS par_vs_ac_factor,
    is_hierarchy_valid,
    source_system_code,
    current_timestamp()                                 AS load_timestamp
FROM l3_spec_products.obt_specification_ctd;

-- =============================================================================
-- V5: obt_acceptance_criteria — limit comparison with width analytics
-- =============================================================================
SELECT
    spec_number,
    spec_version,
    spec_type_code,
    sequence_number                         AS seq,
    test_name,
    ac_limit_description                    AS acceptance_criteria,
    ac_uom_code                             AS uom,
    ac_regulatory_basis,
    ac_width,
    nor_lower_limit,
    nor_upper_limit,
    nor_width,
    nor_tightness_pct,
    par_lower_limit,
    par_upper_limit,
    par_width,
    par_vs_ac_factor,
    is_hierarchy_valid
FROM l3_spec_products.obt_acceptance_criteria
ORDER BY spec_type_code, spec_number, sequence_number;
