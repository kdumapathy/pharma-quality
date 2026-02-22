-- =============================================================================
-- dspec_specification population
-- =============================================================================
-- Source : l2_2_spec_unified star schema
-- Target : l2_2_spec_unified.dspec_specification
-- Grain  : One row per spec_item with ALL limit types pivoted as columns
--          (AC, NOR, PAR, ALERT, ACTION)
-- Strategy: TRUNCATE + INSERT (full overwrite — idempotent)
-- Note   : fu = dim_uom joined on f.uom_key (limit-level uom)
--          u  = dim_uom joined on i.uom_key (item-level result uom)
-- =============================================================================

USE CATALOG pharma_quality;

TRUNCATE TABLE l2_2_spec_unified.dspec_specification;

INSERT INTO l2_2_spec_unified.dspec_specification
SELECT
    -- -------------------------------------------------------------------------
    -- Section A: Specification Header
    -- -------------------------------------------------------------------------
    s.spec_key,
    s.spec_number,
    s.spec_version,
    s.spec_title,
    s.spec_type_code,
    s.spec_type_name,
    s.ctd_section,
    s.stage_code,
    s.stage_name,
    s.status_code,
    s.status_name,
    s.effective_start_date,
    s.effective_end_date,
    s.approval_date,
    s.approver_name,
    st.site_code,
    st.site_name,
    st.regulatory_region                                        AS site_regulatory_region,
    st.gmp_status,
    mk.country_code                                             AS market_country_code,
    mk.country_name                                             AS market_country_name,
    mk.market_status,
    s.compendia_reference,
    s.supersedes_spec_id,

    -- -------------------------------------------------------------------------
    -- Section B: Product & Material
    -- -------------------------------------------------------------------------
    p.product_name,
    p.inn_name,
    p.brand_name,
    p.dosage_form_code,
    p.dosage_form_name,
    p.route_of_administration,
    p.strength,
    p.nda_number,
    m.material_name,
    m.material_type_code,
    m.cas_number,
    m.molecular_weight,

    -- -------------------------------------------------------------------------
    -- Section C: Regulatory Context
    -- -------------------------------------------------------------------------
    rc.regulatory_context_code,
    rc.region_code,
    rc.regulatory_body,
    rc.submission_type,

    -- -------------------------------------------------------------------------
    -- Section D: Specification Item / Test
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
    tm.test_method_number,
    tm.test_method_name,
    tm.test_method_version,
    tm.analytical_technique,
    i.compendia_test_ref,
    u.uom_code,
    u.uom_name,
    i.reporting_type,
    i.result_precision,
    i.stage_applicability,
    i.is_required,
    i.is_compendial,

    -- -------------------------------------------------------------------------
    -- Section E: Acceptance Criteria (pivoted)
    -- -------------------------------------------------------------------------
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.lower_limit_value    END) AS ac_lower_limit,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.upper_limit_value    END) AS ac_upper_limit,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.target_value         END) AS ac_target,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.lower_limit_operator END) AS ac_lower_operator,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.upper_limit_operator END) AS ac_upper_operator,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.limit_text           END) AS ac_limit_text,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.limit_description    END) AS ac_limit_description,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.limit_basis          END) AS ac_limit_basis,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.stage_code           END) AS ac_stage,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.stability_time_point END) AS ac_stability_time_point,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.stability_condition  END) AS ac_stability_condition,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.regulatory_basis     END) AS ac_regulatory_basis,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.is_in_filing         END) AS ac_is_in_filing,
    MAX(CASE WHEN lt.limit_type_code = 'AC' THEN fu.uom_code            END) AS ac_uom_code,

    -- -------------------------------------------------------------------------
    -- Section F: Normal Operating Range (pivoted)
    -- -------------------------------------------------------------------------
    MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value    END) AS nor_lower_limit,
    MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value    END) AS nor_upper_limit,
    MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.target_value         END) AS nor_target,
    MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_operator END) AS nor_lower_operator,
    MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_operator END) AS nor_upper_operator,
    MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.limit_description    END) AS nor_limit_description,
    MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN fu.uom_code            END) AS nor_uom_code,

    -- -------------------------------------------------------------------------
    -- Section G: Proven Acceptable Range (pivoted)
    -- -------------------------------------------------------------------------
    MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value    END) AS par_lower_limit,
    MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value    END) AS par_upper_limit,
    MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.target_value         END) AS par_target,
    MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_operator END) AS par_lower_operator,
    MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_operator END) AS par_upper_operator,
    MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.limit_description    END) AS par_limit_description,
    MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN fu.uom_code            END) AS par_uom_code,

    -- -------------------------------------------------------------------------
    -- Section H: Alert Limits (pivoted + SPC fields)
    -- -------------------------------------------------------------------------
    MAX(CASE WHEN lt.limit_type_code = 'ALERT' THEN f.lower_limit_value    END) AS alert_lower_limit,
    MAX(CASE WHEN lt.limit_type_code = 'ALERT' THEN f.upper_limit_value    END) AS alert_upper_limit,
    MAX(CASE WHEN lt.limit_type_code = 'ALERT' THEN f.limit_description    END) AS alert_limit_description,
    MAX(CASE WHEN lt.limit_type_code = 'ALERT' THEN f.calculation_method   END) AS alert_calculation_method,
    MAX(CASE WHEN lt.limit_type_code = 'ALERT' THEN f.sample_size          END) AS alert_sample_size,
    MAX(CASE WHEN lt.limit_type_code = 'ALERT' THEN f.last_calculated_date END) AS alert_last_calculated_date,

    -- -------------------------------------------------------------------------
    -- Section I: Action Limits (pivoted + SPC fields)
    -- -------------------------------------------------------------------------
    MAX(CASE WHEN lt.limit_type_code = 'ACTION' THEN f.lower_limit_value    END) AS action_lower_limit,
    MAX(CASE WHEN lt.limit_type_code = 'ACTION' THEN f.upper_limit_value    END) AS action_upper_limit,
    MAX(CASE WHEN lt.limit_type_code = 'ACTION' THEN f.limit_description    END) AS action_limit_description,
    MAX(CASE WHEN lt.limit_type_code = 'ACTION' THEN f.calculation_method   END) AS action_calculation_method,
    MAX(CASE WHEN lt.limit_type_code = 'ACTION' THEN f.sample_size          END) AS action_sample_size,
    MAX(CASE WHEN lt.limit_type_code = 'ACTION' THEN f.last_calculated_date END) AS action_last_calculated_date,

    -- -------------------------------------------------------------------------
    -- Section J: Hierarchy Validation (PAR >= AC >= NOR; NULL = not defined = OK)
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
    THEN TRUE ELSE FALSE END                                    AS is_hierarchy_valid,
    NULL                                                        AS hierarchy_violation_notes,

    -- -------------------------------------------------------------------------
    -- Section K: Metadata
    -- -------------------------------------------------------------------------
    current_timestamp()                                         AS load_timestamp,
    TRUE                                                        AS is_current

FROM l2_2_spec_unified.dim_specification s
JOIN l2_2_spec_unified.dim_specification_item i
    ON s.spec_key = i.spec_key AND i.is_current = TRUE
LEFT JOIN l2_2_spec_unified.fact_specification_limit f
    ON i.spec_item_key = f.spec_item_key AND f.is_current = TRUE
LEFT JOIN l2_2_spec_unified.dim_limit_type lt
    ON f.limit_type_key = lt.limit_type_key
LEFT JOIN l2_2_spec_unified.dim_product p
    ON s.product_key = p.product_key AND p.is_current = TRUE
LEFT JOIN l2_2_spec_unified.dim_material m
    ON s.material_key = m.material_key AND m.is_current = TRUE
LEFT JOIN l2_2_spec_unified.dim_regulatory_context rc
    ON s.regulatory_context_key = rc.regulatory_context_key
LEFT JOIN l2_2_spec_unified.dim_site st
    ON s.site_key = st.site_key AND st.is_current = TRUE
LEFT JOIN l2_2_spec_unified.dim_market mk
    ON s.market_key = mk.market_key AND mk.is_current = TRUE
LEFT JOIN l2_2_spec_unified.dim_test_method tm
    ON i.test_method_key = tm.test_method_key AND tm.is_current = TRUE
LEFT JOIN l2_2_spec_unified.dim_uom u
    ON i.uom_key = u.uom_key
LEFT JOIN l2_2_spec_unified.dim_uom fu
    ON f.uom_key = fu.uom_key
WHERE s.is_current = TRUE
GROUP BY
    s.spec_key, s.spec_number, s.spec_version, s.spec_title,
    s.spec_type_code, s.spec_type_name, s.ctd_section,
    s.stage_code, s.stage_name, s.status_code, s.status_name,
    s.effective_start_date, s.effective_end_date, s.approval_date, s.approver_name,
    st.site_code, st.site_name, st.regulatory_region, st.gmp_status,
    mk.country_code, mk.country_name, mk.market_status,
    s.compendia_reference, s.supersedes_spec_id,
    p.product_name, p.inn_name, p.brand_name, p.dosage_form_code,
    p.dosage_form_name, p.route_of_administration, p.strength, p.nda_number,
    m.material_name, m.material_type_code, m.cas_number, m.molecular_weight,
    rc.regulatory_context_code, rc.region_code, rc.regulatory_body, rc.submission_type,
    i.spec_item_key, i.sequence_number, i.test_code, i.test_name, i.test_description,
    i.test_category_code, i.test_category_name, i.test_subcategory,
    i.analyte_code, i.criticality,
    tm.test_method_number, tm.test_method_name, tm.test_method_version,
    tm.analytical_technique, i.compendia_test_ref,
    u.uom_code, u.uom_name, i.reporting_type, i.result_precision,
    i.stage_applicability, i.is_required, i.is_compendial;

-- =============================================================================
-- V6: dspec_specification — pivoted view for spec review
-- =============================================================================
SELECT
    spec_number,
    spec_version,
    spec_type_code,
    ctd_section,
    site_name,
    site_regulatory_region,
    gmp_status,
    market_country_name,
    market_status,
    sequence_number     AS seq,
    test_name,
    analyte_code,
    criticality,
    test_category_name,
    analytical_technique,
    uom_code,
    ac_lower_limit,
    ac_upper_limit,
    ac_limit_description,
    ac_uom_code,
    ac_regulatory_basis,
    ac_is_in_filing,
    nor_lower_limit,
    nor_upper_limit,
    nor_limit_description,
    par_lower_limit,
    par_upper_limit,
    par_limit_description,
    alert_lower_limit,
    alert_upper_limit,
    alert_calculation_method,
    alert_sample_size,
    is_hierarchy_valid,
    is_current
FROM l2_2_spec_unified.dspec_specification
ORDER BY spec_type_code, spec_number, sequence_number;
