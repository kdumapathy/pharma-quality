-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Populate L3 Final Data Products
-- MAGIC Builds the L3 One Big Tables from L2.2 star schema:
-- MAGIC - `obt_specification_ctd` — CTD-ready output (grain: spec × item × limit)
-- MAGIC - `obt_acceptance_criteria` — Acceptance criteria with hierarchy metrics (grain: spec × item)
-- MAGIC - `obt_stability_results` — Stability analytical results (grain: batch × test × condition × time point)
-- MAGIC
-- MAGIC Aligned with PQ/CMC field naming conventions and ICH Q6A/Q1A standards.

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## obt_specification_ctd

-- COMMAND ----------

TRUNCATE TABLE l3_data_product.obt_specification_ctd;

-- COMMAND ----------

-- DBTITLE 1,TRUNCATE and LOAD obt_specification_ctd


INSERT INTO l3_data_product.obt_specification_ctd (
    obt_ctd_key,
    spec_key,
    spec_number,
    spec_version,
    spec_title,
    spec_type_code,
    spec_type_name,
    status_code,
    stage_code,
    stage_name,
    product_name,
    inn_name,
    brand_name,
    dosage_form_code,
    dosage_form_name,
    route_of_administration,
    strength,
    nda_number,
    material_name,
    material_type_code,
    cas_number,
    site_code,
    site_name,
    site_regulatory_region,
    region_code,
    market_country_code,
    market_country_name,
    market_status,
    test_name,
    test_code,
    test_category_code,
    test_category_name,
    criticality,
    sequence_number,
    is_required,
    reporting_type,
    compendia_reference,
    compendia_test_ref,
    test_method_name,
    test_method_number,
    limit_type_code,
    limit_type_name,
    lower_limit_value,
    upper_limit_value,
    target_value,
    limit_description,
    limit_text,
    uom_code,
    uom_name,
    limit_basis,
    stability_time_point,
    stability_condition,
    ctd_section,
    is_in_filing,
    regulatory_basis,
    effective_start_date,
    effective_end_date,
    approval_date,
    approver_name,
    is_current,
    load_timestamp
)
SELECT
    HASH(f.spec_limit_key)                  AS obt_ctd_key,
    f.spec_key,
    s.spec_number,
    s.spec_version,
    s.spec_title,
    s.spec_type_code,
    s.spec_type_name,
    s.status_code,
    s.stage_code,
    s.stage_name,
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
    st.site_code,
    st.site_name,
    st.regulatory_region                    AS site_regulatory_region,
    mk.region_code,
    mk.country_code                         AS market_country_code,
    mk.country_name                         AS market_country_name,
    mk.market_status,
    COALESCE(NULLIF(TRIM(i.test_name), ''), NULLIF(TRIM(i.test_code), ''), 'UNKNOWN_TEST') AS test_name,
    i.test_code,
    i.test_category_code,
    i.test_category_name,
    i.criticality,
    i.sequence_number,
    i.is_required,
    i.reporting_type,
    s.compendia_reference,
    i.compendia_test_ref,
    tm.test_method_name,
    tm.test_method_number,
    lt.limit_type_code,
    lt.limit_type_name,
    f.lower_limit_value,
    f.upper_limit_value,
    f.target_value,
    f.limit_description,
    f.limit_text,
    u.uom_code,
    u.uom_name,
    f.limit_basis,
    f.stability_time_point,
    f.stability_condition,
    s.ctd_section,
    f.is_in_filing,
    f.regulatory_basis,
    ds.full_date                            AS effective_start_date,
    de.full_date                            AS effective_end_date,
    s.approval_date,
    s.approver_name,
    f.is_current,
    CURRENT_TIMESTAMP()                     AS load_timestamp
FROM l2_2_unified_model.fact_specification_limit f
JOIN l2_2_unified_model.dim_specification s       ON f.spec_key = s.spec_key
JOIN l2_2_unified_model.dim_specification_item i  ON f.spec_item_key = i.spec_item_key
JOIN l2_2_unified_model.dim_limit_type lt         ON f.limit_type_key = lt.limit_type_key
LEFT JOIN l2_2_unified_model.dim_product p        ON s.product_key = p.product_key
LEFT JOIN l2_2_unified_model.dim_material m       ON s.material_key = m.material_key
LEFT JOIN l2_2_unified_model.dim_site st          ON s.site_key = st.site_key
LEFT JOIN l2_2_unified_model.dim_market mk        ON s.market_key = mk.market_key
LEFT JOIN l2_2_unified_model.dim_test_method tm   ON i.test_method_key = tm.test_method_key
LEFT JOIN l2_2_unified_model.dim_uom u            ON f.uom_key = u.uom_key
LEFT JOIN l2_2_unified_model.dim_date ds          ON f.effective_start_date_key = ds.date_key
LEFT JOIN l2_2_unified_model.dim_date de          ON f.effective_end_date_key = de.date_key
WHERE f.is_current = TRUE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## obt_acceptance_criteria

-- COMMAND ----------

TRUNCATE TABLE l3_data_product.obt_acceptance_criteria;

-- COMMAND ----------

-- DBTITLE 1,LOAD obt_acceptance_criteria (PQ/CMC aligned)
INSERT INTO l3_data_product.obt_acceptance_criteria (
    obt_ac_key,
    spec_number,
    spec_version,
    spec_type_code,
    status_code,
    stage_code,
    product_name,
    material_name,
    dosage_form,
    strength,
    test_name,
    test_code,
    test_category_code,
    criticality,
    uom_code,
    sequence_number,
    reporting_type,
    is_required,
    ac_lower_limit,
    ac_upper_limit,
    ac_target_value,
    ac_width,
    nor_lower_limit,
    nor_upper_limit,
    nor_target_value,
    nor_width,
    par_lower_limit,
    par_upper_limit,
    par_target_value,
    par_width,
    nor_tightness_pct,
    par_vs_ac_factor,
    is_hierarchy_valid,
    is_current,
    load_timestamp
)
SELECT
    HASH(s.spec_number, s.spec_version, s.spec_type_code, s.stage_code, p.product_name, m.material_name, p.dosage_form_name, p.strength, COALESCE(NULLIF(TRIM(i.test_name), ''), NULLIF(TRIM(i.test_code), ''), 'UNKNOWN_TEST'), i.test_category_code, i.sequence_number) AS obt_ac_key,
    s.spec_number,
    s.spec_version,
    s.spec_type_code,
    s.status_code,
    s.stage_code,
    p.product_name,
    m.material_name,
    p.dosage_form_name                      AS dosage_form,
    p.strength,
    COALESCE(NULLIF(TRIM(i.test_name), ''), NULLIF(TRIM(i.test_code), ''), 'UNKNOWN_TEST') AS test_name,
    i.test_code,
    i.test_category_code,
    i.criticality,
    u.uom_code,
    i.sequence_number,
    i.reporting_type,
    i.is_required,
    MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END) AS ac_lower_limit,
    MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END) AS ac_upper_limit,
    MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.target_value END)      AS ac_target_value,
    MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END) -
        MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.lower_limit_value END) AS ac_width,
    MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value END) AS nor_lower_limit,
    MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value END) AS nor_upper_limit,
    MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.target_value END)      AS nor_target_value,
    MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value END) -
        MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value END) AS nor_width,
    MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value END) AS par_lower_limit,
    MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value END) AS par_upper_limit,
    MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.target_value END)      AS par_target_value,
    MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value END) -
        MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value END) AS par_width,
    CASE WHEN (MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.upper_limit_value END) -
               MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.lower_limit_value END)) > 0
        THEN CAST(
            (MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value END) -
             MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value END)) /
            (MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END) -
             MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END)) * 100
            AS DECIMAL(8, 4))
        ELSE NULL END AS nor_tightness_pct,
    CASE WHEN (MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.upper_limit_value END) -
               MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.lower_limit_value END)) > 0
        THEN CAST(
            (MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value END) -
             MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value END)) /
            (MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END) -
             MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END))
            AS DECIMAL(8, 4))
        ELSE NULL END AS par_vs_ac_factor,
    CASE WHEN
        (MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value END) <=
         MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END)
         OR MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value END) IS NULL)
        AND
        (MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END) <=
         MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value END)
         OR MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value END) IS NULL)
        AND
        (MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value END) >=
         MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END)
         OR MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value END) IS NULL)
        AND
        (MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END) >=
         MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value END)
         OR MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value END) IS NULL)
    THEN TRUE ELSE FALSE END AS is_hierarchy_valid,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM l2_2_unified_model.fact_specification_limit f
JOIN l2_2_unified_model.dim_specification s       ON f.spec_key = s.spec_key
JOIN l2_2_unified_model.dim_specification_item i  ON f.spec_item_key = i.spec_item_key
JOIN l2_2_unified_model.dim_limit_type lt         ON f.limit_type_key = lt.limit_type_key
LEFT JOIN l2_2_unified_model.dim_product p        ON s.product_key = p.product_key
LEFT JOIN l2_2_unified_model.dim_material m       ON s.material_key = m.material_key
LEFT JOIN l2_2_unified_model.dim_uom u            ON i.uom_key = u.uom_key
WHERE f.is_current = TRUE
  AND COALESCE(UPPER(TRIM(f.stage_code)), 'RELEASE') IN ('RELEASE', 'BOTH')
  AND lt.limit_type_code IN ('AC', 'NOR', 'PAR')
GROUP BY
    s.spec_number, s.spec_version, s.spec_type_code, s.status_code, s.stage_code,
    p.product_name, m.material_name, p.dosage_form_name, p.strength,
    COALESCE(NULLIF(TRIM(i.test_name), ''), NULLIF(TRIM(i.test_code), ''), 'UNKNOWN_TEST'),
    i.test_code, i.test_category_code, i.criticality, u.uom_code,
    i.sequence_number, i.reporting_type, i.is_required;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## obt_stability_results
-- MAGIC Fully denormalized stability analytical results for trend analysis and reporting.

-- COMMAND ----------

TRUNCATE TABLE l3_data_product.obt_stability_results;

-- COMMAND ----------

-- DBTITLE 1,TRUNCATE and LOAD obt_stability_results (PQ/CMC aligned)


INSERT INTO l3_data_product.obt_stability_results (
    obt_stab_key,
    batch_number,
    batch_type,
    manufacturing_date,
    expiry_date,
    batch_size,
    batch_size_unit,
    product_name,
    product_family,
    inn_name,
    dosage_form,
    strength,
    material_name,
    material_type,
    site_name,
    site_code,
    country_code,
    lab_name,
    spec_number,
    spec_version,
    spec_type_code,
    test_name,
    test_code,
    test_category_code,
    test_category_name,
    criticality,
    method_name,
    technique,
    stability_study_id,
    storage_condition_code,
    storage_condition_name,
    ich_condition_type,
    time_point_code,
    time_point_months,
    time_point_name,
    result_value,
    result_text,
    percent_label_claim,
    uom_code,
    uom_name,
    result_status_code,
    reported_lower_limit,
    reported_upper_limit,
    reported_target,
    spec_ac_lower_limit,
    spec_ac_upper_limit,
    is_oos,
    is_oot,
    instrument_name,
    analyst_name,
    reviewer_name,
    report_id,
    coa_number,
    test_date,
    pull_date,
    is_current,
    load_timestamp
)
SELECT
    HASH(f.analytical_result_key)       AS obt_stab_key,
    b.batch_number,
    b.batch_type,
    b.manufacturing_date,
    b.expiry_date,
    b.batch_size,
    b.batch_size_unit,
    p.product_name,
    p.product_family,
    p.inn_name,
    p.dosage_form_name                  AS dosage_form,
    p.strength,
    m.material_name,
    m.material_type_name                AS material_type,
    st.site_name,
    st.site_code,
    st.country_code,
    f.lab_name,
    s.spec_number,
    s.spec_version,
    s.spec_type_code,
    COALESCE(NULLIF(TRIM(i.test_name), ''), NULLIF(TRIM(i.test_code), ''), 'UNKNOWN_TEST') AS test_name,
    i.test_code,
    i.test_category_code,
    i.test_category_name,
    i.criticality,
    tm.test_method_name                 AS method_name,
    tm.analytical_technique             AS technique,
    f.stability_study_id,
    sc.condition_code                   AS storage_condition_code,
    sc.condition_name                   AS storage_condition_name,
    sc.ich_condition_type,
    tp.timepoint_code                   AS time_point_code,
    tp.timepoint_months                 AS time_point_months,
    tp.timepoint_name                   AS time_point_name,
    f.result_value,
    f.result_text,
    f.percent_label_claim,
    u.uom_code,
    u.uom_name,
    f.result_status_code,
    f.reported_lower_limit,
    f.reported_upper_limit,
    f.reported_target,
    ac_lim.lower_limit_value            AS spec_ac_lower_limit,
    ac_lim.upper_limit_value            AS spec_ac_upper_limit,
    f.is_oos,
    f.is_oot,
    inst.instrument_name,
    f.analyst_name,
    f.reviewer_name,
    f.report_id,
    f.coa_number,
    dt.full_date                        AS test_date,
    CAST(NULL AS DATE)                  AS pull_date,
    f.is_current,
    CURRENT_TIMESTAMP()                 AS load_timestamp
FROM l2_2_unified_model.fact_analytical_result f
JOIN l2_2_unified_model.dim_batch b                  ON f.batch_key = b.batch_key
LEFT JOIN l2_2_unified_model.dim_specification s     ON f.spec_key = s.spec_key
LEFT JOIN l2_2_unified_model.dim_specification_item i ON f.spec_item_key = i.spec_item_key
LEFT JOIN l2_2_unified_model.dim_product p           ON s.product_key = p.product_key
LEFT JOIN l2_2_unified_model.dim_material m          ON s.material_key = m.material_key
LEFT JOIN l2_2_unified_model.dim_site st             ON s.site_key = st.site_key
LEFT JOIN l2_2_unified_model.dim_test_method tm      ON i.test_method_key = tm.test_method_key
LEFT JOIN l2_2_unified_model.dim_stability_condition sc ON f.condition_key = sc.condition_key
LEFT JOIN l2_2_unified_model.dim_timepoint tp        ON f.timepoint_key = tp.timepoint_key
LEFT JOIN l2_2_unified_model.dim_uom u               ON f.uom_key = u.uom_key
LEFT JOIN l2_2_unified_model.dim_instrument inst     ON f.instrument_key = inst.instrument_key
LEFT JOIN l2_2_unified_model.dim_date dt             ON f.test_date_key = dt.date_key
-- Join to get specification AC limits for the same spec item
LEFT JOIN (
    SELECT spec_item_key, lower_limit_value, upper_limit_value
    FROM l2_2_unified_model.fact_specification_limit fsl
    JOIN l2_2_unified_model.dim_limit_type lt ON fsl.limit_type_key = lt.limit_type_key
    WHERE lt.limit_type_code = 'AC'
      AND fsl.is_current = TRUE
      AND COALESCE(fsl.stage_code, 'RELEASE') = 'RELEASE'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fsl.spec_item_key ORDER BY fsl.load_timestamp DESC) = 1
) ac_lim ON f.spec_item_key = ac_lim.spec_item_key
WHERE i.test_name IS NOT NULL;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify L3 Tables

-- COMMAND ----------

SELECT 'obt_specification_ctd' AS table_name, COUNT(*) AS rows FROM l3_data_product.obt_specification_ctd
UNION ALL
SELECT 'obt_acceptance_criteria', COUNT(*) FROM l3_data_product.obt_acceptance_criteria
UNION ALL
SELECT 'obt_stability_results', COUNT(*) FROM l3_data_product.obt_stability_results;
