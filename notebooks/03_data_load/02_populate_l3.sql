-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Populate L3 Final Data Products
-- MAGIC Builds the L3 One Big Tables from L2.2 star schema:
-- MAGIC - `obt_specification_ctd` — CTD-ready output (grain: spec × item × limit)
-- MAGIC - `obt_acceptance_criteria` — Acceptance criteria with hierarchy metrics (grain: spec × item)

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## obt_specification_ctd

-- COMMAND ----------

MERGE INTO l3_spec_products.obt_specification_ctd AS tgt
USING (
    SELECT
        HASH(f.spec_limit_key)          AS obt_ctd_key,

        -- Specification
        s.spec_number,
        s.spec_version,
        s.spec_title,
        s.spec_type_code,
        s.spec_type_name,
        s.status_code,
        s.stage_code,

        -- Product & Material
        p.product_name,
        p.product_family,
        s.dosage_form,
        s.strength,
        m.material_name,
        m.material_type,

        -- Site & Market
        st.site_name,
        st.country_code,
        mk.market_name,
        mk.region_code,

        -- Test / Item
        i.test_name,
        i.test_code,
        i.test_category_code,
        i.test_category_name,
        i.criticality_code,
        i.sequence_number,
        i.reporting_type,
        i.is_required,

        -- Method
        tm.method_name,
        tm.method_number,
        tm.technique,
        i.compendia_test_ref,

        -- Limit
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

        -- Stability
        f.stability_time_point,
        f.stability_condition,

        -- Regulatory
        s.ctd_section,
        f.is_in_filing,
        f.regulatory_basis,

        -- Dates
        ds.full_date AS effective_start_date,
        de.full_date AS effective_end_date,
        da.full_date AS approval_date,

        -- Metadata
        f.is_current,
        CURRENT_TIMESTAMP() AS load_timestamp

    FROM l2_2_spec_unified.fact_specification_limit f
    JOIN l2_2_spec_unified.dim_specification s       ON f.spec_key = s.spec_key
    JOIN l2_2_spec_unified.dim_specification_item i  ON f.spec_item_key = i.spec_item_key
    JOIN l2_2_spec_unified.dim_limit_type lt         ON f.limit_type_key = lt.limit_type_key
    LEFT JOIN l2_2_spec_unified.dim_product p        ON s.product_key = p.product_key
    LEFT JOIN l2_2_spec_unified.dim_material m       ON s.material_key = m.material_key
    LEFT JOIN l2_2_spec_unified.dim_site st          ON s.site_key = st.site_key
    LEFT JOIN l2_2_spec_unified.dim_market mk        ON s.market_key = mk.market_key
    LEFT JOIN l2_2_spec_unified.dim_test_method tm   ON i.test_method_key = tm.test_method_key
    LEFT JOIN l2_2_spec_unified.dim_uom u            ON f.uom_key = u.uom_key
    LEFT JOIN l2_2_spec_unified.dim_date ds          ON f.effective_start_date_key = ds.date_key
    LEFT JOIN l2_2_spec_unified.dim_date de          ON f.effective_end_date_key = de.date_key
    LEFT JOIN l2_2_spec_unified.dim_date da          ON s.approval_date_key = da.date_key
) AS src
ON tgt.obt_ctd_key = src.obt_ctd_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## obt_acceptance_criteria

-- COMMAND ----------

MERGE INTO l3_spec_products.obt_acceptance_criteria AS tgt
USING (
    SELECT
        HASH(s.spec_key, i.spec_item_key)   AS obt_ac_key,

        -- Specification
        s.spec_number,
        s.spec_version,
        s.spec_type_code,
        s.status_code,
        s.stage_code,

        -- Product & Material
        p.product_name,
        m.material_name,
        s.dosage_form,
        s.strength,

        -- Test / Item
        i.test_name,
        i.test_code,
        i.test_category_code,
        i.criticality_code,
        u.uom_code,
        i.sequence_number,
        i.reporting_type,

        -- AC limits
        MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END) AS ac_lower_limit,
        MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END) AS ac_upper_limit,
        MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.target_value END)      AS ac_target_value,
        MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END) -
            MAX(CASE WHEN lt.limit_type_code = 'AC' THEN f.lower_limit_value END) AS ac_width,

        -- NOR limits
        MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value END) AS nor_lower_limit,
        MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value END) AS nor_upper_limit,
        MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.target_value END)      AS nor_target_value,
        MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value END) -
            MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value END) AS nor_width,

        -- PAR limits
        MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value END) AS par_lower_limit,
        MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value END) AS par_upper_limit,
        MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.target_value END)      AS par_target_value,
        MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value END) -
            MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value END) AS par_width,

        -- Hierarchy metrics
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

        -- Metadata
        TRUE AS is_current,
        CURRENT_TIMESTAMP() AS load_timestamp

    FROM l2_2_spec_unified.fact_specification_limit f
    JOIN l2_2_spec_unified.dim_specification s       ON f.spec_key = s.spec_key
    JOIN l2_2_spec_unified.dim_specification_item i  ON f.spec_item_key = i.spec_item_key
    JOIN l2_2_spec_unified.dim_limit_type lt         ON f.limit_type_key = lt.limit_type_key
    LEFT JOIN l2_2_spec_unified.dim_product p        ON s.product_key = p.product_key
    LEFT JOIN l2_2_spec_unified.dim_material m       ON s.material_key = m.material_key
    LEFT JOIN l2_2_spec_unified.dim_uom u            ON i.uom_key = u.uom_key
    WHERE f.is_current = TRUE
      AND f.stage_code = 'RELEASE'
      AND lt.limit_type_code IN ('AC', 'NOR', 'PAR')
    GROUP BY
        s.spec_key, i.spec_item_key,
        s.spec_number, s.spec_version, s.spec_type_code, s.status_code, s.stage_code,
        p.product_name, m.material_name, s.dosage_form, s.strength,
        i.test_name, i.test_code, i.test_category_code, i.criticality_code,
        u.uom_code, i.sequence_number, i.reporting_type
) AS src
ON tgt.obt_ac_key = src.obt_ac_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify L3 Tables

-- COMMAND ----------

SELECT 'obt_specification_ctd' AS table_name, COUNT(*) AS rows FROM l3_spec_products.obt_specification_ctd
UNION ALL
SELECT 'obt_acceptance_criteria', COUNT(*) FROM l3_spec_products.obt_acceptance_criteria;
