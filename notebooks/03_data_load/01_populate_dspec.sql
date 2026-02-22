-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Populate dspec_specification — Denormalized Specification Table
-- MAGIC Joins dim_specification, dim_specification_item, and fact_specification_limit
-- MAGIC to produce a wide denormalized table with pivoted limit columns per item.

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

USE SCHEMA l2_2_spec_unified;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Build denormalized specification rows

-- COMMAND ----------

MERGE INTO dspec_specification AS tgt
USING (
    SELECT
        -- Surrogate key: hash-based for idempotency
        HASH(s.spec_key, i.spec_item_key, s.is_current)    AS dspec_key,
        s.spec_key,
        i.spec_item_key,

        -- Specification header
        s.spec_number,
        s.spec_version,
        s.spec_title,
        s.spec_type_code,
        s.spec_type_name,
        p.product_name,
        m.material_name,
        st.site_name,
        mk.market_name,
        s.status_code,
        s.stage_code,
        s.dosage_form,
        s.strength,

        -- Item
        i.test_name,
        i.test_code,
        i.test_category_code,
        i.criticality_code,
        u.uom_code,
        i.sequence_number,
        i.reporting_type,
        i.is_required,

        -- Pivoted AC limits
        MAX(CASE WHEN lt.limit_type_code = 'AC'     THEN f.lower_limit_value END)  AS ac_lower_limit,
        MAX(CASE WHEN lt.limit_type_code = 'AC'     THEN f.upper_limit_value END)  AS ac_upper_limit,
        MAX(CASE WHEN lt.limit_type_code = 'AC'     THEN f.target_value END)       AS ac_target_value,
        MAX(CASE WHEN lt.limit_type_code = 'AC'     THEN f.limit_description END)  AS ac_limit_description,

        -- Pivoted NOR limits
        MAX(CASE WHEN lt.limit_type_code = 'NOR'    THEN f.lower_limit_value END)  AS nor_lower_limit,
        MAX(CASE WHEN lt.limit_type_code = 'NOR'    THEN f.upper_limit_value END)  AS nor_upper_limit,
        MAX(CASE WHEN lt.limit_type_code = 'NOR'    THEN f.target_value END)       AS nor_target_value,
        MAX(CASE WHEN lt.limit_type_code = 'NOR'    THEN f.limit_description END)  AS nor_limit_description,

        -- Pivoted PAR limits
        MAX(CASE WHEN lt.limit_type_code = 'PAR'    THEN f.lower_limit_value END)  AS par_lower_limit,
        MAX(CASE WHEN lt.limit_type_code = 'PAR'    THEN f.upper_limit_value END)  AS par_upper_limit,
        MAX(CASE WHEN lt.limit_type_code = 'PAR'    THEN f.target_value END)       AS par_target_value,
        MAX(CASE WHEN lt.limit_type_code = 'PAR'    THEN f.limit_description END)  AS par_limit_description,

        -- Pivoted Alert limits
        MAX(CASE WHEN lt.limit_type_code = 'ALERT'  THEN f.lower_limit_value END)  AS alert_lower_limit,
        MAX(CASE WHEN lt.limit_type_code = 'ALERT'  THEN f.upper_limit_value END)  AS alert_upper_limit,
        MAX(CASE WHEN lt.limit_type_code = 'ALERT'  THEN f.limit_description END)  AS alert_limit_description,

        -- Pivoted Action limits
        MAX(CASE WHEN lt.limit_type_code = 'ACTION' THEN f.lower_limit_value END)  AS action_lower_limit,
        MAX(CASE WHEN lt.limit_type_code = 'ACTION' THEN f.upper_limit_value END)  AS action_upper_limit,
        MAX(CASE WHEN lt.limit_type_code = 'ACTION' THEN f.limit_description END)  AS action_limit_description,

        -- Hierarchy validation
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

        s.is_current,
        CURRENT_TIMESTAMP() AS load_timestamp

    FROM dim_specification s
    JOIN dim_specification_item i ON i.spec_key = s.spec_key AND i.is_current = TRUE
    LEFT JOIN fact_specification_limit f ON f.spec_key = s.spec_key
        AND f.spec_item_key = i.spec_item_key
        AND f.is_current = TRUE
        AND f.stage_code = 'RELEASE'
    LEFT JOIN dim_limit_type lt ON f.limit_type_key = lt.limit_type_key
    LEFT JOIN dim_product p ON s.product_key = p.product_key
    LEFT JOIN dim_material m ON s.material_key = m.material_key
    LEFT JOIN dim_site st ON s.site_key = st.site_key
    LEFT JOIN dim_market mk ON s.market_key = mk.market_key
    LEFT JOIN dim_uom u ON i.uom_key = u.uom_key
    WHERE s.is_current = TRUE
    GROUP BY
        s.spec_key, i.spec_item_key,
        s.spec_number, s.spec_version, s.spec_title, s.spec_type_code, s.spec_type_name,
        p.product_name, m.material_name, st.site_name, mk.market_name,
        s.status_code, s.stage_code, s.dosage_form, s.strength,
        i.test_name, i.test_code, i.test_category_code, i.criticality_code,
        u.uom_code, i.sequence_number, i.reporting_type, i.is_required,
        s.is_current
) AS src
ON tgt.dspec_key = src.dspec_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify

-- COMMAND ----------

SELECT spec_number, test_name, ac_limit_description, nor_limit_description, par_limit_description, is_hierarchy_valid
FROM dspec_specification
WHERE is_current = TRUE
ORDER BY sequence_number;
