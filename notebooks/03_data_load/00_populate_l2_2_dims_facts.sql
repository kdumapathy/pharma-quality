-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Populate L2.2 Dimensions & Facts
-- MAGIC Transforms L2.1 source conform data into the L2.2 star schema.
-- MAGIC
-- MAGIC **Dimensions populated:**
-- MAGIC - `dim_product`, `dim_material`, `dim_test_method`, `dim_site`, `dim_market` (from L2.1 LIMS)
-- MAGIC - `dim_specification`, `dim_specification_item` (from L2.1 LIMS)
-- MAGIC
-- MAGIC **Fact populated:**
-- MAGIC - `fact_specification_limit` (merged from L2.1 LIMS limits + process recipe limits)

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

USE SCHEMA l2_2_spec_unified;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_product

-- COMMAND ----------

MERGE INTO dim_product AS tgt
USING (
    SELECT DISTINCT
        HASH(s.product_id_lims)             AS product_key,
        s.product_id_lims                   AS product_id,
        s.product_name,
        CAST(NULL AS STRING)                AS product_family,
        CAST(NULL AS STRING)                AS brand_name,
        s.dosage_form,
        CAST(NULL AS STRING)                AS route_of_administration,
        CAST(NULL AS STRING)                AS therapeutic_area,
        s.strength,
        TRUE                                AS is_active,
        s.effective_start_date              AS effective_from,
        CAST(NULL AS DATE)                  AS effective_to,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_lims.src_lims_specification s
    WHERE s.is_current = TRUE AND s.product_id_lims IS NOT NULL
) AS src
ON tgt.product_key = src.product_key
WHEN MATCHED THEN UPDATE SET
    product_name = src.product_name, dosage_form = src.dosage_form,
    strength = src.strength, load_timestamp = src.load_timestamp
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_material

-- COMMAND ----------

MERGE INTO dim_material AS tgt
USING (
    SELECT DISTINCT
        HASH(s.material_id_lims)            AS material_key,
        s.material_id_lims                  AS material_id,
        s.material_name,
        CAST(NULL AS STRING)                AS material_type,
        CAST(NULL AS STRING)                AS cas_number,
        CAST(NULL AS STRING)                AS inn_name,
        CAST(NULL AS STRING)                AS compendial_name,
        CAST(NULL AS STRING)                AS grade,
        TRUE                                AS is_active,
        s.effective_start_date              AS effective_from,
        CAST(NULL AS DATE)                  AS effective_to,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_lims.src_lims_specification s
    WHERE s.is_current = TRUE AND s.material_id_lims IS NOT NULL
) AS src
ON tgt.material_key = src.material_key
WHEN MATCHED THEN UPDATE SET
    material_name = src.material_name, load_timestamp = src.load_timestamp
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_test_method

-- COMMAND ----------

MERGE INTO dim_test_method AS tgt
USING (
    SELECT DISTINCT
        HASH(i.test_method_id_lims)         AS test_method_key,
        i.test_method_id_lims               AS test_method_id,
        COALESCE(i.compendia_test_ref, i.test_name) AS method_name,
        CAST(NULL AS STRING)                AS method_number,
        CAST(NULL AS STRING)                AS method_version,
        CAST(NULL AS STRING)                AS method_type,
        CAST(NULL AS STRING)                AS technique,
        i.compendia_test_ref                AS compendia_reference,
        CAST(NULL AS BOOLEAN)               AS is_validated,
        TRUE                                AS is_active,
        CAST(NULL AS DATE)                  AS effective_from,
        CAST(NULL AS DATE)                  AS effective_to,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_lims.src_lims_spec_item i
    WHERE i.is_current = TRUE AND i.test_method_id_lims IS NOT NULL
) AS src
ON tgt.test_method_key = src.test_method_key
WHEN MATCHED THEN UPDATE SET
    method_name = src.method_name, compendia_reference = src.compendia_reference,
    load_timestamp = src.load_timestamp
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_site

-- COMMAND ----------

MERGE INTO dim_site AS tgt
USING (
    SELECT DISTINCT
        HASH(s.site_id_lims)                AS site_key,
        s.site_id_lims                      AS site_id,
        s.site_name,
        CAST(NULL AS STRING)                AS site_type,
        CAST(NULL AS STRING)                AS country_code,
        CAST(NULL AS STRING)                AS country_name,
        s.market_region                     AS region_code,
        TRUE                                AS is_active,
        CAST(NULL AS DATE)                  AS effective_from,
        CAST(NULL AS DATE)                  AS effective_to,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_lims.src_lims_specification s
    WHERE s.is_current = TRUE AND s.site_id_lims IS NOT NULL
) AS src
ON tgt.site_key = src.site_key
WHEN MATCHED THEN UPDATE SET
    site_name = src.site_name, region_code = src.region_code,
    load_timestamp = src.load_timestamp
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_market

-- COMMAND ----------

MERGE INTO dim_market AS tgt
USING (
    SELECT DISTINCT
        HASH(s.market_region)               AS market_key,
        s.market_region                     AS market_code,
        s.market_region                     AS market_name,
        s.market_region                     AS region_code,
        s.market_region                     AS region_name,
        CAST(NULL AS STRING)                AS regulatory_authority,
        TRUE                                AS is_active,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_lims.src_lims_specification s
    WHERE s.is_current = TRUE AND s.market_region IS NOT NULL
) AS src
ON tgt.market_key = src.market_key
WHEN MATCHED THEN UPDATE SET
    market_name = src.market_name, load_timestamp = src.load_timestamp
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_specification

-- COMMAND ----------

MERGE INTO dim_specification AS tgt
USING (
    SELECT
        HASH(s.source_specification_id)     AS spec_key,
        s.source_specification_id,
        s.spec_number,
        s.spec_version,
        s.spec_title,
        s.spec_type_code,
        s.spec_type_name,
        HASH(s.product_id_lims)             AS product_key,
        HASH(s.material_id_lims)            AS material_key,
        HASH(s.site_id_lims)                AS site_key,
        HASH(s.market_region)               AS market_key,
        s.status_code,
        s.status_name,
        s.stage_code,
        s.dosage_form,
        s.strength,
        s.compendia_reference,
        s.ctd_section,
        CAST(DATE_FORMAT(s.effective_start_date, 'yyyyMMdd') AS INT) AS effective_start_date_key,
        CAST(DATE_FORMAT(s.effective_end_date, 'yyyyMMdd') AS INT)   AS effective_end_date_key,
        CAST(DATE_FORMAT(s.approval_date, 'yyyyMMdd') AS INT)       AS approval_date_key,
        s.approved_by,
        CAST(NULL AS BIGINT)                AS supersedes_spec_key,
        TRUE                                AS is_current,
        CURRENT_TIMESTAMP()                 AS valid_from,
        CAST(NULL AS TIMESTAMP)             AS valid_to,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_lims.src_lims_specification s
    WHERE s.is_current = TRUE
) AS src
ON tgt.spec_key = src.spec_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_specification_item

-- COMMAND ----------

MERGE INTO dim_specification_item AS tgt
USING (
    SELECT
        HASH(i.source_spec_item_id)         AS spec_item_key,
        i.source_spec_item_id               AS source_spec_item_id,
        HASH(i.source_specification_id)     AS spec_key,
        HASH(i.test_method_id_lims)         AS test_method_key,
        i.test_code,
        i.test_name,
        i.analyte_code,
        i.parameter_name,
        i.test_category_code,
        i.test_category_name,
        i.test_subcategory,
        u.uom_key,
        i.criticality_code,
        i.sequence_number,
        i.reporting_type,
        i.result_precision,
        i.is_required,
        i.compendia_test_ref,
        i.stage_applicability,
        TRUE                                AS is_current,
        CURRENT_TIMESTAMP()                 AS valid_from,
        CAST(NULL AS TIMESTAMP)             AS valid_to,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_lims.src_lims_spec_item i
    LEFT JOIN dim_uom u ON u.uom_code = i.uom_code
    WHERE i.is_current = TRUE
) AS src
ON tgt.spec_item_key = src.spec_item_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## fact_specification_limit
-- MAGIC Merges limits from **both** LIMS and Process Recipe sources.

-- COMMAND ----------

MERGE INTO fact_specification_limit AS tgt
USING (
    -- LIMS limits (primarily AC, but can include any type)
    SELECT
        HASH(l.source_limit_id, 'LIMS')    AS spec_limit_key,
        HASH(l.source_specification_id)     AS spec_key,
        HASH(l.source_spec_item_id)         AS spec_item_key,
        lt.limit_type_key,
        u.uom_key,
        CAST(DATE_FORMAT(l.effective_start_date, 'yyyyMMdd') AS INT) AS effective_start_date_key,
        CAST(DATE_FORMAT(l.effective_end_date, 'yyyyMMdd') AS INT)   AS effective_end_date_key,
        l.lower_limit_value,
        l.upper_limit_value,
        l.target_value,
        COALESCE(l.upper_limit_value - l.lower_limit_value, CAST(NULL AS DECIMAL(18,6))) AS limit_range_width,
        l.lower_limit_operator,
        l.upper_limit_operator,
        l.limit_text,
        l.limit_description,
        l.limit_basis,
        l.stage_code,
        l.stability_time_point,
        l.stability_condition,
        l.calculation_method,
        l.sample_size,
        CAST(DATE_FORMAT(l.last_calculated_date, 'yyyyMMdd') AS INT) AS last_calculated_date_key,
        l.is_in_filing,
        l.regulatory_basis,
        l.source_limit_id,
        TRUE                                AS is_current,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_lims.src_lims_spec_limit l
    JOIN dim_limit_type lt ON lt.limit_type_code = l.limit_type_code
    LEFT JOIN dim_uom u ON u.uom_code = l.uom_code
    WHERE l.is_current = TRUE

    UNION ALL

    -- Process Recipe limits (NOR, PAR, Target, Alert, Action)
    SELECT
        HASH(r.source_recipe_id, r.limit_type_code, COALESCE(r.source_spec_item_id, '')) AS spec_limit_key,
        HASH(r.source_specification_id)     AS spec_key,
        HASH(r.source_spec_item_id)         AS spec_item_key,
        lt.limit_type_key,
        u.uom_key,
        CAST(DATE_FORMAT(r.effective_start_date, 'yyyyMMdd') AS INT) AS effective_start_date_key,
        CAST(DATE_FORMAT(r.effective_end_date, 'yyyyMMdd') AS INT)   AS effective_end_date_key,
        r.lower_limit_value,
        r.upper_limit_value,
        r.target_value,
        COALESCE(r.upper_limit_value - r.lower_limit_value, CAST(NULL AS DECIMAL(18,6))) AS limit_range_width,
        CASE WHEN r.lower_limit_value IS NOT NULL THEN 'GTE' ELSE 'NONE' END AS lower_limit_operator,
        CASE WHEN r.upper_limit_value IS NOT NULL THEN 'LTE' ELSE 'NONE' END AS upper_limit_operator,
        CAST(NULL AS STRING)                AS limit_text,
        CASE
            WHEN r.lower_limit_value IS NOT NULL AND r.upper_limit_value IS NOT NULL
                THEN CONCAT(CAST(r.lower_limit_value AS STRING), ' - ', CAST(r.upper_limit_value AS STRING), ' ', COALESCE(r.uom_code, ''))
            WHEN r.lower_limit_value IS NOT NULL
                THEN CONCAT('NLT ', CAST(r.lower_limit_value AS STRING), ' ', COALESCE(r.uom_code, ''))
            WHEN r.upper_limit_value IS NOT NULL
                THEN CONCAT('NMT ', CAST(r.upper_limit_value AS STRING), ' ', COALESCE(r.uom_code, ''))
            ELSE NULL
        END                                 AS limit_description,
        r.limit_basis,
        COALESCE(r.stage_code, 'RELEASE')   AS stage_code,
        CAST(NULL AS STRING)                AS stability_time_point,
        CAST(NULL AS STRING)                AS stability_condition,
        r.calculation_method,
        r.sample_size,
        CAST(DATE_FORMAT(r.last_calculated_date, 'yyyyMMdd') AS INT) AS last_calculated_date_key,
        FALSE                               AS is_in_filing,
        CAST(NULL AS STRING)                AS regulatory_basis,
        r.source_recipe_id                  AS source_limit_id,
        TRUE                                AS is_current,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_lims.src_process_recipe r
    JOIN dim_limit_type lt ON lt.limit_type_code = r.limit_type_code
    LEFT JOIN dim_uom u ON u.uom_code = r.uom_code
    WHERE r.is_current = TRUE
) AS src
ON tgt.spec_limit_key = src.spec_limit_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify L2.2 Dims & Facts

-- COMMAND ----------

SELECT 'dim_product' AS table_name, COUNT(*) AS rows FROM dim_product
UNION ALL SELECT 'dim_material', COUNT(*) FROM dim_material
UNION ALL SELECT 'dim_test_method', COUNT(*) FROM dim_test_method
UNION ALL SELECT 'dim_site', COUNT(*) FROM dim_site
UNION ALL SELECT 'dim_market', COUNT(*) FROM dim_market
UNION ALL SELECT 'dim_specification', COUNT(*) FROM dim_specification
UNION ALL SELECT 'dim_specification_item', COUNT(*) FROM dim_specification_item
UNION ALL SELECT 'fact_specification_limit', COUNT(*) FROM fact_specification_limit;
