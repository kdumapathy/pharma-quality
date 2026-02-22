-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Populate L2.2 Dimensions & Facts
-- MAGIC Transforms L2.1 source conform data into the L2.2 star schema.
-- MAGIC
-- MAGIC **Dimensions populated:**
-- MAGIC - `dim_product`, `dim_material`, `dim_test_method`, `dim_site`, `dim_market` (from LIMS + PDF)
-- MAGIC - `dim_specification`, `dim_specification_item` (from LIMS + PDF)
-- MAGIC
-- MAGIC **Fact populated:**
-- MAGIC - `fact_specification_limit` (merged from LIMS limits + process recipe limits + PDF limits)

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
    SELECT DISTINCT product_key, FIRST_VALUE(product_id) OVER (PARTITION BY product_key ORDER BY src_priority) AS product_id,
        FIRST_VALUE(product_name) OVER (PARTITION BY product_key ORDER BY src_priority) AS product_name,
        CAST(NULL AS STRING) AS product_family, CAST(NULL AS STRING) AS brand_name,
        FIRST_VALUE(dosage_form) OVER (PARTITION BY product_key ORDER BY src_priority) AS dosage_form,
        CAST(NULL AS STRING) AS route_of_administration, CAST(NULL AS STRING) AS therapeutic_area,
        FIRST_VALUE(strength) OVER (PARTITION BY product_key ORDER BY src_priority) AS strength,
        TRUE AS is_active,
        FIRST_VALUE(effective_from) OVER (PARTITION BY product_key ORDER BY src_priority) AS effective_from,
        CAST(NULL AS DATE) AS effective_to, CURRENT_TIMESTAMP() AS load_timestamp
    FROM (
        SELECT HASH(s.product_id_lims) AS product_key, s.product_id_lims AS product_id,
            s.product_name, s.dosage_form, s.strength, s.effective_start_date AS effective_from, 1 AS src_priority
        FROM l2_1_lims.src_lims_specification s WHERE s.is_current = TRUE AND s.product_id_lims IS NOT NULL
        UNION ALL
        SELECT HASH(p.product_id_pdf) AS product_key, p.product_id_pdf AS product_id,
            p.product_name, CAST(NULL AS STRING) AS dosage_form, CAST(NULL AS STRING) AS strength, p.effective_date AS effective_from, 2 AS src_priority
        FROM l2_1_lims.src_pdf_specification p WHERE p.is_current = TRUE AND p.product_id_pdf IS NOT NULL
    )
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
    SELECT DISTINCT material_key,
        FIRST_VALUE(material_id) OVER (PARTITION BY material_key ORDER BY src_priority) AS material_id,
        FIRST_VALUE(material_name) OVER (PARTITION BY material_key ORDER BY src_priority) AS material_name,
        CAST(NULL AS STRING) AS material_type, CAST(NULL AS STRING) AS cas_number,
        CAST(NULL AS STRING) AS inn_name, CAST(NULL AS STRING) AS compendial_name, CAST(NULL AS STRING) AS grade,
        TRUE AS is_active,
        FIRST_VALUE(effective_from) OVER (PARTITION BY material_key ORDER BY src_priority) AS effective_from,
        CAST(NULL AS DATE) AS effective_to, CURRENT_TIMESTAMP() AS load_timestamp
    FROM (
        SELECT HASH(s.material_id_lims) AS material_key, s.material_id_lims AS material_id,
            s.material_name, s.effective_start_date AS effective_from, 1 AS src_priority
        FROM l2_1_lims.src_lims_specification s WHERE s.is_current = TRUE AND s.material_id_lims IS NOT NULL
        UNION ALL
        SELECT HASH(p.material_id_pdf) AS material_key, p.material_id_pdf AS material_id,
            p.material_name, p.effective_date AS effective_from, 2 AS src_priority
        FROM l2_1_lims.src_pdf_specification p WHERE p.is_current = TRUE AND p.material_id_pdf IS NOT NULL
    )
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
    -- From LIMS
    SELECT
        HASH(s.source_specification_id)     AS spec_key,
        s.source_specification_id,
        s.spec_number, s.spec_version, s.spec_title, s.spec_type_code, s.spec_type_name,
        HASH(s.product_id_lims) AS product_key, HASH(s.material_id_lims) AS material_key,
        HASH(s.site_id_lims) AS site_key, HASH(s.market_region) AS market_key,
        s.status_code, s.status_name, s.stage_code, s.dosage_form, s.strength,
        s.compendia_reference, s.ctd_section,
        CAST(DATE_FORMAT(s.effective_start_date, 'yyyyMMdd') AS INT) AS effective_start_date_key,
        CAST(DATE_FORMAT(s.effective_end_date, 'yyyyMMdd') AS INT)   AS effective_end_date_key,
        CAST(DATE_FORMAT(s.approval_date, 'yyyyMMdd') AS INT)       AS approval_date_key,
        s.approved_by,
        CAST(NULL AS BIGINT) AS supersedes_spec_key,
        TRUE AS is_current, CURRENT_TIMESTAMP() AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to, CURRENT_TIMESTAMP() AS load_timestamp
    FROM l2_1_lims.src_lims_specification s
    WHERE s.is_current = TRUE

    UNION ALL

    -- From PDF (distinct specs not already in LIMS)
    SELECT
        HASH(p.spec_number)                 AS spec_key,
        CONCAT('PDF:', p.source_document_id) AS source_specification_id,
        p.spec_number, p.spec_version, p.spec_title, p.spec_type_code,
        CAST(NULL AS STRING) AS spec_type_name,
        HASH(p.product_id_pdf) AS product_key, HASH(p.material_id_pdf) AS material_key,
        CAST(NULL AS BIGINT) AS site_key, HASH(p.market_region) AS market_key,
        'APP' AS status_code, 'Approved' AS status_name,
        'COM' AS stage_code, CAST(NULL AS STRING) AS dosage_form, CAST(NULL AS STRING) AS strength,
        p.compendia_reference, p.ctd_section,
        CAST(DATE_FORMAT(p.effective_date, 'yyyyMMdd') AS INT)  AS effective_start_date_key,
        CAST(NULL AS INT) AS effective_end_date_key,
        CAST(DATE_FORMAT(p.approval_date, 'yyyyMMdd') AS INT)   AS approval_date_key,
        p.approved_by,
        CAST(NULL AS BIGINT) AS supersedes_spec_key,
        TRUE AS is_current, CURRENT_TIMESTAMP() AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to, CURRENT_TIMESTAMP() AS load_timestamp
    FROM l2_1_lims.src_pdf_specification p
    WHERE p.is_current = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.spec_number ORDER BY p.source_ingestion_timestamp DESC) = 1
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
    -- From LIMS
    SELECT
        HASH(i.source_spec_item_id)         AS spec_item_key,
        i.source_spec_item_id               AS source_spec_item_id,
        HASH(i.source_specification_id)     AS spec_key,
        HASH(i.test_method_id_lims)         AS test_method_key,
        i.test_code, i.test_name, i.analyte_code, i.parameter_name,
        i.test_category_code, i.test_category_name, i.test_subcategory,
        u.uom_key, i.criticality_code, i.sequence_number, i.reporting_type,
        i.result_precision, i.is_required, i.compendia_test_ref, i.stage_applicability,
        TRUE AS is_current, CURRENT_TIMESTAMP() AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to, CURRENT_TIMESTAMP() AS load_timestamp
    FROM l2_1_lims.src_lims_spec_item i
    LEFT JOIN dim_uom u ON u.uom_code = i.uom_code
    WHERE i.is_current = TRUE

    UNION ALL

    -- From PDF (distinct test items per spec not already from LIMS)
    SELECT
        HASH(CONCAT(p.spec_number, ':', p.test_code)) AS spec_item_key,
        CONCAT('PDF:', p.source_document_id, ':', p.test_code) AS source_spec_item_id,
        HASH(p.spec_number)                 AS spec_key,
        CAST(NULL AS BIGINT)                AS test_method_key,
        p.test_code, p.test_name, CAST(NULL AS STRING) AS analyte_code,
        p.test_name AS parameter_name,
        p.test_category_code, CAST(NULL AS STRING) AS test_category_name, CAST(NULL AS STRING) AS test_subcategory,
        u.uom_key, p.criticality_code, CAST(NULL AS INT) AS sequence_number,
        CASE WHEN p.limit_text IS NOT NULL AND p.lower_limit_value IS NULL AND p.upper_limit_value IS NULL
            THEN 'TEXT' ELSE 'NUMERIC' END AS reporting_type,
        CAST(NULL AS INT) AS result_precision, CAST(NULL AS BOOLEAN) AS is_required,
        p.test_method_reference AS compendia_test_ref, p.stage_code AS stage_applicability,
        TRUE AS is_current, CURRENT_TIMESTAMP() AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to, CURRENT_TIMESTAMP() AS load_timestamp
    FROM l2_1_lims.src_pdf_specification p
    LEFT JOIN dim_uom u ON u.uom_code = p.uom_code
    WHERE p.is_current = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.spec_number, p.test_code ORDER BY p.source_ingestion_timestamp DESC) = 1
) AS src
ON tgt.spec_item_key = src.spec_item_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## fact_specification_limit
-- MAGIC Merges limits from **all three** sources: LIMS, Process Recipe, and PDF.

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

    UNION ALL

    -- PDF/SOP document limits (AC, NOR, PAR from transcribed specifications)
    SELECT
        HASH(p.source_row_key, 'PDF')       AS spec_limit_key,
        HASH(p.spec_number)                  AS spec_key,
        HASH(CONCAT(p.spec_number, ':', p.test_code)) AS spec_item_key,
        lt.limit_type_key,
        u.uom_key,
        CAST(DATE_FORMAT(p.effective_date, 'yyyyMMdd') AS INT) AS effective_start_date_key,
        CAST(NULL AS INT)                   AS effective_end_date_key,
        p.lower_limit_value,
        p.upper_limit_value,
        p.target_value,
        COALESCE(p.upper_limit_value - p.lower_limit_value, CAST(NULL AS DECIMAL(18,6))) AS limit_range_width,
        CASE WHEN p.lower_limit_value IS NOT NULL THEN 'GTE' ELSE 'NONE' END AS lower_limit_operator,
        CASE WHEN p.upper_limit_value IS NOT NULL THEN 'LTE' ELSE 'NONE' END AS upper_limit_operator,
        p.limit_text,
        p.limit_expression                  AS limit_description,
        CAST(NULL AS STRING)                AS limit_basis,
        COALESCE(p.stage_code, 'RELEASE')   AS stage_code,
        CAST(NULL AS STRING)                AS stability_time_point,
        p.stability_condition,
        CAST(NULL AS STRING)                AS calculation_method,
        CAST(NULL AS INT)                   AS sample_size,
        CAST(NULL AS INT)                   AS last_calculated_date_key,
        TRUE                                AS is_in_filing,
        p.regulatory_basis,
        p.source_row_key                    AS source_limit_id,
        TRUE                                AS is_current,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_lims.src_pdf_specification p
    JOIN dim_limit_type lt ON lt.limit_type_code = p.limit_type_code
    LEFT JOIN dim_uom u ON u.uom_code = p.uom_code
    WHERE p.is_current = TRUE
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
