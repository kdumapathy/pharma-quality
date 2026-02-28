-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Populate L2.2 Dimensions & Facts
-- MAGIC Transforms L2.1 source conform data into the L2.2 star schema.
-- MAGIC Aligned with PQ/CMC (Pharmaceutical Quality / Chemistry, Manufacturing, Controls) field naming.
-- MAGIC
-- MAGIC **Dimensions populated:**
-- MAGIC - `dim_product`, `dim_material`, `dim_test_method`, `dim_site`, `dim_market` (from LIMS + PDF)
-- MAGIC - `dim_specification`, `dim_specification_item` (from LIMS + PDF)
-- MAGIC - `dim_batch`, `dim_instrument`, `dim_laboratory` (from Vendor Analytical Results)
-- MAGIC
-- MAGIC **Fact populated:**
-- MAGIC - `fact_specification_limit` (merged from LIMS limits + process recipe limits + PDF limits)
-- MAGIC - `fact_analytical_result` (from vendor analytical results)

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

USE SCHEMA l2_2_unified_model;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_product

-- COMMAND ----------

-- DBTITLE 1,dim_product MERGE (PQ/CMC enriched)
MERGE INTO dim_product AS tgt
USING (
    SELECT DISTINCT
        FIRST_VALUE(product_id) OVER (PARTITION BY product_key ORDER BY src_priority) AS product_id,
        FIRST_VALUE(product_name) OVER (PARTITION BY product_key ORDER BY src_priority) AS product_name,
        HASH(FIRST_VALUE(product_id) OVER (PARTITION BY product_key ORDER BY src_priority)) AS product_key,
        CAST(NULL AS STRING) AS inn_name,
        CAST(NULL AS STRING) AS brand_name,
        CAST(NULL AS STRING) AS product_family,
        CAST(NULL AS STRING) AS dosage_form_code,
        FIRST_VALUE(dosage_form) OVER (PARTITION BY product_key ORDER BY src_priority) AS dosage_form_name,
        CAST(NULL AS STRING) AS route_of_administration,
        FIRST_VALUE(strength) OVER (PARTITION BY product_key ORDER BY src_priority) AS strength,
        CAST(NULL AS DECIMAL(12, 4)) AS strength_value,
        CAST(NULL AS STRING) AS strength_uom,
        CAST(NULL AS STRING) AS therapeutic_area,
        CAST(NULL AS STRING) AS nda_number,
        CAST(NULL AS INT) AS shelf_life_months,
        CAST(NULL AS STRING) AS storage_conditions,
        CAST(NULL AS STRING) AS container_closure_system,
        CURRENT_TIMESTAMP() AS load_timestamp,
        TRUE AS is_active
    FROM (
        SELECT HASH(s.product_id_lims) AS product_key, s.product_id_lims AS product_id,
            s.product_name, s.dosage_form, s.strength, s.effective_start_date AS effective_from, 1 AS src_priority
        FROM l2_1_scl.src_lims_specification s WHERE s.is_current = TRUE AND s.product_id_lims IS NOT NULL
        UNION ALL
        SELECT HASH(p.product_id_pdf) AS product_key, p.product_id_pdf AS product_id,
            p.product_name, CAST(NULL AS STRING) AS dosage_form, CAST(NULL AS STRING) AS strength, p.effective_date AS effective_from, 2 AS src_priority
        FROM l2_1_scl.src_pdf_specification p WHERE p.is_current = TRUE AND p.product_id_pdf IS NOT NULL
    )
) AS src
ON tgt.product_id = src.product_id
WHEN MATCHED THEN UPDATE SET
    product_name = src.product_name,
    dosage_form_name = src.dosage_form_name,
    strength = src.strength,
    load_timestamp = src.load_timestamp
WHEN NOT MATCHED THEN INSERT (
    product_key, product_id, product_name, inn_name, brand_name, product_family, dosage_form_code, dosage_form_name,
    route_of_administration, strength, strength_value, strength_uom, therapeutic_area, nda_number,
    shelf_life_months, storage_conditions, container_closure_system, is_active, load_timestamp
)
VALUES (
    src.product_key, src.product_id, src.product_name, src.inn_name, src.brand_name, src.product_family, src.dosage_form_code, src.dosage_form_name,
    src.route_of_administration, src.strength, src.strength_value, src.strength_uom, src.therapeutic_area, src.nda_number,
    src.shelf_life_months, src.storage_conditions, src.container_closure_system, src.is_active, src.load_timestamp
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_material

-- COMMAND ----------

-- DBTITLE 1,dim_material MERGE (CMC enriched)
MERGE INTO dim_material AS tgt
USING (
    SELECT DISTINCT
        FIRST_VALUE(material_id) OVER (PARTITION BY material_key ORDER BY src_priority) AS material_id,
        FIRST_VALUE(material_name) OVER (PARTITION BY material_key ORDER BY src_priority) AS material_name,
        HASH(FIRST_VALUE(material_id) OVER (PARTITION BY material_key ORDER BY src_priority)) AS material_key,
        CAST(NULL AS STRING) AS material_type_code,
        CAST(NULL AS STRING) AS material_type_name,
        CAST(NULL AS STRING) AS cas_number,
        CAST(NULL AS STRING) AS molecular_formula,
        CAST(NULL AS DECIMAL(10, 4)) AS molecular_weight,
        CAST(NULL AS STRING) AS inn_name,
        CAST(NULL AS STRING) AS compendial_name,
        CAST(NULL AS STRING) AS pharmacopoeia_grade,
        CAST(NULL AS STRING) AS grade,
        CAST(NULL AS STRING) AS supplier_name,
        CAST(NULL AS INT) AS retest_period_months,
        CAST(NULL AS STRING) AS storage_requirements,
        TRUE AS is_active,
        CURRENT_TIMESTAMP() AS load_timestamp
    FROM (
        SELECT HASH(s.material_id_lims) AS material_key, s.material_id_lims AS material_id,
            s.material_name, s.effective_start_date AS effective_from, 1 AS src_priority
        FROM l2_1_scl.src_lims_specification s WHERE s.is_current = TRUE AND s.material_id_lims IS NOT NULL
        UNION ALL
        SELECT HASH(p.material_id_pdf) AS material_key, p.material_id_pdf AS material_id,
            p.material_name, p.effective_date AS effective_from, 2 AS src_priority
        FROM l2_1_scl.src_pdf_specification p WHERE p.is_current = TRUE AND p.material_id_pdf IS NOT NULL
    )
) AS src
ON tgt.material_id = src.material_id
WHEN MATCHED THEN UPDATE SET
    material_name = src.material_name, load_timestamp = src.load_timestamp
WHEN NOT MATCHED THEN INSERT (
    material_key, material_id, material_name, material_type_code, material_type_name, cas_number,
    molecular_formula, molecular_weight, inn_name, compendial_name, pharmacopoeia_grade, grade,
    supplier_name, retest_period_months, storage_requirements, is_active, load_timestamp
)
VALUES (
    src.material_key, src.material_id, src.material_name, src.material_type_code, src.material_type_name, src.cas_number,
    src.molecular_formula, src.molecular_weight, src.inn_name, src.compendial_name, src.pharmacopoeia_grade, src.grade,
    src.supplier_name, src.retest_period_months, src.storage_requirements, src.is_active, src.load_timestamp
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_test_method

-- COMMAND ----------

-- DBTITLE 1,dim_test_method MERGE (ICH Q2 enriched)
MERGE INTO dim_test_method AS tgt
USING (
    SELECT
        i.test_method_id_lims               AS test_method_id,
        HASH(i.test_method_id_lims)         AS test_method_key,
        COALESCE(i.compendia_test_ref, i.test_name) AS test_method_name,
        CAST(NULL AS STRING)                AS test_method_number,
        CAST(NULL AS STRING)                AS test_method_version,
        CAST(NULL AS STRING)                AS method_type,
        CAST(NULL AS STRING)                AS analytical_technique,
        i.compendia_test_ref                AS compendia_reference,
        CAST(NULL AS DECIMAL(18, 6))        AS detection_limit,
        CAST(NULL AS DECIMAL(18, 6))        AS quantitation_limit,
        CAST(NULL AS STRING)                AS validation_status,
        CAST(NULL AS DATE)                  AS validation_date,
        TRUE                                AS is_validated,
        TRUE                                AS is_active,
        CURRENT_DATE()                      AS effective_from,
        NULL                                AS effective_to,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_scl.src_lims_spec_item i
    WHERE i.is_current = TRUE AND i.test_method_id_lims IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY i.test_method_id_lims ORDER BY i.load_timestamp DESC) = 1
) AS src
ON tgt.test_method_id = src.test_method_id
WHEN MATCHED THEN UPDATE SET
    test_method_name = src.test_method_name,
    load_timestamp = src.load_timestamp
WHEN NOT MATCHED THEN INSERT (
    test_method_key, test_method_id, test_method_name, test_method_number, test_method_version,
    method_type, analytical_technique, compendia_reference, detection_limit, quantitation_limit,
    validation_status, validation_date, is_validated, is_active, effective_from, effective_to, load_timestamp
)
VALUES (
    src.test_method_key, src.test_method_id, src.test_method_name, src.test_method_number, src.test_method_version,
    src.method_type, src.analytical_technique, src.compendia_reference, src.detection_limit, src.quantitation_limit,
    src.validation_status, src.validation_date, src.is_validated, src.is_active, src.effective_from, src.effective_to, src.load_timestamp
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_site

-- COMMAND ----------

-- DBTITLE 1,dim_site MERGE (GMP enriched)
MERGE INTO dim_site AS tgt
USING (
    SELECT
        s.site_id_lims                      AS site_id,
        HASH(s.site_id_lims)                AS site_key,
        s.site_id_lims                      AS site_code,
        s.site_name,
        CAST(NULL AS STRING)                AS site_type,
        CAST(NULL AS STRING)                AS address_line,
        CAST(NULL AS STRING)                AS city,
        CAST(NULL AS STRING)                AS state_province,
        CAST(NULL AS STRING)                AS country_code,
        CAST(NULL AS STRING)                AS country_name,
        CAST(NULL AS STRING)                AS regulatory_region,
        CAST(NULL AS STRING)                AS gmp_status,
        CAST(NULL AS STRING)                AS gmp_certificate_number,
        CAST(NULL AS STRING)                AS fda_fei_number,
        CAST(NULL AS DATE)                  AS last_inspection_date,
        CAST(NULL AS STRING)                AS last_inspection_outcome,
        TRUE                                AS is_active,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_scl.src_lims_specification s
    WHERE s.is_current = TRUE AND s.site_id_lims IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.site_id_lims ORDER BY s.load_timestamp DESC) = 1
) AS src
ON tgt.site_id = src.site_id
WHEN MATCHED THEN UPDATE SET
    site_name = src.site_name,
    load_timestamp = src.load_timestamp
WHEN NOT MATCHED THEN INSERT (
    site_key, site_id, site_code, site_name, site_type, address_line, city, state_province,
    country_code, country_name, regulatory_region, gmp_status, gmp_certificate_number,
    fda_fei_number, last_inspection_date, last_inspection_outcome, is_active, load_timestamp
)
VALUES (
    src.site_key, src.site_id, src.site_code, src.site_name, src.site_type, src.address_line, src.city, src.state_province,
    src.country_code, src.country_name, src.regulatory_region, src.gmp_status, src.gmp_certificate_number,
    src.fda_fei_number, src.last_inspection_date, src.last_inspection_outcome, src.is_active, src.load_timestamp
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_market

-- COMMAND ----------

-- DBTITLE 1,dim_market MERGE (MA enriched)
MERGE INTO dim_market AS tgt
USING (
    SELECT DISTINCT
        s.market_region                     AS market_code,
        s.market_region                     AS market_name,
        CAST(NULL AS STRING)                AS country_code,
        CAST(NULL AS STRING)                AS country_name,
        s.market_region                     AS region_code,
        s.market_region                     AS region_name,
        CAST(NULL AS STRING)                AS regulatory_authority,
        CAST(NULL AS STRING)                AS primary_pharmacopoeia,
        CAST(NULL AS STRING)                AS market_status,
        CAST(NULL AS STRING)                AS marketing_auth_number,
        TRUE                                AS is_active,
        CURRENT_TIMESTAMP()                 AS load_timestamp,
        HASH(s.market_region)               AS market_key
    FROM l2_1_scl.src_lims_specification s
    WHERE s.is_current = TRUE AND s.market_region IS NOT NULL
) AS src
ON tgt.market_code = src.market_code
WHEN MATCHED THEN UPDATE SET
    market_name = src.market_name,
    region_code = src.region_code,
    region_name = src.region_name,
    regulatory_authority = src.regulatory_authority,
    is_active = src.is_active,
    load_timestamp = src.load_timestamp
WHEN NOT MATCHED THEN INSERT (
    market_key, market_code, market_name, country_code, country_name, region_code, region_name,
    regulatory_authority, primary_pharmacopoeia, market_status, marketing_auth_number, is_active, load_timestamp
)
VALUES (
    src.market_key, src.market_code, src.market_name, src.country_code, src.country_name, src.region_code, src.region_name,
    src.regulatory_authority, src.primary_pharmacopoeia, src.market_status, src.marketing_auth_number, src.is_active, src.load_timestamp
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_specification

-- COMMAND ----------

-- DBTITLE 1,dim_specification MERGE (PQ/CMC enriched)
MERGE INTO dim_specification AS tgt
USING (
    WITH src_specs AS (
        -- From LIMS
        SELECT
            s.source_specification_id           AS source_specification_id,
            s.spec_number, s.spec_version, s.spec_title, s.spec_type_code, s.spec_type_name,
            dp.product_key,
            dm.material_key,
            ds.site_key,
            dmk.market_key,
            s.ctd_section,
            s.stage_code,
            CAST(NULL AS STRING) AS stage_name,
            s.status_code, s.status_name,
            s.compendia_reference,
            s.effective_start_date AS effective_start_date,
            s.effective_end_date AS effective_end_date,
            CAST(DATE_FORMAT(s.effective_start_date, 'yyyyMMdd') AS INT) AS effective_start_date_key,
            CAST(DATE_FORMAT(s.effective_end_date, 'yyyyMMdd') AS INT) AS effective_end_date_key,
            s.approval_date AS approval_date,
            CAST(DATE_FORMAT(s.approval_date, 'yyyyMMdd') AS INT) AS approval_date_key,
            CAST(NULL AS STRING) AS approver_name,
            s.approved_by AS approved_by,
            CAST(NULL AS STRING) AS supersedes_spec_id,
            CAST(NULL AS BIGINT) AS supersedes_spec_key,
            CURRENT_TIMESTAMP() AS load_timestamp,
            TRUE AS is_current, CURRENT_TIMESTAMP() AS valid_from,
            CAST(NULL AS TIMESTAMP) AS valid_to
        FROM l2_1_scl.src_lims_specification s
        LEFT JOIN dim_product dp ON dp.product_id = s.product_id_lims
        LEFT JOIN dim_material dm ON dm.material_id = s.material_id_lims
        LEFT JOIN dim_site ds ON ds.site_id = s.site_id_lims
        LEFT JOIN dim_market dmk ON dmk.market_code = s.market_region
        WHERE s.is_current = TRUE

        UNION ALL

        -- From PDF (distinct specs not already in LIMS)
        SELECT
            CONCAT('PDF:', p.source_document_id) AS source_specification_id,
            p.spec_number, p.spec_version, p.spec_title, p.spec_type_code,
            CAST(NULL AS STRING) AS spec_type_name,
            dp.product_key,
            dm.material_key,
            NULL AS site_key,
            dmk.market_key,
            p.ctd_section,
            p.stage_code,
            CAST(NULL AS STRING) AS stage_name,
            'APP' AS status_code, 'Approved' AS status_name,
            p.compendia_reference,
            CAST(p.effective_date AS DATE) AS effective_start_date,
            CAST(NULL AS DATE) AS effective_end_date,
            CAST(DATE_FORMAT(p.effective_date, 'yyyyMMdd') AS INT) AS effective_start_date_key,
            NULL AS effective_end_date_key,
            CAST(p.approval_date AS DATE) AS approval_date,
            CAST(DATE_FORMAT(p.approval_date, 'yyyyMMdd') AS INT) AS approval_date_key,
            p.approved_by AS approver_name,
            p.approved_by AS approved_by,
            CAST(NULL AS STRING) AS supersedes_spec_id,
            CAST(NULL AS BIGINT) AS supersedes_spec_key,
            CURRENT_TIMESTAMP() AS load_timestamp,
            TRUE AS is_current, CURRENT_TIMESTAMP() AS valid_from,
            CAST(NULL AS TIMESTAMP) AS valid_to
        FROM l2_1_scl.src_pdf_specification p
        LEFT JOIN dim_product dp ON dp.product_id = p.product_id_pdf
        LEFT JOIN dim_material dm ON dm.material_id = p.material_id_pdf
        LEFT JOIN dim_market dmk ON dmk.market_code = p.market_region
        WHERE p.is_current = TRUE
        QUALIFY ROW_NUMBER() OVER (PARTITION BY p.spec_number ORDER BY p.source_ingestion_timestamp DESC) = 1
    )
    SELECT
        HASH(source_specification_id) AS spec_key,
        *
    FROM src_specs
) AS src
ON tgt.source_specification_id = src.source_specification_id
WHEN MATCHED THEN UPDATE SET
    spec_number = src.spec_number,
    spec_version = src.spec_version,
    spec_title = src.spec_title,
    spec_type_code = src.spec_type_code,
    spec_type_name = src.spec_type_name,
    product_key = src.product_key,
    material_key = src.material_key,
    site_key = src.site_key,
    market_key = src.market_key,
    ctd_section = src.ctd_section,
    stage_code = src.stage_code,
    stage_name = src.stage_name,
    status_code = src.status_code,
    status_name = src.status_name,
    compendia_reference = src.compendia_reference,
    effective_start_date = src.effective_start_date,
    effective_end_date = src.effective_end_date,
    effective_start_date_key = src.effective_start_date_key,
    effective_end_date_key = src.effective_end_date_key,
    approval_date = src.approval_date,
    approval_date_key = src.approval_date_key,
    approver_name = src.approver_name,
    approved_by = src.approved_by,
    supersedes_spec_id = src.supersedes_spec_id,
    supersedes_spec_key = src.supersedes_spec_key,
    load_timestamp = src.load_timestamp,
    is_current = src.is_current,
    valid_from = src.valid_from,
    valid_to = src.valid_to
WHEN NOT MATCHED THEN INSERT (
    spec_key, source_specification_id, spec_number, spec_version, spec_title, spec_type_code, spec_type_name,
    product_key, material_key, site_key, market_key, ctd_section, stage_code, stage_name,
    status_code, status_name, compendia_reference,
    effective_start_date, effective_end_date, effective_start_date_key, effective_end_date_key,
    approval_date, approval_date_key, approver_name, approved_by,
    supersedes_spec_id, supersedes_spec_key,
    load_timestamp, is_current, valid_from, valid_to
)
VALUES (
    src.spec_key, src.source_specification_id, src.spec_number, src.spec_version, src.spec_title, src.spec_type_code, src.spec_type_name,
    src.product_key, src.material_key, src.site_key, src.market_key, src.ctd_section, src.stage_code, src.stage_name,
    src.status_code, src.status_name, src.compendia_reference,
    src.effective_start_date, src.effective_end_date, src.effective_start_date_key, src.effective_end_date_key,
    src.approval_date, src.approval_date_key, src.approver_name, src.approved_by,
    src.supersedes_spec_id, src.supersedes_spec_key,
    src.load_timestamp, src.is_current, src.valid_from, src.valid_to
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_specification_item

-- COMMAND ----------

-- DBTITLE 1,dim_specification_item MERGE (ICH Q6A/Q8/Q9 enriched)
MERGE INTO dim_specification_item AS tgt
USING (
    WITH src_items AS (
        -- From LIMS
        SELECT
            i.source_spec_item_id               AS source_spec_item_id,
            i.source_specification_id           AS source_specification_id,
            i.test_method_id_lims               AS test_method_id,
            i.uom_code,
            i.test_code,
            COALESCE(NULLIF(TRIM(i.test_name), ''), NULLIF(TRIM(i.test_code), ''), 'UNKNOWN_TEST') AS test_name,
            i.analyte_code,
            i.test_category_code, i.test_category_name, i.test_subcategory,
            i.criticality_code                  AS criticality,
            i.sequence_number,
            i.is_required,
            CAST(NULL AS BOOLEAN)               AS is_compendial,
            CAST(NULL AS BOOLEAN)               AS is_stability_indicating,
            i.reporting_type,
            i.result_precision,
            i.compendia_test_ref,
            i.stage_applicability,
            CAST(NULL AS DECIMAL(18, 6))        AS reporting_threshold,
            CAST(NULL AS DECIMAL(18, 6))        AS identification_threshold,
            CAST(NULL AS DECIMAL(18, 6))        AS qualification_threshold,
            TRUE AS is_current,
            CURRENT_TIMESTAMP() AS valid_from,
            CAST(NULL AS TIMESTAMP) AS valid_to,
            CURRENT_TIMESTAMP() AS load_timestamp
        FROM l2_1_scl.src_lims_spec_item i
        WHERE i.is_current = TRUE

        UNION ALL

        -- From PDF (distinct test items per spec not already from LIMS)
        SELECT
            CONCAT('PDF:', p.source_document_id, ':', p.test_code) AS source_spec_item_id,
            CONCAT('PDF:', p.source_document_id) AS source_specification_id,
            CAST(NULL AS STRING) AS test_method_id,
            p.uom_code,
            p.test_code,
            COALESCE(NULLIF(TRIM(p.test_name), ''), NULLIF(TRIM(p.test_code), ''), 'UNKNOWN_TEST') AS test_name,
            CAST(NULL AS STRING) AS analyte_code,
            p.test_category_code, CAST(NULL AS STRING) AS test_category_name, CAST(NULL AS STRING) AS test_subcategory,
            CAST(NULL AS STRING) AS criticality,
            CAST(NULL AS INT) AS sequence_number,
            CAST(NULL AS BOOLEAN) AS is_required,
            CAST(NULL AS BOOLEAN) AS is_compendial,
            CAST(NULL AS BOOLEAN) AS is_stability_indicating,
            'TEXT' AS reporting_type,
            CAST(NULL AS INT) AS result_precision,
            p.test_method_reference AS compendia_test_ref,
            p.stage_code AS stage_applicability,
            CAST(NULL AS DECIMAL(18, 6)) AS reporting_threshold,
            CAST(NULL AS DECIMAL(18, 6)) AS identification_threshold,
            CAST(NULL AS DECIMAL(18, 6)) AS qualification_threshold,
            TRUE AS is_current,
            CURRENT_TIMESTAMP() AS valid_from,
            CAST(NULL AS TIMESTAMP) AS valid_to,
            CURRENT_TIMESTAMP() AS load_timestamp
        FROM l2_1_scl.src_pdf_specification p
        WHERE p.is_current = TRUE
        QUALIFY ROW_NUMBER() OVER (PARTITION BY p.spec_number, p.test_code ORDER BY p.source_ingestion_timestamp DESC) = 1
    )
    SELECT
        HASH(source_spec_item_id) AS spec_item_key,
        src.source_spec_item_id,
        ds.spec_key,
        dtm.test_method_key,
        COALESCE(du.uom_key, du_default.uom_key) AS uom_key,
        src.test_code,
        src.test_name,
        src.analyte_code,
        src.test_category_code,
        src.test_category_name,
        src.test_subcategory,
        src.criticality,
        src.sequence_number,
        src.is_required,
        src.is_compendial,
        src.is_stability_indicating,
        src.reporting_type,
        src.result_precision,
        src.compendia_test_ref,
        src.stage_applicability,
        src.reporting_threshold,
        src.identification_threshold,
        src.qualification_threshold,
        src.is_current,
        src.valid_from,
        src.valid_to,
        src.load_timestamp
    FROM src_items src
    LEFT JOIN dim_specification ds ON ds.source_specification_id = src.source_specification_id
    LEFT JOIN dim_test_method dtm ON dtm.test_method_id = src.test_method_id
    LEFT JOIN dim_uom du ON du.uom_code = src.uom_code
    LEFT JOIN dim_uom du_default ON du_default.uom_code = 'N/A'
) AS src
ON tgt.source_spec_item_id = src.source_spec_item_id
WHEN MATCHED THEN UPDATE SET
    spec_key = src.spec_key,
    test_method_key = src.test_method_key,
    uom_key = src.uom_key,
    test_code = src.test_code,
    test_name = src.test_name,
    analyte_code = src.analyte_code,
    test_category_code = src.test_category_code,
    test_category_name = src.test_category_name,
    test_subcategory = src.test_subcategory,
    criticality = src.criticality,
    sequence_number = src.sequence_number,
    is_required = src.is_required,
    is_compendial = src.is_compendial,
    is_stability_indicating = src.is_stability_indicating,
    reporting_type = src.reporting_type,
    result_precision = src.result_precision,
    compendia_test_ref = src.compendia_test_ref,
    stage_applicability = src.stage_applicability,
    reporting_threshold = src.reporting_threshold,
    identification_threshold = src.identification_threshold,
    qualification_threshold = src.qualification_threshold,
    is_current = src.is_current,
    valid_from = src.valid_from,
    valid_to = src.valid_to,
    load_timestamp = src.load_timestamp
WHEN NOT MATCHED THEN INSERT (
    spec_item_key, source_spec_item_id, spec_key, test_method_key, uom_key, test_code, test_name, analyte_code,
    test_category_code, test_category_name, test_subcategory, criticality, sequence_number, is_required,
    is_compendial, is_stability_indicating, reporting_type, result_precision, compendia_test_ref, stage_applicability,
    reporting_threshold, identification_threshold, qualification_threshold,
    is_current, valid_from, valid_to, load_timestamp
)
VALUES (
    src.spec_item_key, src.source_spec_item_id, src.spec_key, src.test_method_key, src.uom_key, src.test_code, src.test_name, src.analyte_code,
    src.test_category_code, src.test_category_name, src.test_subcategory, src.criticality, src.sequence_number, src.is_required,
    src.is_compendial, src.is_stability_indicating, src.reporting_type, src.result_precision, src.compendia_test_ref, src.stage_applicability,
    src.reporting_threshold, src.identification_threshold, src.qualification_threshold,
    src.is_current, src.valid_from, src.valid_to, src.load_timestamp
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## fact_specification_limit
-- MAGIC Merges limits from **all three** sources: LIMS, Process Recipe, and PDF.

-- COMMAND ----------

-- DBTITLE 1,fact_specification_limit MERGE (all sources)
MERGE INTO fact_specification_limit AS tgt
USING (
    WITH src_limits AS (
    -- LIMS limits (primarily AC, but can include any type)
    SELECT
        l.source_specification_id,
        l.source_spec_item_id,
        l.limit_type_code,
        l.uom_code,
        CAST(l.effective_start_date AS DATE) AS effective_date,
        CAST(l.effective_end_date AS DATE)   AS effective_end_date,
        l.lower_limit_value,
        l.upper_limit_value,
        l.target_value,
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
        CAST(l.last_calculated_date AS DATE) AS last_calculated_date,
        l.is_in_filing,
        l.regulatory_basis,
        l.source_limit_id AS source_limit_id,
        TRUE                                AS is_current,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_scl.src_lims_spec_limit l
    WHERE l.is_current = TRUE

    UNION ALL

    -- Process Recipe limits (NOR, PAR, Target, Alert, Action)
    SELECT
        r.source_specification_id,
        r.source_spec_item_id,
        r.limit_type_code,
        r.uom_code,
        CAST(r.effective_start_date AS DATE) AS effective_date,
        CAST(r.effective_end_date AS DATE)   AS effective_end_date,
        r.lower_limit_value,
        r.upper_limit_value,
        r.target_value,
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
        CAST(r.last_calculated_date AS DATE) AS last_calculated_date,
        FALSE                               AS is_in_filing,
        CAST(NULL AS STRING)                AS regulatory_basis,
        r.source_recipe_id AS source_limit_id,
        TRUE                                AS is_current,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_scl.src_process_recipe r
    WHERE r.is_current = TRUE

    UNION ALL

    -- PDF/SOP document limits (AC, NOR, PAR from transcribed specifications)
    SELECT
        CONCAT('PDF:', p.source_document_id) AS source_specification_id,
        CONCAT('PDF:', p.source_document_id, ':', p.test_code) AS source_spec_item_id,
        p.limit_type_code,
        p.uom_code,
        CAST(p.effective_date AS DATE)       AS effective_date,
        CAST(NULL AS DATE)                   AS effective_end_date,
        p.lower_limit_value,
        p.upper_limit_value,
        p.target_value,
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
        CAST(NULL AS DATE)                  AS last_calculated_date,
        TRUE                                AS is_in_filing,
        p.regulatory_basis,
        p.source_row_key AS source_limit_id,
        TRUE                                AS is_current,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_scl.src_pdf_specification p
    WHERE p.is_current = TRUE
    )
    SELECT
        HASH(ds.spec_key, dsi.spec_item_key, dlt.limit_type_key, src.stage_code, src.source_limit_id) AS spec_limit_key,
        ds.spec_key,
        dsi.spec_item_key,
        dlt.limit_type_key,
        COALESCE(du.uom_key, du_default.uom_key) AS uom_key,
        CAST(DATE_FORMAT(src.effective_date, 'yyyyMMdd') AS INT) AS effective_start_date_key,
        CAST(DATE_FORMAT(src.effective_end_date, 'yyyyMMdd') AS INT) AS effective_end_date_key,
        src.lower_limit_value,
        src.upper_limit_value,
        src.target_value,
        (src.upper_limit_value - src.lower_limit_value) AS limit_range_width,
        src.lower_limit_operator,
        src.upper_limit_operator,
        src.limit_text,
        src.limit_description,
        src.limit_basis,
        src.stage_code,
        src.stability_time_point,
        src.stability_condition,
        src.calculation_method,
        src.sample_size,
        CAST(DATE_FORMAT(src.last_calculated_date, 'yyyyMMdd') AS INT) AS last_calculated_date_key,
        src.is_in_filing,
        src.regulatory_basis,
        src.source_limit_id,
        src.is_current,
        src.load_timestamp
    FROM src_limits src
    JOIN dim_specification ds ON ds.source_specification_id = src.source_specification_id
    JOIN dim_specification_item dsi ON dsi.source_spec_item_id = src.source_spec_item_id
    JOIN dim_limit_type dlt ON dlt.limit_type_code = src.limit_type_code
    LEFT JOIN dim_uom du ON du.uom_code = src.uom_code
    LEFT JOIN dim_uom du_default ON du_default.uom_code = 'N/A'
) AS src
ON tgt.spec_key = src.spec_key
AND tgt.spec_item_key = src.spec_item_key
AND tgt.limit_type_key = src.limit_type_key
AND tgt.stage_code = src.stage_code
AND tgt.source_limit_id = src.source_limit_id
WHEN MATCHED THEN UPDATE SET
    uom_key = src.uom_key,
    effective_start_date_key = src.effective_start_date_key,
    effective_end_date_key = src.effective_end_date_key,
    lower_limit_value = src.lower_limit_value,
    upper_limit_value = src.upper_limit_value,
    target_value = src.target_value,
    limit_range_width = src.limit_range_width,
    lower_limit_operator = src.lower_limit_operator,
    upper_limit_operator = src.upper_limit_operator,
    limit_text = src.limit_text,
    limit_description = src.limit_description,
    limit_basis = src.limit_basis,
    stability_time_point = src.stability_time_point,
    stability_condition = src.stability_condition,
    calculation_method = src.calculation_method,
    sample_size = src.sample_size,
    last_calculated_date_key = src.last_calculated_date_key,
    is_in_filing = src.is_in_filing,
    regulatory_basis = src.regulatory_basis,
    is_current = src.is_current,
    load_timestamp = src.load_timestamp
WHEN NOT MATCHED THEN INSERT (
    spec_limit_key, spec_key, spec_item_key, limit_type_key, uom_key, effective_start_date_key, effective_end_date_key, lower_limit_value, upper_limit_value, target_value, limit_range_width, lower_limit_operator, upper_limit_operator, limit_text, limit_description, limit_basis, stage_code, stability_time_point, stability_condition, calculation_method, sample_size, last_calculated_date_key, is_in_filing, regulatory_basis, source_limit_id, is_current, load_timestamp
)
VALUES (
    src.spec_limit_key, src.spec_key, src.spec_item_key, src.limit_type_key, src.uom_key, src.effective_start_date_key, src.effective_end_date_key, src.lower_limit_value, src.upper_limit_value, src.target_value, src.limit_range_width, src.lower_limit_operator, src.upper_limit_operator, src.limit_text, src.limit_description, src.limit_basis, src.stage_code, src.stability_time_point, src.stability_condition, src.calculation_method, src.sample_size, src.last_calculated_date_key, src.is_in_filing, src.regulatory_basis, src.source_limit_id, src.is_current, src.load_timestamp
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_batch
-- MAGIC Manufacturing batch dimension from vendor analytical results.

-- COMMAND ----------

MERGE INTO dim_batch AS tgt
USING (
    WITH src_batch AS (
        SELECT DISTINCT
            HASH(v.batch_number) AS batch_key,
            v.batch_number,
            v.batch_system_id,
            v.product_id_vendor,
            v.site_id_vendor,
            v.manufacturing_date,
            v.expiry_date,
            v.batch_size,
            v.batch_size_unit,
            CAST(NULL AS STRING) AS batch_type,
            CAST(NULL AS DATE)   AS retest_date,
            CAST(NULL AS DECIMAL(8, 4)) AS yield_pct,
            CAST(NULL AS STRING) AS batch_status,
            CAST(NULL AS DATE)   AS disposition_date,
            CAST(NULL AS STRING) AS packaging_configuration,
            TRUE AS is_active,
            CURRENT_TIMESTAMP() AS load_timestamp
        FROM l2_1_scl.src_vendor_analytical_results v
        WHERE v.is_current = TRUE AND v.batch_number IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY v.batch_number ORDER BY v.source_ingestion_timestamp DESC) = 1
    )
    SELECT
        sb.batch_key,
        sb.batch_number,
        sb.batch_system_id,
        dp.product_key,
        ds.site_key,
        sb.batch_type,
        sb.manufacturing_date,
        sb.expiry_date,
        sb.retest_date,
        sb.batch_size,
        sb.batch_size_unit,
        sb.yield_pct,
        sb.batch_status,
        sb.disposition_date,
        sb.packaging_configuration,
        sb.is_active,
        sb.load_timestamp
    FROM src_batch sb
    LEFT JOIN dim_product dp ON dp.product_id = sb.product_id_vendor
    LEFT JOIN dim_site ds ON ds.site_id = sb.site_id_vendor
) AS src
ON tgt.batch_key = src.batch_key
WHEN MATCHED THEN UPDATE SET
    batch_system_id = src.batch_system_id,
    manufacturing_date = src.manufacturing_date,
    expiry_date = src.expiry_date,
    batch_size = src.batch_size,
    batch_size_unit = src.batch_size_unit,
    load_timestamp = src.load_timestamp
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_instrument
-- MAGIC Analytical instrument dimension from vendor analytical results.

-- COMMAND ----------

MERGE INTO dim_instrument AS tgt
USING (
    SELECT DISTINCT
        HASH(v.instrument_id_vendor)        AS instrument_key,
        v.instrument_id_vendor              AS instrument_id,
        v.instrument_name,
        CAST(NULL AS STRING)                AS instrument_type,
        CAST(NULL AS STRING)                AS serial_number,
        CAST(NULL AS STRING)                AS manufacturer,
        CAST(NULL AS STRING)                AS qualification_status,
        CAST(NULL AS DATE)                  AS calibration_due_date,
        CAST(NULL AS STRING)                AS location,
        TRUE                                AS is_active,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_scl.src_vendor_analytical_results v
    WHERE v.is_current = TRUE AND v.instrument_id_vendor IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY v.instrument_id_vendor ORDER BY v.source_ingestion_timestamp DESC) = 1
) AS src
ON tgt.instrument_key = src.instrument_key
WHEN MATCHED THEN UPDATE SET
    instrument_name = src.instrument_name, load_timestamp = src.load_timestamp
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_laboratory
-- MAGIC QC / analytical laboratory dimension from vendor analytical results.

-- COMMAND ----------

MERGE INTO dim_laboratory AS tgt
USING (
    SELECT DISTINCT
        HASH(v.lab_id_vendor)               AS laboratory_key,
        v.lab_id_vendor                     AS laboratory_id,
        v.lab_name                          AS laboratory_name,
        CAST(NULL AS STRING)                AS laboratory_type,
        ds.site_key                         AS site_key,
        CAST(NULL AS STRING)                AS accreditation_status,
        TRUE                                AS is_active,
        CURRENT_TIMESTAMP()                 AS load_timestamp
    FROM l2_1_scl.src_vendor_analytical_results v
    LEFT JOIN dim_site ds ON ds.site_id = v.site_id_vendor
    WHERE v.is_current = TRUE AND v.lab_id_vendor IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY v.lab_id_vendor ORDER BY v.source_ingestion_timestamp DESC) = 1
) AS src
ON tgt.laboratory_key = src.laboratory_key
WHEN MATCHED THEN UPDATE SET
    laboratory_name = src.laboratory_name, load_timestamp = src.load_timestamp
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## fact_analytical_result
-- MAGIC Analytical test results from vendor sources, linked to specification dims.

-- COMMAND ----------

-- DBTITLE 1,fact_analytical_result MERGE (PQ/CMC enriched)
MERGE INTO fact_analytical_result AS tgt
USING (
    WITH src_results AS (
        SELECT
            HASH(v.source_result_id, 'VENDOR') AS analytical_result_key,
            v.batch_number,
            v.source_specification_id,
            v.source_spec_item_id,
            v.storage_condition_code,
            v.time_point_code,
            v.instrument_id_vendor,
            v.lab_id_vendor,
            v.uom_code,
            CAST(DATE_FORMAT(v.test_date, 'yyyyMMdd') AS INT) AS test_date_key,

            v.result_value,
            v.result_text,
            v.result_status_code,
            CAST(NULL AS DECIMAL(18, 6))    AS percent_label_claim,

            v.reported_lower_limit,
            v.reported_upper_limit,
            v.reported_target,

            -- Derive OOS flag: result outside reported limits
            CASE
                WHEN v.result_value IS NOT NULL AND v.reported_lower_limit IS NOT NULL
                    AND v.result_value < v.reported_lower_limit THEN TRUE
                WHEN v.result_value IS NOT NULL AND v.reported_upper_limit IS NOT NULL
                    AND v.result_value > v.reported_upper_limit THEN TRUE
                WHEN UPPER(v.result_status_code) = 'OOS' THEN TRUE
                ELSE FALSE
            END AS is_oos,
            CASE WHEN UPPER(v.result_status_code) = 'OOT' THEN TRUE ELSE FALSE END AS is_oot,

            CAST(NULL AS STRING)            AS sample_type,
            CAST(NULL AS INT)               AS replicate_number,

            v.analyst_name,
            v.reviewer_name,
            v.lab_name,
            v.report_id,
            v.coa_number,
            v.stability_study_id,

            v.source_result_id,
            TRUE AS is_current,
            CURRENT_TIMESTAMP() AS load_timestamp

        FROM l2_1_scl.src_vendor_analytical_results v
        WHERE v.is_current = TRUE
    )
    SELECT
        sr.analytical_result_key,
        db.batch_key,
        ds.spec_key,
        dsi.spec_item_key,
        sc.condition_key,
        tp.timepoint_key,
        di.instrument_key,
        dl.laboratory_key,
        u.uom_key,
        sr.test_date_key,

        sr.result_value,
        sr.result_text,
        sr.result_status_code,
        sr.percent_label_claim,

        sr.reported_lower_limit,
        sr.reported_upper_limit,
        sr.reported_target,

        sr.is_oos,
        sr.is_oot,

        sr.sample_type,
        sr.replicate_number,

        sr.analyst_name,
        sr.reviewer_name,
        sr.lab_name,
        sr.report_id,
        sr.coa_number,
        sr.stability_study_id,

        sr.source_result_id,
        sr.is_current,
        sr.load_timestamp
    FROM src_results sr
    LEFT JOIN dim_batch db ON db.batch_number = sr.batch_number
    LEFT JOIN dim_specification ds ON ds.source_specification_id = sr.source_specification_id
    LEFT JOIN dim_specification_item dsi ON dsi.source_spec_item_id = sr.source_spec_item_id
    LEFT JOIN dim_stability_condition sc ON sc.condition_code = sr.storage_condition_code
    LEFT JOIN dim_timepoint tp ON tp.timepoint_code = sr.time_point_code
    LEFT JOIN dim_instrument di ON di.instrument_id = sr.instrument_id_vendor
    LEFT JOIN dim_laboratory dl ON dl.laboratory_id = sr.lab_id_vendor
    LEFT JOIN dim_uom u ON u.uom_code = sr.uom_code
) AS src
ON tgt.analytical_result_key = src.analytical_result_key
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
UNION ALL SELECT 'dim_batch', COUNT(*) FROM dim_batch
UNION ALL SELECT 'dim_instrument', COUNT(*) FROM dim_instrument
UNION ALL SELECT 'dim_laboratory', COUNT(*) FROM dim_laboratory
UNION ALL SELECT 'fact_specification_limit', COUNT(*) FROM fact_specification_limit
UNION ALL SELECT 'fact_analytical_result', COUNT(*) FROM fact_analytical_result;
