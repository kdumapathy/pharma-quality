-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Populate L2.1 Source Conform — LIMS + Recipe
-- MAGIC Transforms L1 raw data into L2.1 cleansed, typed, deduplicated tables.
-- MAGIC
-- MAGIC **Sources:**
-- MAGIC - `raw_lims_specification` → `src_lims_specification`
-- MAGIC - `raw_lims_spec_item` → `src_lims_spec_item`
-- MAGIC - `raw_lims_spec_limit` → `src_lims_spec_limit`
-- MAGIC - `raw_process_recipe` → `src_process_recipe`

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## src_lims_specification

-- COMMAND ----------

MERGE INTO l2_1_lims.src_lims_specification AS tgt
USING (
    SELECT
        specification_id                                AS source_specification_id,
        _batch_id                                       AS source_batch_id,
        _ingestion_timestamp                            AS source_ingestion_timestamp,
        _record_hash                                    AS record_hash,
        TRIM(spec_number)                               AS spec_number,
        COALESCE(TRIM(spec_version), '1.0')             AS spec_version,
        TRIM(spec_title)                                AS spec_title,
        CASE UPPER(TRIM(spec_type))
            WHEN 'DRUG PRODUCT'   THEN 'DP'
            WHEN 'DRUG SUBSTANCE' THEN 'DS'
            WHEN 'RAW MATERIAL'   THEN 'RM'
            WHEN 'EXCIPIENT'      THEN 'EXCIP'
            WHEN 'INTERMEDIATE'   THEN 'INTERMED'
            WHEN 'IN-PROCESS'     THEN 'IPC'
            ELSE UPPER(TRIM(spec_type))
        END                                             AS spec_type_code,
        TRIM(spec_type)                                 AS spec_type_name,
        TRIM(product_id)                                AS product_id_lims,
        TRIM(product_name)                              AS product_name,
        TRIM(material_id)                               AS material_id_lims,
        TRIM(material_name)                             AS material_name,
        TRIM(site_id)                                   AS site_id_lims,
        TRIM(site_name)                                 AS site_name,
        TRIM(market_region)                             AS market_region,
        TRIM(dosage_form)                               AS dosage_form,
        TRIM(strength)                                  AS strength,
        CASE UPPER(TRIM(status))
            WHEN 'APPROVED' THEN 'APP'
            WHEN 'DRAFT'    THEN 'DRA'
            WHEN 'SUPERSEDED' THEN 'SUP'
            WHEN 'OBSOLETE' THEN 'OBS'
            WHEN 'ARCHIVED' THEN 'ARCH'
            ELSE UPPER(TRIM(status))
        END                                             AS status_code,
        TRIM(status)                                    AS status_name,
        TRY_CAST(effective_start_date AS DATE)          AS effective_start_date,
        TRY_CAST(effective_end_date AS DATE)            AS effective_end_date,
        TRY_CAST(approval_date AS DATE)                 AS approval_date,
        TRIM(approved_by)                               AS approved_by,
        TRIM(ctd_ref)                                   AS ctd_section,
        CASE UPPER(TRIM(stage))
            WHEN 'DEVELOPMENT' THEN 'DEV'
            WHEN 'CLINICAL'    THEN 'CLI'
            WHEN 'COMMERCIAL'  THEN 'COM'
            ELSE UPPER(TRIM(stage))
        END                                             AS stage_code,
        TRIM(compendia)                                 AS compendia_reference,
        TRIM(superseded_by)                             AS supersedes_spec_id,
        CASE WHEN TRY_CAST(effective_start_date AS DATE) IS NULL AND effective_start_date IS NOT NULL THEN TRUE ELSE FALSE END AS dq_date_parse_error,
        TRUE                                            AS dq_type_code_mapped,
        TRUE                                            AS dq_status_code_mapped,
        FALSE                                           AS dq_duplicate_flag,
        CURRENT_TIMESTAMP()                             AS load_timestamp,
        TRUE                                            AS is_current
    FROM l1_raw.raw_lims_specification
    WHERE specification_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY specification_id ORDER BY _ingestion_timestamp DESC) = 1
) AS src
ON tgt.source_specification_id = src.source_specification_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## src_lims_spec_item

-- COMMAND ----------

MERGE INTO l2_1_lims.src_lims_spec_item AS tgt
USING (
    SELECT
        spec_item_id                                    AS source_spec_item_id,
        specification_id                                AS source_specification_id,
        _batch_id                                       AS source_batch_id,
        _ingestion_timestamp                            AS source_ingestion_timestamp,
        _record_hash                                    AS record_hash,
        TRIM(test_code)                                 AS test_code,
        TRIM(test_name)                                 AS test_name,
        TRIM(analyte_code)                              AS analyte_code,
        TRIM(parameter_name)                            AS parameter_name,
        CASE UPPER(TRIM(test_category))
            WHEN 'PHYSICAL'         THEN 'PHY'
            WHEN 'CHEMICAL'         THEN 'CHE'
            WHEN 'IMPURITY'         THEN 'IMP'
            WHEN 'MICROBIOLOGICAL'  THEN 'MIC'
            WHEN 'BIOLOGICAL'       THEN 'BIO'
            WHEN 'STERILITY'        THEN 'STER'
            WHEN 'PACKAGING'        THEN 'PACK'
            ELSE UPPER(TRIM(test_category))
        END                                             AS test_category_code,
        TRIM(test_category)                             AS test_category_name,
        TRIM(test_subcategory)                          AS test_subcategory,
        TRIM(uom)                                       AS uom_code,
        UPPER(TRIM(criticality))                        AS criticality_code,
        TRY_CAST(sequence_number AS INT)                AS sequence_number,
        CASE UPPER(TRIM(reporting_type))
            WHEN 'NUMERIC'   THEN 'NUMERIC'
            WHEN 'PASS-FAIL' THEN 'PASS_FAIL'
            WHEN 'TEXT'       THEN 'TEXT'
            ELSE UPPER(TRIM(reporting_type))
        END                                             AS reporting_type,
        TRY_CAST(result_precision AS INT)               AS result_precision,
        CASE UPPER(TRIM(is_required))
            WHEN 'Y' THEN TRUE
            WHEN 'YES' THEN TRUE
            WHEN 'TRUE' THEN TRUE
            ELSE FALSE
        END                                             AS is_required,
        TRIM(compendia_ref)                             AS compendia_test_ref,
        CASE UPPER(TRIM(stage_applicability))
            WHEN 'RELEASE'   THEN 'RELEASE'
            WHEN 'STABILITY' THEN 'STABILITY'
            WHEN 'IPC'       THEN 'IPC'
            WHEN 'BOTH'      THEN 'BOTH'
            ELSE UPPER(TRIM(stage_applicability))
        END                                             AS stage_applicability,
        TRIM(test_method_id)                            AS test_method_id_lims,
        TRUE                                            AS dq_category_mapped,
        TRUE                                            AS dq_criticality_mapped,
        FALSE                                           AS dq_type_cast_error,
        CURRENT_TIMESTAMP()                             AS load_timestamp,
        TRUE                                            AS is_current
    FROM l1_raw.raw_lims_spec_item
    WHERE spec_item_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY spec_item_id ORDER BY _ingestion_timestamp DESC) = 1
) AS src
ON tgt.source_spec_item_id = src.source_spec_item_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## src_lims_spec_limit

-- COMMAND ----------

MERGE INTO l2_1_lims.src_lims_spec_limit AS tgt
USING (
    SELECT
        limit_id                                        AS source_limit_id,
        spec_item_id                                    AS source_spec_item_id,
        specification_id                                AS source_specification_id,
        _batch_id                                       AS source_batch_id,
        _ingestion_timestamp                            AS source_ingestion_timestamp,
        _record_hash                                    AS record_hash,
        UPPER(TRIM(limit_type))                         AS limit_type_code,
        TRY_CAST(lower_limit AS DECIMAL(18,6))          AS lower_limit_value,
        TRY_CAST(upper_limit AS DECIMAL(18,6))          AS upper_limit_value,
        TRY_CAST(target_value AS DECIMAL(18,6))         AS target_value,
        CASE
            WHEN UPPER(TRIM(comparison_operator)) IN ('NLT','GT','GTE') THEN UPPER(TRIM(comparison_operator))
            WHEN lower_limit IS NOT NULL AND upper_limit IS NOT NULL THEN 'GTE'
            WHEN lower_limit IS NOT NULL THEN 'NLT'
            ELSE 'NONE'
        END                                             AS lower_limit_operator,
        CASE
            WHEN UPPER(TRIM(comparison_operator)) IN ('NMT','LT','LTE') THEN UPPER(TRIM(comparison_operator))
            WHEN lower_limit IS NOT NULL AND upper_limit IS NOT NULL THEN 'LTE'
            WHEN upper_limit IS NOT NULL THEN 'NMT'
            ELSE 'NONE'
        END                                             AS upper_limit_operator,
        TRIM(limit_text)                                AS limit_text,
        CASE
            WHEN limit_text IS NOT NULL THEN limit_text
            WHEN lower_limit IS NOT NULL AND upper_limit IS NOT NULL THEN CONCAT(lower_limit, ' - ', upper_limit, ' ', COALESCE(uom, ''))
            WHEN lower_limit IS NOT NULL THEN CONCAT('NLT ', lower_limit, ' ', COALESCE(uom, ''))
            WHEN upper_limit IS NOT NULL THEN CONCAT('NMT ', upper_limit, ' ', COALESCE(uom, ''))
            ELSE NULL
        END                                             AS limit_description,
        TRIM(uom)                                       AS uom_code,
        UPPER(REPLACE(TRIM(limit_basis), '-', '_'))     AS limit_basis,
        CASE UPPER(TRIM(stage))
            WHEN 'RELEASE'   THEN 'RELEASE'
            WHEN 'STABILITY' THEN 'STABILITY'
            WHEN 'IPC'       THEN 'IPC'
            ELSE UPPER(TRIM(stage))
        END                                             AS stage_code,
        UPPER(TRIM(stability_time_point))               AS stability_time_point,
        UPPER(TRIM(stability_condition))                AS stability_condition,
        TRY_CAST(effective_start_date AS DATE)          AS effective_start_date,
        TRY_CAST(effective_end_date AS DATE)            AS effective_end_date,
        UPPER(TRIM(calculation_method))                 AS calculation_method,
        TRY_CAST(sample_size AS INT)                    AS sample_size,
        TRY_CAST(last_calculated_date AS DATE)          AS last_calculated_date,
        CASE UPPER(TRIM(is_in_filing))
            WHEN 'TRUE' THEN TRUE
            WHEN 'Y' THEN TRUE
            ELSE FALSE
        END                                             AS is_in_filing,
        TRIM(regulatory_basis)                          AS regulatory_basis,
        TRUE                                            AS dq_limit_type_mapped,
        TRUE                                            AS dq_operator_mapped,
        CASE WHEN TRY_CAST(lower_limit AS DECIMAL(18,6)) IS NULL AND lower_limit IS NOT NULL THEN TRUE ELSE FALSE END AS dq_numeric_cast_error,
        FALSE                                           AS dq_date_parse_error,
        CURRENT_TIMESTAMP()                             AS load_timestamp,
        TRUE                                            AS is_current
    FROM l1_raw.raw_lims_spec_limit
    WHERE limit_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY limit_id ORDER BY _ingestion_timestamp DESC) = 1
) AS src
ON tgt.source_limit_id = src.source_limit_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## src_process_recipe

-- COMMAND ----------

MERGE INTO l2_1_lims.src_process_recipe AS tgt
USING (
    SELECT
        recipe_id                                       AS source_recipe_id,
        specification_id                                AS source_specification_id,
        spec_item_id                                    AS source_spec_item_id,
        _batch_id                                       AS source_batch_id,
        _ingestion_timestamp                            AS source_ingestion_timestamp,
        _record_hash                                    AS record_hash,
        TRIM(recipe_name)                               AS recipe_name,
        TRIM(recipe_version)                            AS recipe_version,
        UPPER(TRIM(recipe_type))                        AS recipe_type,
        TRIM(parameter_code)                            AS parameter_code,
        TRIM(parameter_name)                            AS parameter_name,
        TRIM(product_id)                                AS product_id_recipe,
        TRIM(product_name)                              AS product_name,
        TRIM(material_id)                               AS material_id_recipe,
        TRIM(material_name)                             AS material_name,
        TRIM(site_id)                                   AS site_id_recipe,
        TRIM(site_name)                                 AS site_name,
        UPPER(TRIM(limit_type))                         AS limit_type_code,
        TRY_CAST(lower_limit AS DECIMAL(18,6))          AS lower_limit_value,
        TRY_CAST(upper_limit AS DECIMAL(18,6))          AS upper_limit_value,
        TRY_CAST(target_value AS DECIMAL(18,6))         AS target_value,
        TRIM(uom)                                       AS uom_code,
        UPPER(REPLACE(TRIM(limit_basis), '-', '_'))     AS limit_basis,
        CASE UPPER(TRIM(stage))
            WHEN 'DEVELOPMENT' THEN 'DEV'
            WHEN 'CLINICAL'    THEN 'CLI'
            WHEN 'COMMERCIAL'  THEN 'COM'
            ELSE UPPER(TRIM(stage))
        END                                             AS stage_code,
        UPPER(TRIM(calculation_method))                 AS calculation_method,
        TRY_CAST(sample_size AS INT)                    AS sample_size,
        TRY_CAST(cpk_value AS DECIMAL(8,4))             AS cpk_value,
        TRY_CAST(last_calculated_date AS DATE)          AS last_calculated_date,
        TRY_CAST(effective_start_date AS DATE)          AS effective_start_date,
        TRY_CAST(effective_end_date AS DATE)            AS effective_end_date,
        TRUE                                            AS dq_limit_type_mapped,
        CASE WHEN TRY_CAST(lower_limit AS DECIMAL(18,6)) IS NULL AND lower_limit IS NOT NULL THEN TRUE ELSE FALSE END AS dq_numeric_cast_error,
        CASE WHEN TRY_CAST(effective_start_date AS DATE) IS NULL AND effective_start_date IS NOT NULL THEN TRUE ELSE FALSE END AS dq_date_parse_error,
        CASE WHEN specification_id IS NOT NULL THEN TRUE ELSE FALSE END AS dq_spec_link_valid,
        CURRENT_TIMESTAMP()                             AS load_timestamp,
        TRUE                                            AS is_current
    FROM l1_raw.raw_process_recipe
    WHERE recipe_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY recipe_id, limit_type, spec_item_id ORDER BY _ingestion_timestamp DESC) = 1
) AS src
ON tgt.source_recipe_id = src.source_recipe_id
    AND tgt.limit_type_code = src.limit_type_code
    AND COALESCE(tgt.source_spec_item_id, '') = COALESCE(src.source_spec_item_id, '')
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## src_pdf_specification
-- MAGIC Transforms the flat transcribed PDF/SOP data into typed, standardized rows.

-- COMMAND ----------

MERGE INTO l2_1_lims.src_pdf_specification AS tgt
USING (
    SELECT
        document_id                                     AS source_document_id,
        CONCAT(document_id, ':', COALESCE(test_code, test_name), ':', COALESCE(limit_type, 'AC')) AS source_row_key,
        _batch_id                                       AS source_batch_id,
        _ingestion_timestamp                            AS source_ingestion_timestamp,
        _record_hash                                    AS record_hash,

        TRIM(document_name)                             AS document_name,
        TRIM(document_version)                          AS document_version,
        UPPER(TRIM(document_type))                      AS document_type,
        TRIM(sop_number)                                AS sop_number,
        TRY_CAST(page_number AS INT)                    AS page_number,
        TRIM(section_reference)                         AS section_reference,
        TRY_CAST(transcription_date AS DATE)            AS transcription_date,
        TRIM(transcribed_by)                            AS transcribed_by,

        TRIM(spec_number)                               AS spec_number,
        COALESCE(TRIM(spec_version), '1.0')             AS spec_version,
        TRIM(spec_title)                                AS spec_title,
        CASE UPPER(TRIM(spec_type))
            WHEN 'DRUG PRODUCT'   THEN 'DP'
            WHEN 'DRUG SUBSTANCE' THEN 'DS'
            WHEN 'RAW MATERIAL'   THEN 'RM'
            WHEN 'EXCIPIENT'      THEN 'EXCIP'
            WHEN 'INTERMEDIATE'   THEN 'INTERMED'
            WHEN 'IN-PROCESS'     THEN 'IPC'
            ELSE UPPER(TRIM(spec_type))
        END                                             AS spec_type_code,

        TRIM(product_id)                                AS product_id_pdf,
        TRIM(product_name)                              AS product_name,
        TRIM(material_id)                               AS material_id_pdf,
        TRIM(material_name)                             AS material_name,
        TRIM(site_name)                                 AS site_name,
        TRIM(market_region)                             AS market_region,

        UPPER(TRIM(test_code))                          AS test_code,
        TRIM(test_name)                                 AS test_name,
        CASE UPPER(TRIM(test_category))
            WHEN 'PHYSICAL'         THEN 'PHY'
            WHEN 'CHEMICAL'         THEN 'CHE'
            WHEN 'IMPURITY'         THEN 'IMP'
            WHEN 'MICROBIOLOGICAL'  THEN 'MIC'
            WHEN 'BIOLOGICAL'       THEN 'BIO'
            WHEN 'STERILITY'        THEN 'STER'
            WHEN 'PACKAGING'        THEN 'PACK'
            ELSE UPPER(TRIM(test_category))
        END                                             AS test_category_code,
        TRIM(test_method_reference)                     AS test_method_reference,
        TRIM(uom)                                       AS uom_code,
        UPPER(TRIM(criticality))                        AS criticality_code,

        UPPER(TRIM(limit_type))                         AS limit_type_code,
        TRY_CAST(lower_limit AS DECIMAL(18,6))          AS lower_limit_value,
        TRY_CAST(upper_limit AS DECIMAL(18,6))          AS upper_limit_value,
        TRY_CAST(target_value AS DECIMAL(18,6))         AS target_value,
        TRIM(limit_text)                                AS limit_text,
        TRIM(limit_expression)                          AS limit_expression,

        TRIM(ctd_section)                               AS ctd_section,
        TRIM(compendia_reference)                       AS compendia_reference,
        TRIM(regulatory_basis)                          AS regulatory_basis,
        CASE UPPER(TRIM(stage))
            WHEN 'RELEASE'   THEN 'RELEASE'
            WHEN 'STABILITY' THEN 'STABILITY'
            WHEN 'IPC'       THEN 'IPC'
            ELSE UPPER(TRIM(stage))
        END                                             AS stage_code,
        UPPER(TRIM(stability_condition))                AS stability_condition,

        TRY_CAST(effective_date AS DATE)                AS effective_date,
        TRY_CAST(approval_date AS DATE)                 AS approval_date,
        TRIM(approved_by)                               AS approved_by,

        CASE WHEN spec_number IS NOT NULL THEN TRUE ELSE FALSE END AS dq_spec_number_present,
        CASE WHEN TRY_CAST(lower_limit AS DECIMAL(18,6)) IS NULL AND lower_limit IS NOT NULL THEN TRUE ELSE FALSE END AS dq_numeric_cast_error,
        CASE WHEN TRY_CAST(effective_date AS DATE) IS NULL AND effective_date IS NOT NULL THEN TRUE ELSE FALSE END AS dq_date_parse_error,
        TRUE                                            AS dq_limit_type_mapped,
        CURRENT_TIMESTAMP()                             AS load_timestamp,
        TRUE                                            AS is_current
    FROM l1_raw.raw_pdf_specification
    WHERE document_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY document_id, COALESCE(test_code, test_name), COALESCE(limit_type, 'AC'), COALESCE(stage, 'Release')
        ORDER BY _ingestion_timestamp DESC
    ) = 1
) AS src
ON tgt.source_row_key = src.source_row_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify L2.1 Counts

-- COMMAND ----------

SELECT 'src_lims_specification' AS table_name, COUNT(*) AS rows FROM l2_1_lims.src_lims_specification
UNION ALL
SELECT 'src_lims_spec_item', COUNT(*) FROM l2_1_lims.src_lims_spec_item
UNION ALL
SELECT 'src_lims_spec_limit', COUNT(*) FROM l2_1_lims.src_lims_spec_limit
UNION ALL
SELECT 'src_process_recipe', COUNT(*) FROM l2_1_lims.src_process_recipe
UNION ALL
SELECT 'src_pdf_specification', COUNT(*) FROM l2_1_lims.src_pdf_specification;
