-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Seed End-to-End Sample Data
-- MAGIC Inserts sample data across all layers for development and validation testing.
-- MAGIC
-- MAGIC **Scope:** One drug product specification (Acetaminophen 500 mg Tablet) with representative tests and limits.

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## L1 Raw — Sample Specification

-- COMMAND ----------

INSERT INTO l1_raw.raw_lims_specification VALUES
(
    'ing-001', 'LIMS', '/api/specs/export.json', 'BATCH-2026-001', CURRENT_TIMESTAMP(), 'abc123hash',
    'SPEC-001', 'QC-SPEC-2026-0001', '1.0', 'Acetaminophen 500 mg Film-Coated Tablets',
    'Drug Product', 'PROD-001', 'Acetaminophen 500 mg Tablets', 'MAT-001', 'Acetaminophen',
    'SITE-001', 'Springfield Plant', 'US', 'Film-Coated Tablet', '500 mg',
    'Approved', '2025-01-15', NULL, '2025-01-10', 'John Smith', '3.2.P.5.1',
    'Commercial', NULL, 'USP', '2024-12-01', '2025-01-10', 'admin', NULL
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## L1 Raw — Sample Spec Items

-- COMMAND ----------

INSERT INTO l1_raw.raw_lims_spec_item VALUES
    ('ing-101', 'LIMS', 'BATCH-2026-001', CURRENT_TIMESTAMP(), 'hash101', 'ITEM-001', 'SPEC-001', 'MTH-001', 'ASSAY', 'Assay',           'APAP', 'Assay',         'Chemical',  NULL,  '%',      'CQA', '1', 'Numeric',   '1', 'Y', 'USP <621>',    'Release', '2024-12-01', '2025-01-10'),
    ('ing-102', 'LIMS', 'BATCH-2026-001', CURRENT_TIMESTAMP(), 'hash102', 'ITEM-002', 'SPEC-001', 'MTH-002', 'DISSO', 'Dissolution',      'APAP', 'Dissolution',   'Physical',  NULL,  '% (Q)', 'CQA', '2', 'Numeric',   '0', 'Y', 'USP <711>',    'Release', '2024-12-01', '2025-01-10'),
    ('ing-103', 'LIMS', 'BATCH-2026-001', CURRENT_TIMESTAMP(), 'hash103', 'ITEM-003', 'SPEC-001', 'MTH-003', 'CU',    'Content Uniformity','APAP','Content Uniformity','Physical',NULL,'%',      'CQA', '3', 'Numeric',   '1', 'Y', 'USP <905>',    'Release', '2024-12-01', '2025-01-10'),
    ('ing-104', 'LIMS', 'BATCH-2026-001', CURRENT_TIMESTAMP(), 'hash104', 'ITEM-004', 'SPEC-001', 'MTH-004', 'IMPD',  'Impurity A',       'IMPA', 'Impurity A',    'Chemical',  'Impurity','%','CCQA','4', 'Numeric',   '2', 'Y', NULL,           'Release', '2024-12-01', '2025-01-10'),
    ('ing-105', 'LIMS', 'BATCH-2026-001', CURRENT_TIMESTAMP(), 'hash105', 'ITEM-005', 'SPEC-001', 'MTH-005', 'HARD',  'Hardness',          NULL,  'Hardness',      'Physical',  NULL,  'kp',    'NCQA','5', 'Numeric',   '1', 'N', NULL,           'Release', '2024-12-01', '2025-01-10'),
    ('ing-106', 'LIMS', 'BATCH-2026-001', CURRENT_TIMESTAMP(), 'hash106', 'ITEM-006', 'SPEC-001', NULL,       'DESC',  'Description',       NULL,  'Description',   'Physical',  NULL,  'N/A',   'REPORT','6','Text',     NULL,'N', NULL,           'Release', '2024-12-01', '2025-01-10');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## L1 Raw — Sample Spec Limits

-- COMMAND ----------

INSERT INTO l1_raw.raw_lims_spec_limit VALUES
    -- Assay AC
    ('ing-201','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h201','LIM-001','ITEM-001','SPEC-001','AC','Between','95.0','105.0','100.0',NULL,'%','as-is','Release',NULL,NULL,NULL,NULL,NULL,NULL,'TRUE','ICH Q6A','2024-12-01','2025-01-10'),
    -- Assay NOR
    ('ing-202','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h202','LIM-002','ITEM-001','SPEC-001','NOR','Between','97.0','103.0','100.0',NULL,'%','as-is','Release',NULL,NULL,'3_SIGMA','30','2025-01-05',NULL,NULL,'2024-12-01','2025-01-10'),
    -- Assay PAR
    ('ing-203','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h203','LIM-003','ITEM-001','SPEC-001','PAR','Between','90.0','110.0','100.0',NULL,'%','as-is','Release',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'2024-12-01','2025-01-10'),
    -- Dissolution AC
    ('ing-204','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h204','LIM-004','ITEM-002','SPEC-001','AC','NLT','80.0',NULL,NULL,NULL,'% (Q)','as-is','Release',NULL,NULL,NULL,NULL,NULL,'TRUE','USP','2024-12-01','2025-01-10'),
    -- Content Uniformity AC
    ('ing-205','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h205','LIM-005','ITEM-003','SPEC-001','AC','Between','85.0','115.0','100.0',NULL,'%','as-is','Release',NULL,NULL,NULL,NULL,NULL,'TRUE','USP <905>','2024-12-01','2025-01-10'),
    -- Impurity A AC
    ('ing-206','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h206','LIM-006','ITEM-004','SPEC-001','AC','NMT',NULL,'0.15',NULL,NULL,'%','as-is','Release',NULL,NULL,NULL,NULL,NULL,'TRUE','ICH Q3B','2024-12-01','2025-01-10'),
    -- Impurity A NOR
    ('ing-207','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h207','LIM-007','ITEM-004','SPEC-001','NOR','NMT',NULL,'0.10',NULL,NULL,'%','as-is','Release',NULL,NULL,'3_SIGMA','30','2025-01-05',NULL,NULL,'2024-12-01','2025-01-10'),
    -- Hardness AC
    ('ing-208','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h208','LIM-008','ITEM-005','SPEC-001','AC','Between','5.0','15.0','10.0',NULL,'kp','as-is','Release',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'2024-12-01','2025-01-10'),
    -- Description (text limit)
    ('ing-209','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h209','LIM-009','ITEM-006','SPEC-001','AC',NULL,NULL,NULL,NULL,'White to off-white film-coated tablets','N/A','as-is','Release',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'2024-12-01','2025-01-10'),
    -- Assay Stability AC (T6M 40C/75%RH)
    ('ing-210','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h210','LIM-010','ITEM-001','SPEC-001','AC','Between','90.0','110.0','100.0',NULL,'%','as-is','Stability','T6M','40C75RH',NULL,NULL,NULL,'TRUE','ICH Q1A','2024-12-01','2025-01-10');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## L2.2 — Sample MDM Dimensions

-- COMMAND ----------

-- dim_product
MERGE INTO l2_2_spec_unified.dim_product AS tgt
USING (VALUES
    (1, 'PROD-001', 'Acetaminophen 500 mg Tablets', 'Acetaminophen', 'Tylenol', 'Film-Coated Tablet', 'Oral', 'Pain/Antipyretic', '500 mg', TRUE, DATE'2020-01-01', NULL, CURRENT_TIMESTAMP())
) AS src(product_key, product_id, product_name, product_family, brand_name, dosage_form, route_of_administration, therapeutic_area, strength, is_active, effective_from, effective_to, load_timestamp)
ON tgt.product_key = src.product_key
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- dim_material
MERGE INTO l2_2_spec_unified.dim_material AS tgt
USING (VALUES
    (1, 'MAT-001', 'Acetaminophen', 'API', '103-90-2', 'Paracetamol', 'Acetaminophen USP', 'USP Grade', TRUE, DATE'2020-01-01', NULL, CURRENT_TIMESTAMP())
) AS src(material_key, material_id, material_name, material_type, cas_number, inn_name, compendial_name, grade, is_active, effective_from, effective_to, load_timestamp)
ON tgt.material_key = src.material_key
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- dim_test_method
MERGE INTO l2_2_spec_unified.dim_test_method AS tgt
USING (VALUES
    (1, 'MTH-001', 'HPLC Assay Method',          'AM-001', '1.0', 'COMPENDIAL', 'HPLC',          'USP <621>', TRUE,  TRUE, DATE'2020-01-01', NULL, CURRENT_TIMESTAMP()),
    (2, 'MTH-002', 'Dissolution Apparatus II',    'AM-002', '1.0', 'COMPENDIAL', 'UV-Vis',        'USP <711>', TRUE,  TRUE, DATE'2020-01-01', NULL, CURRENT_TIMESTAMP()),
    (3, 'MTH-003', 'Content Uniformity by HPLC',  'AM-003', '1.0', 'COMPENDIAL', 'HPLC',          'USP <905>', TRUE,  TRUE, DATE'2020-01-01', NULL, CURRENT_TIMESTAMP()),
    (4, 'MTH-004', 'Related Substances by HPLC',  'AM-004', '1.0', 'IN_HOUSE',   'HPLC',          NULL,        TRUE,  TRUE, DATE'2020-01-01', NULL, CURRENT_TIMESTAMP()),
    (5, 'MTH-005', 'Tablet Hardness Tester',      'AM-005', '1.0', 'IN_HOUSE',   'Mechanical',    NULL,        FALSE, TRUE, DATE'2020-01-01', NULL, CURRENT_TIMESTAMP())
) AS src(test_method_key, test_method_id, method_name, method_number, method_version, method_type, technique, compendia_reference, is_validated, is_active, effective_from, effective_to, load_timestamp)
ON tgt.test_method_key = src.test_method_key
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- dim_site
MERGE INTO l2_2_spec_unified.dim_site AS tgt
USING (VALUES
    (1, 'SITE-001', 'Springfield Plant', 'MANUFACTURING', 'US', 'United States', 'US', TRUE, DATE'2020-01-01', NULL, CURRENT_TIMESTAMP())
) AS src(site_key, site_id, site_name, site_type, country_code, country_name, region_code, is_active, effective_from, effective_to, load_timestamp)
ON tgt.site_key = src.site_key
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- dim_market
MERGE INTO l2_2_spec_unified.dim_market AS tgt
USING (VALUES
    (1, 'US', 'United States', 'US', 'United States', 'FDA', TRUE, CURRENT_TIMESTAMP())
) AS src(market_key, market_code, market_name, region_code, region_name, regulatory_authority, is_active, load_timestamp)
ON tgt.market_key = src.market_key
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify Sample Data Counts

-- COMMAND ----------

SELECT 'L1 raw_lims_specification' AS table_name, COUNT(*) AS rows FROM l1_raw.raw_lims_specification
UNION ALL
SELECT 'L1 raw_lims_spec_item', COUNT(*) FROM l1_raw.raw_lims_spec_item
UNION ALL
SELECT 'L1 raw_lims_spec_limit', COUNT(*) FROM l1_raw.raw_lims_spec_limit
UNION ALL
SELECT 'L2.2 dim_product', COUNT(*) FROM l2_2_spec_unified.dim_product
UNION ALL
SELECT 'L2.2 dim_material', COUNT(*) FROM l2_2_spec_unified.dim_material
UNION ALL
SELECT 'L2.2 dim_test_method', COUNT(*) FROM l2_2_spec_unified.dim_test_method
UNION ALL
SELECT 'L2.2 dim_site', COUNT(*) FROM l2_2_spec_unified.dim_site
UNION ALL
SELECT 'L2.2 dim_market', COUNT(*) FROM l2_2_spec_unified.dim_market;
