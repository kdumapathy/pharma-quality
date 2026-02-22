-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Seed End-to-End Sample Data — Raw Layer Only
-- MAGIC Inserts sample data into **L1 raw tables only**. All downstream layers (L2.1, L2.2, L3)
-- MAGIC are populated by the data load pipeline.
-- MAGIC
-- MAGIC **Sources seeded:**
-- MAGIC - LIMS — One drug product specification (Acetaminophen 500 mg Tablet) with AC limits
-- MAGIC - Process Recipe — NOR, PAR, and Target limits for the same specification

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## L1 Raw — LIMS Specification Header

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
-- MAGIC ## L1 Raw — LIMS Specification Items

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
-- MAGIC ## L1 Raw — LIMS Specification Limits (Acceptance Criteria)

-- COMMAND ----------

INSERT INTO l1_raw.raw_lims_spec_limit VALUES
    -- Assay AC
    ('ing-201','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h201','LIM-001','ITEM-001','SPEC-001','AC','Between','95.0','105.0','100.0',NULL,'%','as-is','Release',NULL,NULL,NULL,NULL,NULL,'TRUE','ICH Q6A','2024-12-01','2025-01-10'),
    -- Dissolution AC
    ('ing-204','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h204','LIM-004','ITEM-002','SPEC-001','AC','NLT','80.0',NULL,NULL,NULL,'% (Q)','as-is','Release',NULL,NULL,NULL,NULL,NULL,'TRUE','USP','2024-12-01','2025-01-10'),
    -- Content Uniformity AC
    ('ing-205','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h205','LIM-005','ITEM-003','SPEC-001','AC','Between','85.0','115.0','100.0',NULL,'%','as-is','Release',NULL,NULL,NULL,NULL,NULL,'TRUE','USP <905>','2024-12-01','2025-01-10'),
    -- Impurity A AC
    ('ing-206','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h206','LIM-006','ITEM-004','SPEC-001','AC','NMT',NULL,'0.15',NULL,NULL,'%','as-is','Release',NULL,NULL,NULL,NULL,NULL,'TRUE','ICH Q3B','2024-12-01','2025-01-10'),
    -- Hardness AC
    ('ing-208','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h208','LIM-008','ITEM-005','SPEC-001','AC','Between','5.0','15.0','10.0',NULL,'kp','as-is','Release',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'2024-12-01','2025-01-10'),
    -- Description (text limit)
    ('ing-209','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h209','LIM-009','ITEM-006','SPEC-001','AC',NULL,NULL,NULL,NULL,'White to off-white film-coated tablets','N/A','as-is','Release',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'2024-12-01','2025-01-10'),
    -- Assay Stability AC (T6M 40C/75%RH)
    ('ing-210','LIMS','BATCH-2026-001',CURRENT_TIMESTAMP(),'h210','LIM-010','ITEM-001','SPEC-001','AC','Between','90.0','110.0','100.0',NULL,'%','as-is','Stability','T6M','40C75RH',NULL,NULL,NULL,'TRUE','ICH Q1A','2024-12-01','2025-01-10');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## L1 Raw — Process Recipe Limits (NOR / PAR / Target)
-- MAGIC NOR and PAR limits from the Recipe Management system for the same Acetaminophen 500 mg specification.

-- COMMAND ----------

INSERT INTO l1_raw.raw_process_recipe VALUES
    -- Assay NOR (from 3-sigma SPC, 30 batches)
    ('ring-001','RECIPE','/api/recipes/export.json','RBATCH-2026-001',CURRENT_TIMESTAMP(),'rh001',
     'RCP-001','Acetaminophen 500mg Tablet Recipe','2.0','MANUFACTURING',
     'PROD-001','Acetaminophen 500 mg Tablets','MAT-001','Acetaminophen','SITE-001','Springfield Plant',
     'SPEC-001','ITEM-001','ASSAY','Assay',
     'NOR','97.0','103.0','100.0','%','as-is','Commercial',
     '3_SIGMA','30','1.85','2025-01-05',
     '2024-06-01',NULL,'Approved','Jane Doe','2024-06-01','2025-01-05'),

    -- Assay PAR (from validation data)
    ('ring-002','RECIPE','/api/recipes/export.json','RBATCH-2026-001',CURRENT_TIMESTAMP(),'rh002',
     'RCP-001','Acetaminophen 500mg Tablet Recipe','2.0','MANUFACTURING',
     'PROD-001','Acetaminophen 500 mg Tablets','MAT-001','Acetaminophen','SITE-001','Springfield Plant',
     'SPEC-001','ITEM-001','ASSAY','Assay',
     'PAR','90.0','110.0','100.0','%','as-is','Commercial',
     NULL,NULL,NULL,NULL,
     '2024-06-01',NULL,'Approved','Jane Doe','2024-06-01','2025-01-05'),

    -- Dissolution NOR
    ('ring-003','RECIPE','/api/recipes/export.json','RBATCH-2026-001',CURRENT_TIMESTAMP(),'rh003',
     'RCP-001','Acetaminophen 500mg Tablet Recipe','2.0','MANUFACTURING',
     'PROD-001','Acetaminophen 500 mg Tablets','MAT-001','Acetaminophen','SITE-001','Springfield Plant',
     'SPEC-001','ITEM-002','DISSO','Dissolution',
     'NOR','85.0',NULL,'92.0','% (Q)','as-is','Commercial',
     '3_SIGMA','30','2.10','2025-01-05',
     '2024-06-01',NULL,'Approved','Jane Doe','2024-06-01','2025-01-05'),

    -- Content Uniformity NOR
    ('ring-004','RECIPE','/api/recipes/export.json','RBATCH-2026-001',CURRENT_TIMESTAMP(),'rh004',
     'RCP-001','Acetaminophen 500mg Tablet Recipe','2.0','MANUFACTURING',
     'PROD-001','Acetaminophen 500 mg Tablets','MAT-001','Acetaminophen','SITE-001','Springfield Plant',
     'SPEC-001','ITEM-003','CU','Content Uniformity',
     'NOR','90.0','110.0','100.0','%','as-is','Commercial',
     'CPK','30','1.55','2025-01-05',
     '2024-06-01',NULL,'Approved','Jane Doe','2024-06-01','2025-01-05'),

    -- Impurity A NOR
    ('ring-005','RECIPE','/api/recipes/export.json','RBATCH-2026-001',CURRENT_TIMESTAMP(),'rh005',
     'RCP-001','Acetaminophen 500mg Tablet Recipe','2.0','MANUFACTURING',
     'PROD-001','Acetaminophen 500 mg Tablets','MAT-001','Acetaminophen','SITE-001','Springfield Plant',
     'SPEC-001','ITEM-004','IMPD','Impurity A',
     'NOR',NULL,'0.10',NULL,'%','as-is','Commercial',
     '3_SIGMA','30','2.30','2025-01-05',
     '2024-06-01',NULL,'Approved','Jane Doe','2024-06-01','2025-01-05'),

    -- Hardness NOR
    ('ring-006','RECIPE','/api/recipes/export.json','RBATCH-2026-001',CURRENT_TIMESTAMP(),'rh006',
     'RCP-001','Acetaminophen 500mg Tablet Recipe','2.0','MANUFACTURING',
     'PROD-001','Acetaminophen 500 mg Tablets','MAT-001','Acetaminophen','SITE-001','Springfield Plant',
     'SPEC-001','ITEM-005','HARD','Hardness',
     'NOR','7.0','13.0','10.0','kp','as-is','Commercial',
     '3_SIGMA','30','1.70','2025-01-05',
     '2024-06-01',NULL,'Approved','Jane Doe','2024-06-01','2025-01-05'),

    -- Hardness PAR
    ('ring-007','RECIPE','/api/recipes/export.json','RBATCH-2026-001',CURRENT_TIMESTAMP(),'rh007',
     'RCP-001','Acetaminophen 500mg Tablet Recipe','2.0','MANUFACTURING',
     'PROD-001','Acetaminophen 500 mg Tablets','MAT-001','Acetaminophen','SITE-001','Springfield Plant',
     'SPEC-001','ITEM-005','HARD','Hardness',
     'PAR','4.0','16.0','10.0','kp','as-is','Commercial',
     NULL,NULL,NULL,NULL,
     '2024-06-01',NULL,'Approved','Jane Doe','2024-06-01','2025-01-05');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## L1 Raw — Transcribed PDF/SOP Specification (Ibuprofen 200 mg Tablet)
-- MAGIC A second product specification transcribed from a regulatory SOP document into CSV.
-- MAGIC Each row = one test-limit combination from the source document.

-- COMMAND ----------

INSERT INTO l1_raw.raw_pdf_specification VALUES
    -- Assay AC (page 3 of SOP)
    ('ping-001','PDF','SOP-QC-042-Ibuprofen-200mg-Spec-v2.pdf','PBATCH-2026-001',CURRENT_TIMESTAMP(),'ph001',
     'DOC-042','SOP-QC-042 Ibuprofen 200mg Tablet Specification','2.0','SOP','SOP-QC-042','3','Section 5.1 Acceptance Criteria','2025-02-01','Data Entry Team',
     'QC-SPEC-2026-0042','2.0','Ibuprofen 200 mg Film-Coated Tablets','Drug Product',
     'PROD-002','Ibuprofen 200 mg Tablets','MAT-002','Ibuprofen','Milwaukee Plant','US',
     'ASSAY','Assay','Chemical','USP <621>','%','CQA',
     'AC','98.0','102.0','100.0',NULL,'98.0% - 102.0%',
     '3.2.P.5.1','USP','ICH Q6A','Release',NULL,
     '2025-01-20','2025-01-18','Sarah Johnson'),

    -- Assay NOR (page 4)
    ('ping-002','PDF','SOP-QC-042-Ibuprofen-200mg-Spec-v2.pdf','PBATCH-2026-001',CURRENT_TIMESTAMP(),'ph002',
     'DOC-042','SOP-QC-042 Ibuprofen 200mg Tablet Specification','2.0','SOP','SOP-QC-042','4','Section 5.2 Normal Operating Ranges','2025-02-01','Data Entry Team',
     'QC-SPEC-2026-0042','2.0','Ibuprofen 200 mg Film-Coated Tablets','Drug Product',
     'PROD-002','Ibuprofen 200 mg Tablets','MAT-002','Ibuprofen','Milwaukee Plant','US',
     'ASSAY','Assay','Chemical','USP <621>','%','CQA',
     'NOR','99.0','101.0','100.0',NULL,'99.0% - 101.0%',
     '3.2.P.5.1','USP',NULL,'Release',NULL,
     '2025-01-20','2025-01-18','Sarah Johnson'),

    -- Dissolution AC (page 3)
    ('ping-003','PDF','SOP-QC-042-Ibuprofen-200mg-Spec-v2.pdf','PBATCH-2026-001',CURRENT_TIMESTAMP(),'ph003',
     'DOC-042','SOP-QC-042 Ibuprofen 200mg Tablet Specification','2.0','SOP','SOP-QC-042','3','Section 5.1 Acceptance Criteria','2025-02-01','Data Entry Team',
     'QC-SPEC-2026-0042','2.0','Ibuprofen 200 mg Film-Coated Tablets','Drug Product',
     'PROD-002','Ibuprofen 200 mg Tablets','MAT-002','Ibuprofen','Milwaukee Plant','US',
     'DISSO','Dissolution','Physical','USP <711>','%','CQA',
     'AC','75.0',NULL,NULL,NULL,'NLT 75% (Q) in 30 minutes',
     '3.2.P.5.1','USP','ICH Q6A','Release',NULL,
     '2025-01-20','2025-01-18','Sarah Johnson'),

    -- Total Impurities AC (page 3)
    ('ping-004','PDF','SOP-QC-042-Ibuprofen-200mg-Spec-v2.pdf','PBATCH-2026-001',CURRENT_TIMESTAMP(),'ph004',
     'DOC-042','SOP-QC-042 Ibuprofen 200mg Tablet Specification','2.0','SOP','SOP-QC-042','3','Section 5.1 Acceptance Criteria','2025-02-01','Data Entry Team',
     'QC-SPEC-2026-0042','2.0','Ibuprofen 200 mg Film-Coated Tablets','Drug Product',
     'PROD-002','Ibuprofen 200 mg Tablets','MAT-002','Ibuprofen','Milwaukee Plant','US',
     'IMPTOT','Total Impurities','Chemical',NULL,'%','CQA',
     'AC',NULL,'1.0',NULL,NULL,'NMT 1.0%',
     '3.2.P.5.1','USP','ICH Q3B','Release',NULL,
     '2025-01-20','2025-01-18','Sarah Johnson'),

    -- Total Impurities NOR (page 4)
    ('ping-005','PDF','SOP-QC-042-Ibuprofen-200mg-Spec-v2.pdf','PBATCH-2026-001',CURRENT_TIMESTAMP(),'ph005',
     'DOC-042','SOP-QC-042 Ibuprofen 200mg Tablet Specification','2.0','SOP','SOP-QC-042','4','Section 5.2 Normal Operating Ranges','2025-02-01','Data Entry Team',
     'QC-SPEC-2026-0042','2.0','Ibuprofen 200 mg Film-Coated Tablets','Drug Product',
     'PROD-002','Ibuprofen 200 mg Tablets','MAT-002','Ibuprofen','Milwaukee Plant','US',
     'IMPTOT','Total Impurities','Chemical',NULL,'%','CQA',
     'NOR',NULL,'0.7',NULL,NULL,'NMT 0.7%',
     '3.2.P.5.1','USP',NULL,'Release',NULL,
     '2025-01-20','2025-01-18','Sarah Johnson'),

    -- Hardness AC (page 3)
    ('ping-006','PDF','SOP-QC-042-Ibuprofen-200mg-Spec-v2.pdf','PBATCH-2026-001',CURRENT_TIMESTAMP(),'ph006',
     'DOC-042','SOP-QC-042 Ibuprofen 200mg Tablet Specification','2.0','SOP','SOP-QC-042','3','Section 5.1 Acceptance Criteria','2025-02-01','Data Entry Team',
     'QC-SPEC-2026-0042','2.0','Ibuprofen 200 mg Film-Coated Tablets','Drug Product',
     'PROD-002','Ibuprofen 200 mg Tablets','MAT-002','Ibuprofen','Milwaukee Plant','US',
     'HARD','Hardness','Physical',NULL,'kp','NCQA',
     'AC','6.0','14.0','10.0',NULL,'6.0 - 14.0 kp',
     NULL,NULL,NULL,'Release',NULL,
     '2025-01-20','2025-01-18','Sarah Johnson'),

    -- Description AC (page 3) — text limit
    ('ping-007','PDF','SOP-QC-042-Ibuprofen-200mg-Spec-v2.pdf','PBATCH-2026-001',CURRENT_TIMESTAMP(),'ph007',
     'DOC-042','SOP-QC-042 Ibuprofen 200mg Tablet Specification','2.0','SOP','SOP-QC-042','3','Section 5.1 Acceptance Criteria','2025-02-01','Data Entry Team',
     'QC-SPEC-2026-0042','2.0','Ibuprofen 200 mg Film-Coated Tablets','Drug Product',
     'PROD-002','Ibuprofen 200 mg Tablets','MAT-002','Ibuprofen','Milwaukee Plant','US',
     'DESC','Description','Physical',NULL,'N/A','REPORT',
     'AC',NULL,NULL,NULL,'White to off-white round biconvex film-coated tablets','White to off-white round biconvex film-coated tablets',
     NULL,NULL,NULL,'Release',NULL,
     '2025-01-20','2025-01-18','Sarah Johnson'),

    -- Assay Stability AC (page 5 — accelerated 40C/75%RH T6M)
    ('ping-008','PDF','SOP-QC-042-Ibuprofen-200mg-Spec-v2.pdf','PBATCH-2026-001',CURRENT_TIMESTAMP(),'ph008',
     'DOC-042','SOP-QC-042 Ibuprofen 200mg Tablet Specification','2.0','SOP','SOP-QC-042','5','Section 6.1 Stability Specifications','2025-02-01','Data Entry Team',
     'QC-SPEC-2026-0042','2.0','Ibuprofen 200 mg Film-Coated Tablets','Drug Product',
     'PROD-002','Ibuprofen 200 mg Tablets','MAT-002','Ibuprofen','Milwaukee Plant','US',
     'ASSAY','Assay','Chemical','USP <621>','%','CQA',
     'AC','95.0','105.0','100.0',NULL,'95.0% - 105.0%',
     '3.2.P.5.1','USP','ICH Q1A','Stability','40C75RH',
     '2025-01-20','2025-01-18','Sarah Johnson');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify Raw Data Counts

-- COMMAND ----------

SELECT 'raw_lims_specification' AS table_name, COUNT(*) AS rows FROM l1_raw.raw_lims_specification
UNION ALL
SELECT 'raw_lims_spec_item', COUNT(*) FROM l1_raw.raw_lims_spec_item
UNION ALL
SELECT 'raw_lims_spec_limit', COUNT(*) FROM l1_raw.raw_lims_spec_limit
UNION ALL
SELECT 'raw_process_recipe', COUNT(*) FROM l1_raw.raw_process_recipe
UNION ALL
SELECT 'raw_pdf_specification', COUNT(*) FROM l1_raw.raw_pdf_specification;
