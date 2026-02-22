-- =============================================================================
-- End-to-End Sample Data — Pharma Quality Unified Data Model
-- =============================================================================
-- Scenario: Atorvastatin Calcium 10mg Tablets (Drug Product)
--           and its Drug Substance (Atorvastatin Calcium)
--
-- Flow validated:
--   L1 (raw LIMS data)
--   → L2.1 (source conform — cleansed, typed)
--   → L2.2 (unified model — star schema with dim/fact)
--   → L3 (OBT — CTD-ready flat output)
--
-- To run: execute sections sequentially in a Databricks notebook
--         against the pharma_quality catalog.
-- =============================================================================

-- =============================================================================
-- SECTION 1: L1 RAW DATA (simulates LIMS API / file ingest)
-- =============================================================================

USE CATALOG pharma_quality;
USE SCHEMA l1_raw;

-- L1: Specification Header -----------------------------------------------
INSERT INTO l1_raw.raw_lims_specification VALUES (
    'ING-001', 'LIMS', 'lims_spec_export_20240115.json', 'BATCH-20240115-01',
    current_timestamp(), 'abc123hash',
    'LIMS-SP-DP-001', 'SP-DP-ATV-001', '1.0',
    'Atorvastatin Calcium 10mg Tablets Specification',
    'Drug Product', 'PRD-ATV-001', 'Atorvastatin Calcium 10mg Tablets',
    'MAT-ATV-DS-001', 'Atorvastatin Calcium',
    'SITE-NJ01', 'New Jersey Manufacturing Site',
    'US', 'Tablet', '10 mg', 'Active',
    '2024-01-15', NULL, '2024-01-10',
    'J.Smith', '3.2.P.5.1', 'Commercial', 'SP-DP-ATV-000', 'USP',
    '2024-01-15', '2024-01-15', 'system', NULL
);

INSERT INTO l1_raw.raw_lims_specification VALUES (
    'ING-002', 'LIMS', 'lims_spec_export_20240115.json', 'BATCH-20240115-01',
    current_timestamp(), 'def456hash',
    'LIMS-SP-DS-001', 'SP-DS-ATV-001', '2.0',
    'Atorvastatin Calcium Drug Substance Specification',
    'API', 'PRD-ATV-DS', 'Atorvastatin Calcium (API)',
    'MAT-ATV-DS-001', 'Atorvastatin Calcium',
    'SITE-IN01', 'India API Manufacturing Site',
    'US', NULL, NULL, 'Active',
    '2024-01-15', NULL, '2024-01-08',
    'R.Patel', '3.2.S.4.1', 'Commercial', 'SP-DS-ATV-001V1', 'USP',
    '2024-01-15', '2024-01-15', 'system', NULL
);

-- L1: Specification Items -----------------------------------------------
-- Drug Product spec items
INSERT INTO l1_raw.raw_lims_spec_item VALUES (
    'ING-003', 'LIMS', 'BATCH-20240115-01', current_timestamp(), 'hash-item-001',
    'ITEM-001', 'LIMS-SP-DP-001', 'TM-HPLC-ATV-001',
    'ASS-001', 'Assay', 'ATV', 'Atorvastatin',
    'Chemical', 'Assay', '%', 'CQA', '1',
    'Numeric', '1', 'TRUE', 'USP <621>', 'Release',
    '2024-01-15', '2024-01-15'
);

INSERT INTO l1_raw.raw_lims_spec_item VALUES (
    'ING-004', 'LIMS', 'BATCH-20240115-01', current_timestamp(), 'hash-item-002',
    'ITEM-002', 'LIMS-SP-DP-001', 'TM-VIS-001',
    'APP-001', 'Appearance', NULL, 'Description',
    'Physical', NULL, NULL, 'NCQA', '2',
    'Pass-Fail', NULL, 'TRUE', NULL, 'Release',
    '2024-01-15', '2024-01-15'
);

INSERT INTO l1_raw.raw_lims_spec_item VALUES (
    'ING-005', 'LIMS', 'BATCH-20240115-01', current_timestamp(), 'hash-item-003',
    'ITEM-003', 'LIMS-SP-DP-001', 'TM-HPLC-IMP-001',
    'IMP-001', 'Related Substances', 'IMP-A', 'Impurity A',
    'Impurity', 'Related Substances', '%', 'CQA', '3',
    'Numeric', '2', 'TRUE', 'USP <621>', 'Both',
    '2024-01-15', '2024-01-15'
);

INSERT INTO l1_raw.raw_lims_spec_item VALUES (
    'ING-006', 'LIMS', 'BATCH-20240115-01', current_timestamp(), 'hash-item-004',
    'ITEM-004', 'LIMS-SP-DP-001', 'TM-DIS-001',
    'DIS-001', 'Dissolution', 'ATV', 'Atorvastatin',
    'Physical', 'Dissolution', '%', 'CQA', '4',
    'Numeric', '0', 'TRUE', 'USP <711>', 'Release',
    '2024-01-15', '2024-01-15'
);

-- L1: Specification Limits -----------------------------------------------
-- Assay — Acceptance Criteria (Release)
INSERT INTO l1_raw.raw_lims_spec_limit VALUES (
    'ING-007', 'LIMS', 'BATCH-20240115-01', current_timestamp(), 'hash-lim-001',
    'LIM-001', 'ITEM-001', 'LIMS-SP-DP-001',
    'Acceptance', 'Between', '95.0', '105.0', '100.0',
    NULL, '%', 'As Labeled', 'Release', NULL, NULL,
    '2024-01-15', NULL, NULL, NULL, NULL,
    'TRUE', 'ICH Q6A', '2024-01-15', '2024-01-15'
);

-- Assay — Normal Operating Range (Internal)
INSERT INTO l1_raw.raw_lims_spec_limit VALUES (
    'ING-008', 'LIMS', 'BATCH-20240115-01', current_timestamp(), 'hash-lim-002',
    'LIM-002', 'ITEM-001', 'LIMS-SP-DP-001',
    'NOR', 'Between', '98.0', '102.0', '100.0',
    NULL, '%', 'As Labeled', 'Release', NULL, NULL,
    '2024-01-15', NULL, NULL, NULL, NULL,
    'FALSE', NULL, '2024-01-15', '2024-01-15'
);

-- Assay — Proven Acceptable Range (Design Space)
INSERT INTO l1_raw.raw_lims_spec_limit VALUES (
    'ING-009', 'LIMS', 'BATCH-20240115-01', current_timestamp(), 'hash-lim-003',
    'LIM-003', 'ITEM-001', 'LIMS-SP-DP-001',
    'Proven Acceptable Range', 'Between', '92.0', '108.0', '100.0',
    NULL, '%', 'As Labeled', 'Release', NULL, NULL,
    '2024-01-15', NULL, NULL, NULL, NULL,
    'TRUE', 'ICH Q8', '2024-01-15', '2024-01-15'
);

-- Assay — Alert Limit (SPC-derived)
INSERT INTO l1_raw.raw_lims_spec_limit VALUES (
    'ING-010', 'LIMS', 'BATCH-20240115-01', current_timestamp(), 'hash-lim-004',
    'LIM-004', 'ITEM-001', 'LIMS-SP-DP-001',
    'Alert', 'Between', '98.5', '101.5', '100.0',
    NULL, '%', 'As Labeled', 'Release', NULL, NULL,
    '2024-01-15', NULL, '3 Sigma', '120', '2024-01-01',
    'FALSE', NULL, '2024-01-15', '2024-01-15'
);

-- Appearance — Pass/Fail limit
INSERT INTO l1_raw.raw_lims_spec_limit VALUES (
    'ING-011', 'LIMS', 'BATCH-20240115-01', current_timestamp(), 'hash-lim-005',
    'LIM-005', 'ITEM-002', 'LIMS-SP-DP-001',
    'Acceptance', NULL, NULL, NULL, NULL,
    'White to off-white, round, biconvex, film-coated tablets', NULL, NULL, 'Release', NULL, NULL,
    '2024-01-15', NULL, NULL, NULL, NULL,
    'TRUE', NULL, '2024-01-15', '2024-01-15'
);

-- Impurity A — AC (Release)
INSERT INTO l1_raw.raw_lims_spec_limit VALUES (
    'ING-012', 'LIMS', 'BATCH-20240115-01', current_timestamp(), 'hash-lim-006',
    'LIM-006', 'ITEM-003', 'LIMS-SP-DP-001',
    'Acceptance', 'Not More Than', NULL, '0.10', NULL,
    NULL, '%', NULL, 'Release', NULL, NULL,
    '2024-01-15', NULL, NULL, NULL, NULL,
    'TRUE', 'ICH Q3B', '2024-01-15', '2024-01-15'
);

-- Dissolution — AC (Release: Q=75% in 30 min)
INSERT INTO l1_raw.raw_lims_spec_limit VALUES (
    'ING-013', 'LIMS', 'BATCH-20240115-01', current_timestamp(), 'hash-lim-007',
    'LIM-007', 'ITEM-004', 'LIMS-SP-DP-001',
    'Acceptance', 'Not Less Than', '75', NULL, NULL,
    NULL, '%', NULL, 'Release', NULL, NULL,
    '2024-01-15', NULL, NULL, NULL, NULL,
    'TRUE', 'USP <711>', '2024-01-15', '2024-01-15'
);

-- =============================================================================
-- SECTION 2: L2.2 DIMENSION SEED DATA
-- Note: Identity (surrogate key) columns are excluded from INSERT column lists.
--       Databricks GENERATED ALWAYS AS IDENTITY does not support DEFAULT keyword.
-- =============================================================================

USE SCHEMA l2_2_spec_unified;

-- Sites (dim_site: site_key is identity — excluded from column list)
INSERT INTO l2_2_spec_unified.dim_site
    (site_id, site_code, site_name, site_type,
     address_line, city, state_province, country_code, country_name,
     regulatory_region, gmp_status, gmp_status_date,
     last_inspection_date, last_inspection_outcome,
     source_system_code, source_system_id, load_timestamp, is_current)
VALUES
    ('SITE-NJ01', 'NJ01', 'New Jersey Manufacturing Site', 'MANUFACTURING',
     '1 Pharma Way', 'New Brunswick', 'New Jersey', 'US', 'United States',
     'FDA', 'APPROVED', '2023-08-01', '2022-11-15', 'VAI',
     'MDM', 'SITE-NJ01', current_timestamp(), TRUE);

INSERT INTO l2_2_spec_unified.dim_site
    (site_id, site_code, site_name, site_type,
     address_line, city, state_province, country_code, country_name,
     regulatory_region, gmp_status, gmp_status_date,
     last_inspection_date, last_inspection_outcome,
     source_system_code, source_system_id, load_timestamp, is_current)
VALUES
    ('SITE-IN01', 'IN01', 'India API Manufacturing Site', 'MANUFACTURING',
     '42 API Park', 'Hyderabad', 'Telangana', 'IN', 'India',
     'FDA', 'APPROVED', '2024-03-01', '2023-09-10', 'NAI',
     'MDM', 'SITE-IN01', current_timestamp(), TRUE);

-- Market (dim_market: market_key is identity)
INSERT INTO l2_2_spec_unified.dim_market
    (market_id, country_code, country_name, region_code, region_name,
     regulatory_body, market_status, marketing_auth_number,
     marketing_auth_date, marketing_auth_expiry_date, primary_pharmacopoeia,
     source_system_code, source_system_id, load_timestamp, is_current)
VALUES
    ('MKT-US-001', 'US', 'United States', 'US', 'United States',
     'FDA', 'APPROVED', 'NDA-021366', '2003-12-17', NULL, 'USP',
     'MDM', 'MKT-US-001', current_timestamp(), TRUE);

-- Product (dim_product: product_key is identity)
INSERT INTO l2_2_spec_unified.dim_product
    (product_id, product_code, product_name, inn_name, brand_name,
     dosage_form_code, dosage_form_name, route_of_administration,
     strength, strength_value, strength_uom,
     therapeutic_area, atc_code, nda_number, product_status,
     source_system_code, load_timestamp, is_current)
VALUES
    ('PRD-ATV-001', 'ATV-010-TAB', 'Atorvastatin Calcium 10mg Tablets',
     'Atorvastatin', 'Lipitor', 'TAB', 'Tablet', 'ORAL',
     '10 mg', 10.0000, 'mg',
     'Cardiovascular', 'C10AA05', 'NDA-021366', 'APPROVED',
     'MDM', current_timestamp(), TRUE);

-- Material (dim_material: material_key is identity)
INSERT INTO l2_2_spec_unified.dim_material
    (material_id, material_code, material_name,
     material_type_code, material_type_name,
     cas_number, molecular_formula, molecular_weight,
     structural_formula, pharmacopoeia_grade,
     manufacturer_name, manufacturer_code,
     source_system_code, load_timestamp, is_current)
VALUES
    ('MAT-ATV-DS-001', 'ATV-DS-001', 'Atorvastatin Calcium',
     'API', 'Active Pharmaceutical Ingredient',
     '134523-00-5', 'C66H68CaF2N4O10', 1209.4200,
     NULL, 'USP', 'Sun Pharma', 'SUNP-001',
     'MDM', current_timestamp(), TRUE);

-- Test Methods (dim_test_method: test_method_key is identity)
INSERT INTO l2_2_spec_unified.dim_test_method
    (test_method_id, test_method_number, test_method_name, test_method_version,
     method_type_code, method_type_name, analytical_technique, instrument_type,
     compendia_reference, validation_status, validation_date,
     source_system_code, source_system_id, load_timestamp, is_current)
VALUES
    ('TM-HPLC-ATV-001', 'TM-HPLC-ATV-001', 'Assay of Atorvastatin by HPLC', '2.1',
     'INHOUSE', 'In-House', 'HPLC', 'HPLC System (C18 Column)',
     'USP <621>', 'VALIDATED', '2022-06-15',
     'LIMS', 'TM-HPLC-ATV-001', current_timestamp(), TRUE);

INSERT INTO l2_2_spec_unified.dim_test_method
    (test_method_id, test_method_number, test_method_name, test_method_version,
     method_type_code, method_type_name, analytical_technique, instrument_type,
     compendia_reference, validation_status, validation_date,
     source_system_code, source_system_id, load_timestamp, is_current)
VALUES
    ('TM-VIS-001', 'TM-VIS-001', 'Visual Inspection', '1.0',
     'INHOUSE', 'In-House', 'VISUAL', 'Visual',
     NULL, 'WAIVED', NULL,
     'LIMS', 'TM-VIS-001', current_timestamp(), TRUE);

INSERT INTO l2_2_spec_unified.dim_test_method
    (test_method_id, test_method_number, test_method_name, test_method_version,
     method_type_code, method_type_name, analytical_technique, instrument_type,
     compendia_reference, validation_status, validation_date,
     source_system_code, source_system_id, load_timestamp, is_current)
VALUES
    ('TM-HPLC-IMP-001', 'TM-HPLC-IMP-001', 'Related Substances by HPLC', '1.5',
     'INHOUSE', 'In-House', 'HPLC', 'HPLC System (C18 Column)',
     'USP <621>', 'VALIDATED', '2022-09-01',
     'LIMS', 'TM-HPLC-IMP-001', current_timestamp(), TRUE);

INSERT INTO l2_2_spec_unified.dim_test_method
    (test_method_id, test_method_number, test_method_name, test_method_version,
     method_type_code, method_type_name, analytical_technique, instrument_type,
     compendia_reference, validation_status, validation_date,
     source_system_code, source_system_id, load_timestamp, is_current)
VALUES
    ('TM-DIS-001', 'TM-DIS-001', 'Dissolution Testing USP Apparatus II', '1.0',
     'COMP', 'Compendial', 'DISSOLUTION', 'USP Apparatus II (Paddle)',
     'USP <711>', 'VERIFIED', '2023-01-10',
     'LIMS', 'TM-DIS-001', current_timestamp(), TRUE);

-- Regulatory Context already seeded by dim_regulatory_context.sql DDL (9 rows).
-- US-NDA = regulatory_context_key 1 (first row in seed INSERT).

-- =============================================================================
-- SECTION 3: L2.2 STAR SCHEMA — SPECIFICATION + ITEMS + LIMITS
-- Note: Identity (surrogate key) columns excluded from all INSERT column lists.
--       spec_key 1 = DP spec, spec_key 2 = DS spec (identity generation order).
--       spec_item_key 1-4 = Assay, Appearance, ImpA, Dissolution (DP).
-- =============================================================================

-- dim_specification: Drug Product (DP)
-- regulatory_context_key 1 = US-NDA (first row in dim_regulatory_context seed)
INSERT INTO l2_2_spec_unified.dim_specification
    (spec_id, spec_number, spec_version, spec_title,
     spec_type_code, spec_type_name,
     product_key, material_key, site_key, market_key, regulatory_context_key,
     ctd_section, stage_code, stage_name, status_code, status_name,
     effective_start_date, effective_end_date, approval_date,
     approver_name, approver_title, compendia_reference, supersedes_spec_id,
     source_system_code, source_system_id, load_timestamp, is_current, valid_from, valid_to)
VALUES
    ('LIMS-SP-DP-001', 'SP-DP-ATV-001', '1.0',
     'Atorvastatin Calcium 10mg Tablets Specification',
     'DP', 'Drug Product',
     1, 1,    -- product_key (ATV 10mg Tabs), material_key (ATV DS)
     1,       -- site_key (NJ01)
     1,       -- market_key (US)
     1,       -- regulatory_context_key (US-NDA)
     '3.2.P.5.1', 'COM', 'Commercial', 'APP', 'Approved',
     '2024-01-15', NULL, '2024-01-10',
     'J.Smith', 'QA Director', 'USP',
     'SP-DP-ATV-000', 'LIMS', 'LIMS-SP-DP-001',
     current_timestamp(), TRUE, current_timestamp(), NULL);

-- dim_specification: Drug Substance (DS)
INSERT INTO l2_2_spec_unified.dim_specification
    (spec_id, spec_number, spec_version, spec_title,
     spec_type_code, spec_type_name,
     product_key, material_key, site_key, market_key, regulatory_context_key,
     ctd_section, stage_code, stage_name, status_code, status_name,
     effective_start_date, effective_end_date, approval_date,
     approver_name, approver_title, compendia_reference, supersedes_spec_id,
     source_system_code, source_system_id, load_timestamp, is_current, valid_from, valid_to)
VALUES
    ('LIMS-SP-DS-001', 'SP-DS-ATV-001', '2.0',
     'Atorvastatin Calcium Drug Substance Specification',
     'DS', 'Drug Substance',
     NULL, 1,  -- no product_key for DS; material_key = ATV DS
     2,        -- site_key (IN01)
     1,        -- market_key (US)
     1,        -- regulatory_context_key (US-NDA)
     '3.2.S.4.1', 'COM', 'Commercial', 'APP', 'Approved',
     '2024-01-15', NULL, '2024-01-08',
     'R.Patel', 'QA Head', 'USP',
     'SP-DS-ATV-001V1', 'LIMS', 'LIMS-SP-DS-001',
     current_timestamp(), TRUE, current_timestamp(), NULL);

-- dim_specification_item: 4 tests for Drug Product (spec_key=1)
-- test_method_key: 1=HPLC-ATV, 2=Visual, 3=HPLC-IMP, 4=Dissolution
-- uom_key: 1=% (Percent), NULL=no unit

INSERT INTO l2_2_spec_unified.dim_specification_item
    (spec_item_id, spec_key, test_method_key, uom_key,
     test_code, test_name, test_description,
     test_category_code, test_category_name, test_subcategory,
     analyte_code, criticality, sequence_number,
     is_required, reporting_type, result_precision,
     compendia_test_ref, is_compendial, stage_applicability,
     source_system_code, source_system_id, load_timestamp, is_current)
VALUES
    ('ITEM-001', 1, 1, 1,
     'ASS-001', 'Assay', 'Assay of Atorvastatin Calcium',
     'CHE', 'Chemical', 'Assay',
     'ATV', 'CQA', 1,
     TRUE, 'NUMERIC', 1,
     NULL, TRUE, 'RELEASE',
     'LIMS', 'ITEM-001', current_timestamp(), TRUE);

INSERT INTO l2_2_spec_unified.dim_specification_item
    (spec_item_id, spec_key, test_method_key, uom_key,
     test_code, test_name, test_description,
     test_category_code, test_category_name, test_subcategory,
     analyte_code, criticality, sequence_number,
     is_required, reporting_type, result_precision,
     compendia_test_ref, is_compendial, stage_applicability,
     source_system_code, source_system_id, load_timestamp, is_current)
VALUES
    ('ITEM-002', 1, 2, NULL,
     'APP-001', 'Appearance', 'Visual description of tablet',
     'PHY', 'Physical', NULL,
     NULL, 'NCQA', 2,
     TRUE, 'PASS_FAIL', NULL,
     NULL, TRUE, 'RELEASE',
     'LIMS', 'ITEM-002', current_timestamp(), TRUE);

INSERT INTO l2_2_spec_unified.dim_specification_item
    (spec_item_id, spec_key, test_method_key, uom_key,
     test_code, test_name, test_description,
     test_category_code, test_category_name, test_subcategory,
     analyte_code, criticality, sequence_number,
     is_required, reporting_type, result_precision,
     compendia_test_ref, is_compendial, stage_applicability,
     source_system_code, source_system_id, load_timestamp, is_current)
VALUES
    ('ITEM-003', 1, 3, 1,
     'IMP-001', 'Related Substances', 'Impurity A (individual)',
     'IMP', 'Impurity', 'Related Substances',
     'IMP-A', 'CQA', 3,
     TRUE, 'NUMERIC', 2,
     NULL, TRUE, 'BOTH',
     'LIMS', 'ITEM-003', current_timestamp(), TRUE);

INSERT INTO l2_2_spec_unified.dim_specification_item
    (spec_item_id, spec_key, test_method_key, uom_key,
     test_code, test_name, test_description,
     test_category_code, test_category_name, test_subcategory,
     analyte_code, criticality, sequence_number,
     is_required, reporting_type, result_precision,
     compendia_test_ref, is_compendial, stage_applicability,
     source_system_code, source_system_id, load_timestamp, is_current)
VALUES
    ('ITEM-004', 1, 4, 1,
     'DIS-001', 'Dissolution', 'Dissolution in pH 6.8 phosphate buffer',
     'PHY', 'Physical', 'Dissolution',
     'ATV', 'CQA', 4,
     TRUE, 'NUMERIC', 0,
     'USP <711>', TRUE, 'RELEASE',
     'LIMS', 'ITEM-004', current_timestamp(), TRUE);

-- fact_specification_limit: normalized limits for all 4 DP tests
-- limit_type_key: 1=AC, 2=PAR, 4=NOR, 5=ALERT
-- uom_key: 1=% (Percent)
-- spec_item_key: 1=Assay, 2=Appearance, 3=ImpA, 4=Dissolution

-- Assay — Acceptance Criteria (is_in_filing=TRUE, regulatory_basis=ICH Q6A)
INSERT INTO l2_2_spec_unified.fact_specification_limit
    (spec_key, spec_item_key, limit_type_key, uom_key,
     lower_limit_value, upper_limit_value, target_value,
     lower_limit_operator, upper_limit_operator,
     limit_text, limit_description, limit_basis, stage_code,
     stability_time_point, stability_condition,
     is_conditional, condition_description,
     regulatory_basis, is_in_filing,
     source_system_code, source_system_id,
     effective_date, effective_end_date,
     calculation_method, sample_size, last_calculated_date,
     load_timestamp, is_current)
VALUES
    (1, 1, 1, 1,
     95.0, 105.0, 100.0,
     'NLT', 'NMT',
     NULL, 'NLT 95.0% and NMT 105.0% (on as-labeled basis)', 'AS_LABELED', 'RELEASE',
     NULL, NULL,
     FALSE, NULL,
     'ICH Q6A', TRUE,
     'LIMS', 'LIM-001',
     '2024-01-15', NULL,
     NULL, NULL, NULL,
     current_timestamp(), TRUE);

-- Assay — Normal Operating Range (is_in_filing=FALSE — internal range)
INSERT INTO l2_2_spec_unified.fact_specification_limit
    (spec_key, spec_item_key, limit_type_key, uom_key,
     lower_limit_value, upper_limit_value, target_value,
     lower_limit_operator, upper_limit_operator,
     limit_text, limit_description, limit_basis, stage_code,
     stability_time_point, stability_condition,
     is_conditional, condition_description,
     regulatory_basis, is_in_filing,
     source_system_code, source_system_id,
     effective_date, effective_end_date,
     calculation_method, sample_size, last_calculated_date,
     load_timestamp, is_current)
VALUES
    (1, 1, 4, 1,
     98.0, 102.0, 100.0,
     'NLT', 'NMT',
     NULL, 'NLT 98.0% and NMT 102.0%', 'AS_LABELED', 'RELEASE',
     NULL, NULL,
     FALSE, NULL,
     NULL, FALSE,
     'LIMS', 'LIM-002',
     '2024-01-15', NULL,
     NULL, NULL, NULL,
     current_timestamp(), TRUE);

-- Assay — Proven Acceptable Range (is_in_filing=TRUE, regulatory_basis=ICH Q8)
INSERT INTO l2_2_spec_unified.fact_specification_limit
    (spec_key, spec_item_key, limit_type_key, uom_key,
     lower_limit_value, upper_limit_value, target_value,
     lower_limit_operator, upper_limit_operator,
     limit_text, limit_description, limit_basis, stage_code,
     stability_time_point, stability_condition,
     is_conditional, condition_description,
     regulatory_basis, is_in_filing,
     source_system_code, source_system_id,
     effective_date, effective_end_date,
     calculation_method, sample_size, last_calculated_date,
     load_timestamp, is_current)
VALUES
    (1, 1, 2, 1,
     92.0, 108.0, 100.0,
     'NLT', 'NMT',
     NULL, 'NLT 92.0% and NMT 108.0%', 'AS_LABELED', 'RELEASE',
     NULL, NULL,
     FALSE, NULL,
     'ICH Q8', TRUE,
     'LIMS', 'LIM-003',
     '2024-01-15', NULL,
     NULL, NULL, NULL,
     current_timestamp(), TRUE);

-- Assay — Alert Limit (SPC-derived: 3-sigma, n=120; is_in_filing=FALSE)
INSERT INTO l2_2_spec_unified.fact_specification_limit
    (spec_key, spec_item_key, limit_type_key, uom_key,
     lower_limit_value, upper_limit_value, target_value,
     lower_limit_operator, upper_limit_operator,
     limit_text, limit_description, limit_basis, stage_code,
     stability_time_point, stability_condition,
     is_conditional, condition_description,
     regulatory_basis, is_in_filing,
     source_system_code, source_system_id,
     effective_date, effective_end_date,
     calculation_method, sample_size, last_calculated_date,
     load_timestamp, is_current)
VALUES
    (1, 1, 5, 1,
     98.5, 101.5, 100.0,
     'NLT', 'NMT',
     NULL, 'Alert: NLT 98.5% and NMT 101.5% (3-sigma, n=120)', 'AS_LABELED', 'RELEASE',
     NULL, NULL,
     FALSE, NULL,
     NULL, FALSE,
     'LIMS', 'LIM-004',
     '2024-01-15', NULL,
     '3_SIGMA', 120, '2024-01-01',
     current_timestamp(), TRUE);

-- Appearance — Acceptance Criteria (Pass/Fail; no uom; is_in_filing=TRUE)
INSERT INTO l2_2_spec_unified.fact_specification_limit
    (spec_key, spec_item_key, limit_type_key, uom_key,
     lower_limit_value, upper_limit_value, target_value,
     lower_limit_operator, upper_limit_operator,
     limit_text, limit_description, limit_basis, stage_code,
     stability_time_point, stability_condition,
     is_conditional, condition_description,
     regulatory_basis, is_in_filing,
     source_system_code, source_system_id,
     effective_date, effective_end_date,
     calculation_method, sample_size, last_calculated_date,
     load_timestamp, is_current)
VALUES
    (1, 2, 1, NULL,
     NULL, NULL, NULL,
     'NONE', 'NONE',
     'White to off-white, round, biconvex, film-coated tablets',
     'White to off-white, round, biconvex, film-coated tablets',
     NULL, 'RELEASE',
     NULL, NULL,
     FALSE, NULL,
     NULL, TRUE,
     'LIMS', 'LIM-005',
     '2024-01-15', NULL,
     NULL, NULL, NULL,
     current_timestamp(), TRUE);

-- Impurity A — Acceptance Criteria (NMT 0.10%; is_in_filing=TRUE)
INSERT INTO l2_2_spec_unified.fact_specification_limit
    (spec_key, spec_item_key, limit_type_key, uom_key,
     lower_limit_value, upper_limit_value, target_value,
     lower_limit_operator, upper_limit_operator,
     limit_text, limit_description, limit_basis, stage_code,
     stability_time_point, stability_condition,
     is_conditional, condition_description,
     regulatory_basis, is_in_filing,
     source_system_code, source_system_id,
     effective_date, effective_end_date,
     calculation_method, sample_size, last_calculated_date,
     load_timestamp, is_current)
VALUES
    (1, 3, 1, 1,
     NULL, 0.10, NULL,
     'NONE', 'NMT',
     NULL, 'NMT 0.10%', NULL, 'RELEASE',
     NULL, NULL,
     FALSE, NULL,
     'ICH Q3B', TRUE,
     'LIMS', 'LIM-006',
     '2024-01-15', NULL,
     NULL, NULL, NULL,
     current_timestamp(), TRUE);

-- Dissolution — Acceptance Criteria (Q=NLT 75% in 30 min; is_in_filing=TRUE)
INSERT INTO l2_2_spec_unified.fact_specification_limit
    (spec_key, spec_item_key, limit_type_key, uom_key,
     lower_limit_value, upper_limit_value, target_value,
     lower_limit_operator, upper_limit_operator,
     limit_text, limit_description, limit_basis, stage_code,
     stability_time_point, stability_condition,
     is_conditional, condition_description,
     regulatory_basis, is_in_filing,
     source_system_code, source_system_id,
     effective_date, effective_end_date,
     calculation_method, sample_size, last_calculated_date,
     load_timestamp, is_current)
VALUES
    (1, 4, 1, 1,
     75.0, NULL, NULL,
     'NLT', 'NONE',
     NULL, 'Q = NLT 75% in 30 minutes (USP Apparatus II)', NULL, 'RELEASE',
     NULL, NULL,
     FALSE, NULL,
     'USP <711>', TRUE,
     'LIMS', 'LIM-007',
     '2024-01-15', NULL,
     NULL, NULL, NULL,
     current_timestamp(), TRUE);

-- =============================================================================
-- SECTION 4: VALIDATION QUERIES — verify end-to-end
-- =============================================================================

-- V1: Count per layer
SELECT 'L1 raw_lims_specification'   AS layer_table, COUNT(*) AS rows FROM l1_raw.raw_lims_specification
UNION ALL
SELECT 'L1 raw_lims_spec_item',      COUNT(*) FROM l1_raw.raw_lims_spec_item
UNION ALL
SELECT 'L1 raw_lims_spec_limit',     COUNT(*) FROM l1_raw.raw_lims_spec_limit
UNION ALL
SELECT 'L2.2 dim_specification',     COUNT(*) FROM l2_2_spec_unified.dim_specification
UNION ALL
SELECT 'L2.2 dim_specification_item',COUNT(*) FROM l2_2_spec_unified.dim_specification_item
UNION ALL
SELECT 'L2.2 fact_specification_limit',COUNT(*) FROM l2_2_spec_unified.fact_specification_limit
UNION ALL
SELECT 'L2.2 dim_site',              COUNT(*) FROM l2_2_spec_unified.dim_site
UNION ALL
SELECT 'L2.2 dim_market',            COUNT(*) FROM l2_2_spec_unified.dim_market;

-- V2: Star schema join — spec + items + AC limits (simulates CTD table)
SELECT
    s.spec_number,
    s.spec_version,
    s.spec_type_code,
    s.ctd_section,
    s.effective_start_date,
    st.site_name,
    st.regulatory_region,
    mk.country_name,
    mk.market_status,
    i.sequence_number,
    i.test_name,
    i.analyte_code,          -- NEW from ref ERD
    i.criticality,           -- NEW from ref ERD
    i.test_category_name,
    lt.limit_type_code,
    lt.is_regulatory,
    f.lower_limit_value,
    f.upper_limit_value,
    f.lower_limit_operator,
    f.upper_limit_operator,
    f.limit_text,
    f.limit_description,
    f.effective_date        AS limit_effective_from,
    f.effective_end_date    AS limit_effective_to,  -- NEW from ref ERD
    f.calculation_method,                           -- NEW from ref ERD (SPC)
    f.sample_size,                                  -- NEW from ref ERD (SPC)
    u.uom_code
FROM l2_2_spec_unified.dim_specification s
JOIN l2_2_spec_unified.dim_specification_item i
    ON s.spec_key = i.spec_key AND i.is_current = TRUE
JOIN l2_2_spec_unified.fact_specification_limit f
    ON i.spec_item_key = f.spec_item_key AND f.is_current = TRUE
JOIN l2_2_spec_unified.dim_limit_type lt
    ON f.limit_type_key = lt.limit_type_key
LEFT JOIN l2_2_spec_unified.dim_site st
    ON s.site_key = st.site_key AND st.is_current = TRUE
LEFT JOIN l2_2_spec_unified.dim_market mk
    ON s.market_key = mk.market_key AND mk.is_current = TRUE
LEFT JOIN l2_2_spec_unified.dim_uom u
    ON f.uom_key = u.uom_key
WHERE s.is_current = TRUE
  AND s.spec_type_code = 'DP'
ORDER BY i.sequence_number, lt.sort_order;

-- V3: PAR >= AC >= NOR hierarchy check
SELECT
    i.test_name,
    i.criticality,
    MAX(CASE WHEN lt.limit_type_code = 'PAR'   THEN f.lower_limit_value END) AS par_lower,
    MAX(CASE WHEN lt.limit_type_code = 'AC'    THEN f.lower_limit_value END) AS ac_lower,
    MAX(CASE WHEN lt.limit_type_code = 'NOR'   THEN f.lower_limit_value END) AS nor_lower,
    MAX(CASE WHEN lt.limit_type_code = 'ALERT' THEN f.lower_limit_value END) AS alert_lower,
    MAX(CASE WHEN lt.limit_type_code = 'ALERT' THEN f.calculation_method END) AS spc_method,  -- NEW
    MAX(CASE WHEN lt.limit_type_code = 'ALERT' THEN f.sample_size       END) AS spc_n,        -- NEW
    MAX(CASE WHEN lt.limit_type_code = 'PAR'   THEN f.upper_limit_value END) AS par_upper,
    MAX(CASE WHEN lt.limit_type_code = 'AC'    THEN f.upper_limit_value END) AS ac_upper,
    MAX(CASE WHEN lt.limit_type_code = 'NOR'   THEN f.upper_limit_value END) AS nor_upper,
    CASE WHEN
        MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value END) <=
        MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END)
        AND
        MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END) <=
        MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value END)
        AND
        MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value END) >=
        MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END)
        AND
        MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END) >=
        MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value END)
    THEN 'PASS' ELSE 'FAIL' END AS hierarchy_check
FROM l2_2_spec_unified.fact_specification_limit f
JOIN l2_2_spec_unified.dim_specification_item i ON f.spec_item_key = i.spec_item_key
JOIN l2_2_spec_unified.dim_limit_type lt ON f.limit_type_key = lt.limit_type_key
WHERE f.is_current = TRUE AND f.stage_code = 'RELEASE'
GROUP BY i.spec_item_key, i.test_name, i.criticality;
