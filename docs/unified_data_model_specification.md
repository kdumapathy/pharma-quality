# Unified Data Model Specification — Pharmaceutical Quality (Specifications Domain)

**Version:** 1.0
**Domain:** Pharmaceutical Quality — Specifications
**Primary Consumption:** Regulatory Filing (CTD Module 3)
**Platform:** Databricks (Delta Lake, Unity Catalog)
**Architecture:** Medallion / Layered (L1 → L2.1 → L2.2 → L3)

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture & Layer Definitions](#2-architecture--layer-definitions)
3. [Data Sources](#3-data-sources)
4. [L2.2 Unified Data Model — Star Schema](#4-l22-unified-data-model--star-schema)
   - 4.1 [Entity Relationship Diagram](#41-entity-relationship-diagram)
   - 4.2 [Dimension Tables](#42-dimension-tables)
   - 4.3 [Fact Tables](#43-fact-tables)
5. [L2.2 Denormalized / Semi-Normalized Tables](#5-l22-denormalized--semi-normalized-tables)
6. [L3 Final Data Products (OBT)](#6-l3-final-data-products-obt)
7. [Key Business Rules & Definitions](#7-key-business-rules--definitions)
8. [CTD Section Mapping](#8-ctd-section-mapping)
9. [Naming Conventions](#9-naming-conventions)
10. [Partition & Optimization Strategy](#10-partition--optimization-strategy)
11. [Data Lineage Summary](#11-data-lineage-summary)

---

## 1. Overview

This document specifies the **Unified Data Model (UDM)** for the Pharmaceutical Quality — Specifications domain. The model integrates specification data from multiple source systems (LIMS, ERP, Document Management, regulatory submission tools) into a single, harmonized data product optimized for:

- **Regulatory filing** (CTD Common Technical Document, Modules 3.2.S.4 and 3.2.P.5)
- **Quality control** and release testing
- **Stability program** tracking
- **Process analytical technology (PAT)** and control strategy alignment
- **Audit and version management** (specification lifecycle)

### Specification Domain Scope

A **specification** is a formal document that establishes the criteria to which a drug substance, drug product, intermediate, raw material, or other material must conform. This model captures:

| Entity | Description |
|--------|-------------|
| Specification Header | Identity, version, regulatory context, status |
| Specification Item | Individual tests (Assay, Identity, Dissolution, etc.) |
| Specification Limits | All limit types: Acceptance Criteria, NOR, PAR, Alert, Action |
| Acceptance Criteria | Regulatory-filed limits with full operator semantics |
| Denormalized View | Item + all limits pivoted (release-ready flat structure) |

---

## 2. Architecture & Layer Definitions

```
┌─────────────────────────────────────────────────────────────────────┐
│  SOURCE SYSTEMS                                                      │
│  LIMS (LabWare, Labvantage)  │  SAP QM  │  Vault  │  Manual (Excel) │
└────────────────┬────────────────────────────────────────────────────┘
                 │ Raw ingestion (no transformation)
                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  L1 — RAW LAYER                                                      │
│  Schema: l1_raw                                                      │
│  • Exact copy of source data in Delta format                         │
│  • Immutable, append-only                                            │
│  • Metadata: source_system, load_timestamp, file_name, batch_id     │
│  • No schema enforcement on arrival (schema-on-read)                 │
└────────────────┬────────────────────────────────────────────────────┘
                 │ Source-specific cleansing, typing, conforming
                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  L2.1 — SOURCE CONFORM LAYER                                         │
│  Schema: l2_1_<source_system>  (e.g., l2_1_lims, l2_1_sap)         │
│  • Per-source-system clean and typed tables                          │
│  • Source business rules applied                                     │
│  • Standardized data types, null handling, deduplication            │
│  • Source-native keys preserved                                      │
└────────────────┬────────────────────────────────────────────────────┘
                 │ Cross-source integration, harmonization, MDM
                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  L2.2 — UNIFIED DATA MODEL (Business Conform Layer)                  │
│  Schema: l2_2_spec_unified                                           │
│  • Dimensional / Star Schema (normalized)                            │
│  • Semi-denormalized tables for analytical patterns                  │
│  • SCD Type 2 for specification versioning                           │
│  • Cross-source harmonized keys (surrogate keys)                     │
│  • Business glossary applied (limit types, test categories)          │
└────────────────┬────────────────────────────────────────────────────┘
                 │ Aggregation, flattening, CTD alignment
                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  L3 — FINAL DATA PRODUCT LAYER                                       │
│  Schema: l3_spec_products                                            │
│  • One Big Table (OBT) — full denormalized, CTD-aligned             │
│  • Aggregated summary tables                                         │
│  • Regulatory submission-ready                                       │
│  • Optimized for BI, reporting, API consumption                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Sources

| Source System | Type | Specification Data Provided |
|---------------|------|-----------------------------|
| LabWare LIMS | LIMS | Drug Substance/Product specs, test items, limits, methods |
| Labvantage LIMS | LIMS | Stability specifications, in-process controls |
| SAP QM | ERP | Material specifications, inspection plans |
| Veeva Vault | DMS | Approved specification documents (PDF/structured) |
| Excel/Manual | Manual | Legacy specs, early development specs |
| Regulatory DB | Reg | Filed specification commitments, NDA/ANDA references |

---

## 4. L2.2 Unified Data Model — Star Schema

**Schema:** `l2_2_spec_unified`
**Catalog:** `pharma_quality_catalog`

### 4.1 Entity Relationship Diagram

```
                          ┌─────────────────┐
                          │  dim_date        │
                          │  (date_key)      │
                          └────────┬─────────┘
                                   │ effective/expiry/approval dates
                                   │
┌───────────────┐    ┌─────────────▼──────────────┐    ┌─────────────────┐
│  dim_product  │    │       dim_specification      │    │dim_regulatory   │
│  (product_key)◄────│  spec_key (PK)              ├────►context          │
└───────────────┘    │  spec_number                │    │(reg_context_key)│
                     │  spec_version               │    └─────────────────┘
┌───────────────┐    │  spec_type_code             │
│  dim_material │    │  stage_code                 │
│(material_key) ◄────│  status_code                │
└───────────────┘    │  ctd_section                │
                     └──────────────┬──────────────┘
                                    │ 1:N
                     ┌──────────────▼──────────────┐
                     │    dim_specification_item     │
                     │  spec_item_key (PK)           │
                     │  spec_key (FK)                ├──────► dim_test_method
                     │  test_code                    │        (method_key)
                     │  test_name                    │
                     │  test_category_code           ├──────► dim_uom
                     │  reporting_type               │        (uom_key)
                     └──────────────┬───────────────┘
                                    │ 1:N
                     ┌──────────────▼───────────────┐
                     │    fact_specification_limit    │◄──── dim_limit_type
                     │  spec_limit_key (PK)          │      (limit_type_key)
                     │  spec_item_key (FK)           │
                     │  spec_key (FK)                │◄──── dim_uom
                     │  limit_type_key (FK)          │      (uom_key)
                     │  lower_limit_value            │
                     │  upper_limit_value            │
                     │  target_value                 │
                     │  lower_limit_operator         │
                     │  upper_limit_operator         │
                     │  limit_text                   │
                     │  limit_description            │
                     │  stage_code                   │
                     │  stability_time_point         │
                     │  stability_condition          │
                     │  is_in_filing                 │
                     └───────────────────────────────┘
```

---

### 4.2 Dimension Tables

#### DIM_SPECIFICATION — Specification Header / Metadata

**Table:** `l2_2_spec_unified.dim_specification`
**Grain:** One row per specification version (SCD Type 2)
**Description:** Captures the header-level attributes of a pharmaceutical specification document — its identity, regulatory context, lifecycle status, and linkage to product/material.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `spec_key` | BIGINT | NOT NULL | Surrogate primary key (auto-increment) |
| `spec_id` | STRING | NOT NULL | Natural/business key from source system |
| `spec_number` | STRING | NOT NULL | Specification document number (e.g., SP-DS-2024-001) |
| `spec_version` | STRING | NOT NULL | Version string (e.g., 1.0, 2.1, 3.0) |
| `spec_title` | STRING | | Full specification title |
| `spec_type_code` | STRING | NOT NULL | DS \| DP \| RM \| EXCIP \| INTERMED \| IPC \| CCS |
| `spec_type_name` | STRING | | Drug Substance \| Drug Product \| Raw Material \| Excipient \| Intermediate \| In-Process Control \| Container Closure |
| `product_key` | BIGINT | | FK → dim_product |
| `material_key` | BIGINT | | FK → dim_material |
| `regulatory_context_key` | BIGINT | | FK → dim_regulatory_context |
| `ctd_section` | STRING | | CTD section reference (e.g., 3.2.S.4.1, 3.2.P.5.1) |
| `stage_code` | STRING | | DEV \| CLI \| COM (Development / Clinical / Commercial) |
| `stage_name` | STRING | | Development \| Clinical \| Commercial |
| `status_code` | STRING | NOT NULL | DRA \| APP \| SUP \| OBS \| ARCH |
| `status_name` | STRING | | Draft \| Approved \| Superseded \| Obsolete \| Archived |
| `effective_date` | DATE | | Date specification became effective |
| `expiry_date` | DATE | | Date specification expires (if applicable) |
| `approval_date` | DATE | | Date of formal approval |
| `approver_name` | STRING | | Approving person name |
| `approver_title` | STRING | | Approving person title/role |
| `site_code` | STRING | | Manufacturing/testing site code |
| `site_name` | STRING | | Manufacturing/testing site name |
| `compendia_reference` | STRING | | Compendia basis (USP, EP, JP, BP) |
| `supersedes_spec_id` | STRING | | Natural key of prior version superseded by this record |
| `source_system_code` | STRING | NOT NULL | Source system identifier (LIMS, SAP, VAULT, MANUAL) |
| `source_system_id` | STRING | | Original record ID in source system |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |
| `is_current` | BOOLEAN | NOT NULL | SCD2 current row flag |
| `valid_from` | TIMESTAMP | NOT NULL | SCD2 validity start |
| `valid_to` | TIMESTAMP | | SCD2 validity end (NULL = current) |

**Slowly Changing Dimension:** Type 2 (full history preserved per version)
**Partition:** `spec_type_code`
**Z-Order:** `spec_number, spec_version`

---

#### DIM_SPECIFICATION_ITEM — Individual Specification Tests

**Table:** `l2_2_spec_unified.dim_specification_item`
**Grain:** One row per test/item per specification version
**Description:** Each specification contains ordered test items (e.g., Appearance, Identification, Assay, Dissolution). This table captures the test metadata without limit values.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `spec_item_key` | BIGINT | NOT NULL | Surrogate primary key |
| `spec_item_id` | STRING | NOT NULL | Natural key from source system |
| `spec_key` | BIGINT | NOT NULL | FK → dim_specification |
| `test_method_key` | BIGINT | | FK → dim_test_method |
| `uom_key` | BIGINT | | FK → dim_uom (primary result unit) |
| `test_code` | STRING | | Internal test code (e.g., ASS-001) |
| `test_name` | STRING | NOT NULL | Test name (e.g., Assay, Appearance, Dissolution) |
| `test_description` | STRING | | Detailed description of the test |
| `test_category_code` | STRING | | PHY \| CHE \| MIC \| BIO \| IMP \| STER |
| `test_category_name` | STRING | | Physical \| Chemical \| Microbiological \| Biological \| Impurity \| Sterility |
| `test_subcategory` | STRING | | e.g., Related Substances, Residual Solvents, Heavy Metals |
| `sequence_number` | INT | | Display/reporting order within specification |
| `is_required` | BOOLEAN | | TRUE = mandatory test; FALSE = conditional |
| `reporting_type` | STRING | NOT NULL | NUMERIC \| PASS_FAIL \| TEXT \| REPORT_ONLY |
| `result_precision` | INT | | Decimal places for numeric result reporting |
| `compendia_test_ref` | STRING | | Compendia test reference (e.g., USP \<711\>, EP 2.9.3) |
| `stage_applicability` | STRING | | RELEASE \| STABILITY \| IPC \| BOTH |
| `is_compendial` | BOOLEAN | | TRUE if test/limit sourced from official compendia |
| `source_system_code` | STRING | NOT NULL | Source system identifier |
| `source_system_id` | STRING | | Original record ID in source system |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |
| `is_current` | BOOLEAN | NOT NULL | SCD2 current row flag |

**Partition:** `test_category_code`
**Z-Order:** `spec_key, test_code`

---

#### DIM_LIMIT_TYPE — Limit Type Reference

**Table:** `l2_2_spec_unified.dim_limit_type`
**Grain:** One row per limit type (static reference table)
**Description:** Classifies the type of each limit value, enabling normalized storage of NOR, PAR, Acceptance Criteria, Alert, Action, and other limit types in a single fact table.

| Code | Name | Description | Is Regulatory |
|------|------|-------------|---------------|
| `AC` | Acceptance Criteria | Regulatory specification limit (in CTD filing) | TRUE |
| `NOR` | Normal Operating Range | Internal tighter operating range to ensure AC compliance | FALSE |
| `PAR` | Proven Acceptable Range | Broader range proven via development; may appear in CTD design space | TRUE (Design Space) |
| `ALERT` | Alert Limit | Internal early warning limit (tighter than action) | FALSE |
| `ACTION` | Action Limit | Internal limit triggering mandatory investigation | FALSE |
| `IPC_LIMIT` | In-Process Control Limit | Limits applied during manufacturing process | FALSE |
| `REPORT` | Report Only | No limit; result reported for information | FALSE |
| `COMPENDIA` | Compendia Limit | Limit as defined by official compendia (USP/EP/JP) | TRUE |

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `limit_type_key` | BIGINT | NOT NULL | Surrogate primary key |
| `limit_type_code` | STRING | NOT NULL | Business code (AC, NOR, PAR, ALERT, ACTION, IPC_LIMIT, REPORT, COMPENDIA) |
| `limit_type_name` | STRING | NOT NULL | Display name |
| `limit_type_description` | STRING | | Full description |
| `hierarchy_level` | INT | | 1=Internal, 2=Site, 3=Regulatory |
| `limit_category` | STRING | | OPERATIONAL \| DESIGN_SPACE \| REGULATORY \| PROCESS_CONTROL \| INFORMATIONAL |
| `is_regulatory` | BOOLEAN | | Appears in regulatory filing |
| `sort_order` | INT | | Display sort order |

---

#### DIM_TEST_METHOD — Analytical Test Methods

**Table:** `l2_2_spec_unified.dim_test_method`
**Grain:** One row per test method version

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `test_method_key` | BIGINT | NOT NULL | Surrogate primary key |
| `test_method_id` | STRING | NOT NULL | Natural key from source |
| `test_method_number` | STRING | | Method number (e.g., TM-HPLC-001) |
| `test_method_name` | STRING | NOT NULL | Method name |
| `test_method_version` | STRING | | Method version |
| `method_type_code` | STRING | | COMP (Compendial) \| INHOUSE \| TRANSFER |
| `method_type_name` | STRING | | Compendial \| In-House \| Transferred |
| `analytical_technique` | STRING | | HPLC \| GC \| UV-VIS \| KF \| IR \| NMR \| ICP-MS \| etc. |
| `instrument_type` | STRING | | Instrument/equipment type |
| `compendia_reference` | STRING | | e.g., USP \<621\>, EP 2.2.29 |
| `validation_status` | STRING | | VALIDATED \| QUALIFIED \| VERIFIED \| WAIVED \| EXEMPT |
| `validation_date` | DATE | | Date of method validation completion |
| `source_system_code` | STRING | NOT NULL | Source system |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |
| `is_current` | BOOLEAN | NOT NULL | Current flag |

---

#### DIM_PRODUCT — Pharmaceutical Product

**Table:** `l2_2_spec_unified.dim_product`
**Grain:** One row per product (MDM-managed)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `product_key` | BIGINT | NOT NULL | Surrogate primary key |
| `product_id` | STRING | NOT NULL | MDM natural key |
| `product_code` | STRING | | Internal product code |
| `product_name` | STRING | NOT NULL | Drug product name |
| `inn_name` | STRING | | International Nonproprietary Name (INN/USAN) |
| `brand_name` | STRING | | Commercial/trade name |
| `dosage_form_code` | STRING | | TAB \| CAP \| INJ \| SOL \| CRM \| SUS \| etc. |
| `dosage_form_name` | STRING | | Tablet \| Capsule \| Injection \| Solution \| Cream \| Suspension |
| `route_of_administration` | STRING | | ORAL \| IV \| IM \| SC \| TOPICAL \| etc. |
| `strength` | STRING | | e.g., 10 mg, 250 mg/5 mL |
| `strength_value` | DECIMAL(12,4) | | Numeric strength value |
| `strength_uom` | STRING | | Strength unit (mg, mg/mL, %) |
| `therapeutic_area` | STRING | | Oncology, Cardiology, CNS, etc. |
| `atc_code` | STRING | | WHO ATC classification code |
| `nda_number` | STRING | | NDA/ANDA/BLA/MAA application number |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |
| `is_current` | BOOLEAN | NOT NULL | Current flag |

---

#### DIM_MATERIAL — Drug Substance / Material

**Table:** `l2_2_spec_unified.dim_material`
**Grain:** One row per material/substance (MDM-managed)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `material_key` | BIGINT | NOT NULL | Surrogate primary key |
| `material_id` | STRING | NOT NULL | MDM natural key |
| `material_code` | STRING | | Internal material number (e.g., SAP material number) |
| `material_name` | STRING | NOT NULL | Material name |
| `material_type_code` | STRING | | API \| EXCIP \| INTERMED \| DRUG_PROD \| RM \| PACK |
| `material_type_name` | STRING | | Active Pharmaceutical Ingredient \| Excipient \| Intermediate \| Drug Product \| Raw Material \| Packaging |
| `cas_number` | STRING | | CAS Registry Number |
| `molecular_formula` | STRING | | Molecular formula (for APIs) |
| `molecular_weight` | DECIMAL(10,4) | | Molecular weight (g/mol) |
| `pharmacopoeia_grade` | STRING | | USP \| EP \| JP \| BP \| ACS \| NF \| FCC |
| `manufacturer_name` | STRING | | Material manufacturer name |
| `manufacturer_code` | STRING | | Manufacturer internal code |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |
| `is_current` | BOOLEAN | NOT NULL | Current flag |

---

#### DIM_REGULATORY_CONTEXT — Regulatory Filing Context

**Table:** `l2_2_spec_unified.dim_regulatory_context`
**Grain:** One row per regulatory region/submission type combination

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `regulatory_context_key` | BIGINT | NOT NULL | Surrogate primary key |
| `regulatory_context_code` | STRING | NOT NULL | e.g., US-NDA, EU-MAA, JP-JNDA, GLOBAL |
| `region_code` | STRING | | US \| EU \| JP \| CA \| ROW \| GLOBAL |
| `region_name` | STRING | | United States \| European Union \| Japan \| Canada \| Rest of World |
| `regulatory_body` | STRING | | FDA \| EMA \| PMDA \| Health Canada \| etc. |
| `submission_type` | STRING | | NDA \| ANDA \| BLA \| MAA \| JNDA \| CTD |
| `guideline_code` | STRING | | ICH Q6A \| ICH Q6B \| ICH Q2 \| USP \| EP |
| `guideline_name` | STRING | | Full guideline name |
| `ctd_module` | STRING | | Module 3 |
| `ctd_section` | STRING | | 3.2.S.4.1 \| 3.2.P.5.1 \| etc. |

---

#### DIM_UOM — Unit of Measure

**Table:** `l2_2_spec_unified.dim_uom`
**Grain:** One row per unit of measure

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `uom_key` | BIGINT | NOT NULL | Surrogate primary key |
| `uom_code` | STRING | NOT NULL | Unit code (e.g., %, mg, mg/mL, ppm, CFU/mL, NLT, NMT) |
| `uom_name` | STRING | NOT NULL | Full unit name |
| `uom_category` | STRING | | CONCENTRATION \| MASS \| VOLUME \| RATIO \| COUNT \| TIME \| DIMENSIONLESS |
| `si_conversion_factor` | DECIMAL(20,10) | | Conversion factor to SI base unit |
| `si_uom_code` | STRING | | SI base unit code |

---

#### DIM_DATE — Date Dimension

**Table:** `l2_2_spec_unified.dim_date`
**Grain:** One row per calendar date

| Column | Data Type | Description |
|--------|-----------|-------------|
| `date_key` | INT | Surrogate key (YYYYMMDD integer) |
| `full_date` | DATE | Calendar date |
| `year` | INT | Calendar year |
| `quarter` | INT | Quarter (1–4) |
| `month` | INT | Month (1–12) |
| `month_name` | STRING | January–December |
| `week_of_year` | INT | ISO week number |
| `day_of_year` | INT | Day of year (1–366) |
| `day_of_month` | INT | Day of month (1–31) |
| `day_of_week` | INT | Day of week (1=Monday) |
| `day_name` | STRING | Monday–Sunday |
| `fiscal_year` | INT | Fiscal year |
| `fiscal_quarter` | INT | Fiscal quarter |
| `is_weekend` | BOOLEAN | Saturday or Sunday |
| `is_holiday` | BOOLEAN | Public holiday flag |

---

### 4.3 Fact Tables

#### FACT_SPECIFICATION_LIMIT — Normalized Specification Limits

**Table:** `l2_2_spec_unified.fact_specification_limit`
**Grain:** One row per **limit type** per **specification item** per **stage/time point**
**Description:** The central fact of the specification domain. Stores all limit values (NOR, PAR, Acceptance Criteria, Alert, Action, IPC) in a normalized structure. A single test item will have multiple rows — one per limit type applicable.

**Example:** Assay test in Drug Product Release specification:
- Row 1: limit_type=AC, lower=98.0, upper=102.0, stage=RELEASE
- Row 2: limit_type=NOR, lower=99.0, upper=101.0, stage=RELEASE
- Row 3: limit_type=PAR, lower=97.0, upper=103.0, stage=RELEASE
- Row 4: limit_type=AC, lower=95.0, upper=105.0, stage=STABILITY, time_point=T24M

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `spec_limit_key` | BIGINT | NOT NULL | Surrogate primary key |
| `spec_key` | BIGINT | NOT NULL | FK → dim_specification (degenerate dim) |
| `spec_item_key` | BIGINT | NOT NULL | FK → dim_specification_item |
| `limit_type_key` | BIGINT | NOT NULL | FK → dim_limit_type |
| `uom_key` | BIGINT | | FK → dim_uom (limit-specific unit) |
| **Numeric Limit Values** | | | |
| `lower_limit_value` | DECIMAL(18,6) | | Lower bound numeric value |
| `upper_limit_value` | DECIMAL(18,6) | | Upper bound numeric value |
| `target_value` | DECIMAL(18,6) | | Nominal/target value |
| **Limit Operators** | | | |
| `lower_limit_operator` | STRING | | NLT \| GT \| GTE (Not Less Than / Greater Than / ≥) |
| `upper_limit_operator` | STRING | | NMT \| LT \| LTE (Not More Than / Less Than / ≤) |
| **Text Limits** | | | |
| `limit_text` | STRING | | Non-numeric limit (e.g., "Clear, colorless solution") |
| `limit_description` | STRING | | Full formatted limit expression for CTD (e.g., "NLT 98.0% and NMT 102.0%") |
| **Limit Context** | | | |
| `limit_basis` | STRING | | AS_IS \| ANHYDROUS \| AS_LABELED \| DRIED_BASIS \| INTACT_PROTEIN |
| `stage_code` | STRING | NOT NULL | RELEASE \| STABILITY \| IPC \| BOTH |
| `stability_time_point` | STRING | | T0 \| T1M \| T3M \| T6M \| T9M \| T12M \| T18M \| T24M \| T36M \| T48M \| T60M |
| `stability_condition` | STRING | | 25C60RH \| 30C65RH \| 40C75RH \| REFRIG \| FREEZE \| ACCEL |
| **Conditional Limits** | | | |
| `is_conditional` | BOOLEAN | | Limit applies only under specific conditions |
| `condition_description` | STRING | | Description of condition triggering this limit |
| **Regulatory** | | | |
| `regulatory_basis` | STRING | | ICH Q6A \| USP \<xxx\> \| EP x.x.x \| JP G3 |
| `is_in_filing` | BOOLEAN | NOT NULL | TRUE if this limit appears in the regulatory filing |
| `is_current` | BOOLEAN | NOT NULL | SCD2 current flag |
| **Audit** | | | |
| `effective_date` | DATE | | Date this limit became effective |
| `source_system_code` | STRING | NOT NULL | Source system identifier |
| `source_system_id` | STRING | | Original record ID in source system |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |

**Partition:** `stage_code, spec_type_code` (via join)
**Z-Order:** `spec_item_key, limit_type_key`

---

## 5. L2.2 Denormalized / Semi-Normalized Tables

### DSPEC_SPECIFICATION — Denormalized Specification + Acceptance Criteria

**Table:** `l2_2_spec_unified.dspec_specification`
**Grain:** One row per **specification item** with all limit types pivoted as columns
**Description:** Semi-denormalized analytical table combining specification header, item attributes, and all limit types as pivoted columns. Optimized for specification review, quality control dashboards, and intermediate CTD preparation.

**Design Note:** Limits are pivoted from `fact_specification_limit` using conditional aggregation (MAX CASE WHEN limit_type_code = 'AC' THEN ...). This table is refreshed on a schedule as a materialized view or Delta table.

#### Section A — Specification Header (denormalized from dim_specification)

| Column | Data Type | Description |
|--------|-----------|-------------|
| `spec_key` | BIGINT | Surrogate key of specification |
| `spec_number` | STRING | Specification document number |
| `spec_version` | STRING | Specification version |
| `spec_title` | STRING | Full specification title |
| `spec_type_code` | STRING | DS / DP / RM / EXCIP / INTERMED / IPC / CCS |
| `spec_type_name` | STRING | Drug Substance / Drug Product / etc. |
| `product_name` | STRING | Product name (from dim_product) |
| `inn_name` | STRING | INN name |
| `dosage_form_name` | STRING | Dosage form |
| `strength` | STRING | Strength string |
| `material_name` | STRING | Material/substance name (from dim_material) |
| `material_type_code` | STRING | API / EXCIP / etc. |
| `ctd_section` | STRING | CTD filing section |
| `regulatory_context_code` | STRING | US-NDA / EU-MAA / etc. |
| `stage_code` | STRING | DEV / CLI / COM |
| `stage_name` | STRING | Development / Clinical / Commercial |
| `status_code` | STRING | DRA / APP / SUP / OBS |
| `status_name` | STRING | Draft / Approved / Superseded / Obsolete |
| `effective_date` | DATE | Specification effective date |
| `approval_date` | DATE | Specification approval date |
| `site_code` | STRING | Site code |
| `compendia_reference` | STRING | USP / EP / JP |

#### Section B — Specification Item (denormalized from dim_specification_item)

| Column | Data Type | Description |
|--------|-----------|-------------|
| `spec_item_key` | BIGINT | Surrogate key of test item |
| `sequence_number` | INT | Test order in specification |
| `test_code` | STRING | Test code |
| `test_name` | STRING | Test name (e.g., Assay, Dissolution, Appearance) |
| `test_category_code` | STRING | PHY / CHE / MIC / BIO / IMP / STER |
| `test_category_name` | STRING | Physical / Chemical / Microbiological / etc. |
| `test_subcategory` | STRING | Subcategory (e.g., Related Substances) |
| `test_method_number` | STRING | Method number |
| `test_method_name` | STRING | Method name |
| `analytical_technique` | STRING | HPLC / GC / UV-VIS / etc. |
| `compendia_test_ref` | STRING | USP \<711\> / EP 2.9.3 / etc. |
| `uom_code` | STRING | Result unit code |
| `uom_name` | STRING | Result unit name |
| `reporting_type` | STRING | NUMERIC / PASS_FAIL / TEXT / REPORT_ONLY |
| `result_precision` | INT | Decimal places for reporting |
| `stage_applicability` | STRING | RELEASE / STABILITY / IPC / BOTH |
| `is_required` | BOOLEAN | Mandatory test flag |
| `is_compendial` | BOOLEAN | Compendial test flag |

#### Section C — Acceptance Criteria (AC — Regulatory Limits)

| Column | Data Type | Description |
|--------|-----------|-------------|
| `ac_lower_limit` | DECIMAL(18,6) | Acceptance criteria lower bound |
| `ac_upper_limit` | DECIMAL(18,6) | Acceptance criteria upper bound |
| `ac_target` | DECIMAL(18,6) | Acceptance criteria target/nominal |
| `ac_lower_operator` | STRING | NLT / GT / GTE |
| `ac_upper_operator` | STRING | NMT / LT / LTE |
| `ac_limit_text` | STRING | Text limit (non-numeric) |
| `ac_limit_description` | STRING | Full formatted expression (e.g., "98.0% to 102.0%") |
| `ac_limit_basis` | STRING | AS_IS / ANHYDROUS / AS_LABELED / DRIED_BASIS |
| `ac_stage` | STRING | RELEASE / STABILITY / BOTH |
| `ac_stability_time_point` | STRING | T0 / T6M / T12M / T24M / T36M |
| `ac_stability_condition` | STRING | 25C60RH / 40C75RH / REFRIG |
| `ac_regulatory_basis` | STRING | ICH Q6A / USP \<xxx\> / EP x.x.x |
| `ac_is_in_filing` | BOOLEAN | Appears in regulatory filing |

#### Section D — Normal Operating Range (NOR — Internal)

| Column | Data Type | Description |
|--------|-----------|-------------|
| `nor_lower_limit` | DECIMAL(18,6) | NOR lower bound |
| `nor_upper_limit` | DECIMAL(18,6) | NOR upper bound |
| `nor_target` | DECIMAL(18,6) | NOR target/nominal |
| `nor_limit_description` | STRING | Full formatted expression |

#### Section E — Proven Acceptable Range (PAR — Design Space)

| Column | Data Type | Description |
|--------|-----------|-------------|
| `par_lower_limit` | DECIMAL(18,6) | PAR lower bound |
| `par_upper_limit` | DECIMAL(18,6) | PAR upper bound |
| `par_target` | DECIMAL(18,6) | PAR target/nominal |
| `par_limit_description` | STRING | Full formatted expression |

#### Section F — Alert / Action Limits (Internal Process Control)

| Column | Data Type | Description |
|--------|-----------|-------------|
| `alert_lower_limit` | DECIMAL(18,6) | Alert limit lower bound |
| `alert_upper_limit` | DECIMAL(18,6) | Alert limit upper bound |
| `action_lower_limit` | DECIMAL(18,6) | Action limit lower bound |
| `action_upper_limit` | DECIMAL(18,6) | Action limit upper bound |

#### Section G — Metadata

| Column | Data Type | Description |
|--------|-----------|-------------|
| `load_timestamp` | TIMESTAMP | ETL refresh timestamp |
| `is_current` | BOOLEAN | Current row flag |

---

## 6. L3 Final Data Products (OBT)

**Schema:** `l3_spec_products`

### OBT_SPECIFICATION_CTD — One Big Table for CTD Regulatory Filing

**Table:** `l3_spec_products.obt_specification_ctd`
**Grain:** One row per specification item (release acceptance criteria, one row per test per specification)
**Description:** Fully denormalized, CTD-aligned one-big-table. Merges all specification, item, limit, product, material, and method information into a single flat structure for:
- CTD Module 3 narrative generation
- Regulatory submission portals
- BI/reporting (Power BI, Tableau)
- API exposure to downstream systems

This table includes **only `is_in_filing = TRUE`** limit records (Acceptance Criteria) and is scoped to approved, current specification versions.

| Column Group | Columns | Source |
|---|---|---|
| Specification | spec_number, spec_version, spec_title, spec_type, ctd_section | dim_specification |
| Product | product_name, inn_name, brand_name, dosage_form, route, strength, nda_number | dim_product |
| Material | material_name, cas_number, molecular_formula, molecular_weight | dim_material |
| Regulatory | region, regulatory_body, submission_type, guideline | dim_regulatory_context |
| Test Item | sequence_number, test_name, test_category, test_subcategory, method_number, uom | dim_specification_item + dim_test_method |
| Acceptance Criteria | ac_lower, ac_upper, ac_target, ac_operator_lower, ac_operator_upper, ac_limit_description, ac_limit_basis | fact_specification_limit (AC) |
| Stability Criteria | (multiple rows for stability time points) | fact_specification_limit (AC, STABILITY) |
| NOR | nor_lower, nor_upper, nor_target | fact_specification_limit (NOR) |
| PAR | par_lower, par_upper, par_target | fact_specification_limit (PAR) |
| Audit | effective_date, approval_date, approver, load_timestamp | dim_specification |

**Partitions:** `spec_type_code, stage_code`
**Z-Order:** `spec_number, test_name`

---

### OBT_ACCEPTANCE_CRITERIA — Acceptance Criteria Summary Table

**Table:** `l3_spec_products.obt_acceptance_criteria`
**Grain:** One row per test per specification per stability time point
**Description:** Focused OBT containing only regulatory acceptance criteria (what appears in the specification document). Optimized for specification comparison, trending, and gap analysis.

---

## 7. Key Business Rules & Definitions

### Limit Type Hierarchy
```
Regulatory Layer:  PAR >= AC  (PAR must be at least as wide as AC)
Internal Layer:    AC >= NOR  (AC must be at least as wide as NOR)
Full hierarchy:    PAR ≥ AC ≥ NOR, with NOR ≥ ALERT ≥ ACTION
```

### Limit Operator Codes

| Code | Symbol | Meaning |
|------|--------|---------|
| `NLT` | ≥ | Not Less Than |
| `NMT` | ≤ | Not More Than |
| `GT` | > | Greater Than (strictly) |
| `LT` | < | Less Than (strictly) |
| `EQ` | = | Equal To |
| `RANGE` | — | Expressed as lower–upper range |
| `CONFORM` | — | Conforms to description/reference |
| `REPORT` | — | Report only; no limit |

### Specification Lifecycle States

```
DRA (Draft) → APP (Approved) → [SUP (Superseded)] → OBS (Obsolete)
                                                   → ARCH (Archived)
```

### SCD Type 2 Strategy
- `dim_specification` uses SCD Type 2 to preserve full version history
- New specification versions create new rows; prior rows get `valid_to` set and `is_current = FALSE`
- `dim_specification_item` uses SCD Type 2, linked to the specification version
- `fact_specification_limit` uses `is_current` flag for current limits

### CTD Section Mapping

| Specification Type | CTD Section | Description |
|---|---|---|
| Drug Substance | 3.2.S.4.1 | Specification |
| Drug Substance | 3.2.S.4.2 | Analytical Procedures |
| Drug Substance | 3.2.S.4.3 | Validation of Analytical Procedures |
| Drug Product | 3.2.P.5.1 | Specification |
| Drug Product | 3.2.P.5.2 | Analytical Procedures |
| Drug Product | 3.2.P.5.3 | Validation of Analytical Procedures |
| Drug Product | 3.2.P.5.6 | Justification of Specifications |
| Excipients | 3.2.P.4 | Excipients (CofA references) |

---

## 8. CTD Section Mapping

The L3 OBT is structured to map directly to CTD Module 3 sections:

```
CTD 3.2.S.4.1 / 3.2.P.5.1 — Specification Table
    ← obt_specification_ctd WHERE spec_type_code = 'DS'/'DP'
       AND stage_code = 'COM'
       AND status_code = 'APP'
       AND is_in_filing = TRUE
       AND is_current = TRUE

CTD 3.2.S.4.2 / 3.2.P.5.2 — Analytical Procedures
    ← dim_test_method (method details)

CTD 3.2.P.5.6 — Justification of Specifications
    ← fact_specification_limit WHERE limit_type_code IN ('AC','PAR')
       Compares AC vs PAR vs historical data
```

---

## 9. Naming Conventions

| Layer | Prefix | Example |
|-------|--------|---------|
| L1 Raw | `raw_` | `raw_lims_specification` |
| L2.1 Source Conform | `src_` | `src_lims_specification` |
| L2.2 Dimension | `dim_` | `dim_specification` |
| L2.2 Fact | `fact_` | `fact_specification_limit` |
| L2.2 Denormalized | `dspec_` | `dspec_specification` |
| L3 OBT | `obt_` | `obt_specification_ctd` |
| L3 Aggregated | `agg_` | `agg_specification_summary` |

**Column naming:**
- Surrogate keys: `<entity>_key` (BIGINT)
- Natural keys: `<entity>_id` (STRING)
- Foreign keys: `<referenced_entity>_key`
- Codes: `<attribute>_code` (short, uppercase values)
- Names: `<attribute>_name` (human-readable)
- Flags: `is_<attribute>` (BOOLEAN)
- Timestamps: `load_timestamp`, `valid_from`, `valid_to`
- Dates: `effective_date`, `approval_date`, `expiry_date`

---

## 10. Partition & Optimization Strategy

| Table | Partition Column(s) | Z-Order Column(s) |
|-------|--------------------|--------------------|
| `dim_specification` | `spec_type_code` | `spec_number, spec_version` |
| `dim_specification_item` | `test_category_code` | `spec_key, test_code` |
| `fact_specification_limit` | `stage_code` | `spec_item_key, limit_type_key` |
| `dspec_specification` | `spec_type_code, stage_code` | `spec_number, test_code` |
| `obt_specification_ctd` | `spec_type_code, stage_code` | `spec_number, test_name` |

**Optimization notes:**
- Enable Delta Lake `OPTIMIZE` and `VACUUM` on a weekly schedule
- Use liquid clustering for high-cardinality fact tables in Unity Catalog
- Enable Delta Change Data Feed (CDF) on dimension tables for incremental downstream processing
- `obt_specification_ctd` should be rebuilt as a full refresh (not incremental) to ensure CTD accuracy

---

## 11. Data Lineage Summary

```
Source System (LIMS/SAP/Vault)
    │
    ▼ [Ingest — no transform]
L1: raw_<source>_specification
    raw_<source>_spec_item
    raw_<source>_spec_limit
    │
    ▼ [Cleanse, type, source rules]
L2.1: src_<source>_specification
      src_<source>_spec_item
      src_<source>_spec_limit
      │
      ▼ [Harmonize, MDM, surrogate keys, SCD2]
L2.2: dim_specification          ← core dimension
      dim_specification_item     ← test details
      dim_limit_type             ← reference
      dim_test_method            ← method reference
      dim_product                ← MDM product
      dim_material               ← MDM material
      dim_regulatory_context     ← reference
      dim_uom                    ← reference
      fact_specification_limit   ← all limit types, normalized
      dspec_specification        ← pivoted, semi-denormalized
      │
      ▼ [Flatten, filter, CTD-align]
L3:   obt_specification_ctd      ← CTD-ready OBT
      obt_acceptance_criteria    ← AC-focused OBT
      agg_specification_summary  ← aggregated metrics
```

---

*End of Specification — Version 1.0*
