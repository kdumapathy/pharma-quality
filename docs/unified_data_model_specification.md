# Unified Data Model Specification — Pharmaceutical Quality (Specifications Domain)

**Version:** 2.0
**Domain:** Pharmaceutical Quality — Specifications & Analytical Results (Stability)
**Primary Consumption:** Regulatory Filing (CTD Module 3), Stability Analysis
**Platform:** Databricks (Delta Lake, Unity Catalog)
**Architecture:** Medallion / Layered (L1 → L2.1 → L2.2 → L3)

> **See also:** [ER Diagrams (Mermaid)](./er_diagrams.md) — visual entity relationship diagrams for both the Specification and Stability data models.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture & Layer Definitions](#2-architecture--layer-definitions)
3. [Data Sources](#3-data-sources)
4. [L2.2 Unified Data Model — Star Schema](#4-l22-unified-data-model--star-schema)
   - 4.1 [Entity Relationship Diagram](#41-entity-relationship-diagram)
   - 4.2 [Dimension Tables — Reference](#42-dimension-tables--reference)
   - 4.3 [Dimension Tables — MDM](#43-dimension-tables--mdm)
   - 4.4 [Dimension Tables — Conformed (Specifications)](#44-dimension-tables--conformed-specifications)
   - 4.5 [Dimension Tables — Analytical Results](#45-dimension-tables--analytical-results)
   - 4.6 [Fact Tables](#46-fact-tables)
5. [L2.2 Denormalized / Semi-Normalized Tables](#5-l22-denormalized--semi-normalized-tables)
6. [L3 Final Data Products (OBT)](#6-l3-final-data-products-obt)
7. [Key Business Rules & Definitions](#7-key-business-rules--definitions)
8. [CTD Section Mapping](#8-ctd-section-mapping)
9. [Naming Conventions](#9-naming-conventions)
10. [Partition & Optimization Strategy](#10-partition--optimization-strategy)
11. [Data Lineage Summary](#11-data-lineage-summary)

---

## 1. Overview

This document specifies the **Unified Data Model (UDM)** for the Pharmaceutical Quality domain, covering two sub-domains: **Specifications** and **Analytical Results / Stability**. The model integrates data from multiple source systems (LIMS, process recipe systems, PDF/SOP documents, vendor Excel stability workbooks) into harmonized data products optimized for:

- **Regulatory filing** (CTD Common Technical Document, Modules 3.2.S.4 and 3.2.P.5)
- **Quality control** and release testing
- **Stability program** tracking
- **Process analytical technology (PAT)** and control strategy alignment
- **Audit and version management** (specification lifecycle)

### Domain Scope

**Specifications domain** — A specification is a formal document that establishes the criteria to which a drug substance, drug product, intermediate, raw material, or other material must conform. This model captures:

| Entity | Description |
|--------|-------------|
| Specification Header | Identity, version, site/market context, lifecycle status |
| Specification Item | Individual tests (Assay, Identity, Dissolution, etc.) |
| Specification Limits | All limit types: AC, NOR, PAR, Alert, Action, IPC |
| Acceptance Criteria | Regulatory-filed limits with full operator semantics |
| Denormalized View | Item + all limits pivoted (release-ready flat structure) |

**Analytical Results / Stability domain** — Captures measured test results from vendor and in-house stability studies:

| Entity | Description |
|--------|-------------|
| Batch | Manufacturing batch/lot with product and site linkage |
| Analytical Result | One result per batch × test × ICH condition × time point |
| Stability Condition | ICH storage conditions (25°C/60%RH, 40°C/75%RH, etc.) |
| Time Point | Standard stability time points (T0, T3M, T6M, T12M, T24M, T36M) |
| Stability OBT | Flattened result table with OOS/OOT flags and spec limits |

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
│  Schema: l2_1_<source_system>  (e.g., l2_1_scl, l2_1_sap)         │
│  • Per-source-system clean and typed tables                          │
│  • Source business rules applied                                     │
│  • Standardized data types, null handling, deduplication            │
│  • Source-native keys preserved                                      │
└────────────────┬────────────────────────────────────────────────────┘
                 │ Cross-source integration, harmonization, MDM
                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  L2.2 — UNIFIED DATA MODEL (Business Conform Layer)                  │
│  Schema: l2_2_unified_model                                           │
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
│  Schema: l3_data_product                                            │
│  • One Big Table (OBT) — full denormalized, CTD-aligned             │
│  • Aggregated summary tables                                         │
│  • Regulatory submission-ready                                       │
│  • Optimized for BI, reporting, API consumption                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Sources

| Source System | Type | L1 Raw Table | Data Provided |
|---------------|------|--------------|---------------|
| LabWare / Labvantage LIMS | LIMS | `raw_lims_specification`, `raw_lims_spec_item`, `raw_lims_spec_limit` | Drug Substance/Product specs, test items, limits, analytical methods |
| Recipe Management System | Recipe | `raw_process_recipe` | Process NOR, PAR, Target, Alert, Action limits; SPC capability data (Cpk) |
| PDF / SOP Documents | Manual/Transcribed | `raw_pdf_specification` | Transcribed specification data from PDF and SOP documents (one row per test-limit) |
| Vendor Excel / CRO LIMS | Stability | `raw_vendor_analytical_results` | Stability analytical results from external labs and CROs |

## 3.1 Schema Naming Standard

To keep the model enterprise-friendly and auditable, schema names follow a consistent pattern:

- `l<layer>_<sub_layer>_<domain_or_purpose>` for canonical layers (for example, `l2_2_unified_model`).
- Source-conform schemas use a short source-system code (for example, `l2_1_scl`).
- L3 schemas use product-oriented names to reflect business ownership (for example, `l3_data_product`).

This convention improves discoverability in Unity Catalog and makes lineage and ownership easier to infer from object names alone.

---

## 4. L2.2 Unified Data Model — Star Schema

**Schema:** `l2_2_unified_model`
**Catalog:** `pharma_quality_catalog`

### 4.1 Entity Relationship Diagram

> For interactive Mermaid ER diagrams see [docs/er_diagrams.md](./er_diagrams.md).

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  dim_product │  │ dim_material │  │   dim_site   │  │  dim_market  │  │   dim_date   │
│ (product_key)│  │(material_key)│  │  (site_key)  │  │ (market_key) │  │  (date_key)  │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                 │                  │                  │
       └────────┬────────┘                 └────────┬─────────┘    effective/    │
                │ FKs                               │ FKs          approval      │
                └───────────────────┬───────────────┘              date FKs ─────┘
                                    │
                        ┌───────────▼──────────────┐
                        │      dim_specification    │
                        │  spec_key (PK / SCD2)    │
                        │  source_specification_id  │
                        │  spec_number              │
                        │  spec_version             │
                        │  spec_type_code           │
                        │  status_code / stage_code │
                        │  ctd_section              │
                        │  site_key (FK)            │
                        │  market_key (FK)          │
                        └──────────────┬────────────┘
                                       │ 1:N
                        ┌──────────────▼────────────┐
                        │   dim_specification_item   │
                        │  spec_item_key (PK / SCD2)│
                        │  spec_key (FK)            ├──────► dim_test_method
                        │  test_code / test_name    │        (test_method_key)
                        │  test_category_code       │
                        │  criticality_code         ├──────► dim_uom
                        │  reporting_type           │        (uom_key)
                        └──────────────┬────────────┘
                                       │ 1:N
                        ┌──────────────▼────────────┐
                        │   fact_specification_limit │◄──── dim_limit_type
                        │  spec_limit_key (PK)      │      (limit_type_key)
                        │  spec_item_key (FK)       │
                        │  spec_key (FK)            │◄──── dim_uom
                        │  limit_type_key (FK)      │      (uom_key)
                        │  lower_limit_value        │
                        │  upper_limit_value        │◄──── dim_date
                        │  target_value             │      (effective_start/end
                        │  limit_range_width        │       last_calculated)
                        │  lower_limit_operator     │
                        │  upper_limit_operator     │
                        │  limit_text               │
                        │  limit_description        │
                        │  stage_code               │
                        │  stability_time_point     │
                        │  stability_condition      │
                        │  calculation_method / cpk │
                        │  is_in_filing             │
                        └───────────────────────────┘
```

---

### 4.2 Dimension Tables — Reference

#### DIM_DATE — Calendar Date

**Table:** `l2_2_unified_model.dim_date`
**Grain:** One row per calendar date
**Description:** Standard calendar date dimension. Populated by the seed notebook. Used as date surrogate FK across all tables.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `date_key` | INT | NOT NULL | Surrogate key (YYYYMMDD integer) |
| `full_date` | DATE | NOT NULL | Calendar date |
| `year` | INT | NOT NULL | Calendar year |
| `quarter` | INT | NOT NULL | Quarter (1–4) |
| `month` | INT | NOT NULL | Month (1–12) |
| `month_name` | STRING | NOT NULL | January–December |
| `day_of_month` | INT | NOT NULL | Day of month (1–31) |
| `day_of_week` | INT | NOT NULL | Day of week (1=Monday, 7=Sunday) |
| `day_name` | STRING | NOT NULL | Monday–Sunday |
| `week_of_year` | INT | NOT NULL | ISO week number |
| `is_weekend` | BOOLEAN | NOT NULL | TRUE if Saturday or Sunday |
| `fiscal_year` | INT | | Fiscal year (configurable offset) |
| `fiscal_quarter` | INT | | Fiscal quarter |

---

#### DIM_UOM — Unit of Measure

**Table:** `l2_2_unified_model.dim_uom`
**Grain:** One row per unit of measure

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `uom_key` | INT | NOT NULL | Surrogate primary key |
| `uom_code` | STRING | NOT NULL | Standardized unit code (e.g., %, mg, ppm, CFU/mL) |
| `uom_name` | STRING | NOT NULL | Full unit display name |
| `uom_category` | STRING | NOT NULL | MASS \| CONCENTRATION \| COUNT \| RATIO \| LENGTH \| VOLUME \| OTHER |
| `si_conversion_factor` | DECIMAL(18,10) | | Conversion factor to SI base unit |
| `si_base_unit` | STRING | | SI base unit code |
| `is_active` | BOOLEAN | NOT NULL | Active flag |

---

#### DIM_LIMIT_TYPE — Limit Type Reference

**Table:** `l2_2_unified_model.dim_limit_type`
**Grain:** One row per limit type (static reference table)
**Description:** Classifies the type of each limit value, enabling normalized storage of NOR, PAR, AC, Alert, Action, and IPC limits in a single fact table.

| Code | Name | Description | Is Regulatory |
|------|------|-------------|---------------|
| `AC` | Acceptance Criteria | Regulatory specification limit (in CTD filing) | TRUE |
| `NOR` | Normal Operating Range | Internal tighter range to ensure AC compliance | FALSE |
| `PAR` | Proven Acceptable Range | Broader range proven via development; may appear in CTD design space | TRUE (Design Space) |
| `ALERT` | Alert Limit | Internal early warning limit | FALSE |
| `ACTION` | Action Limit | Internal limit triggering mandatory investigation | FALSE |
| `IPC_LIMIT` | In-Process Control Limit | Applied during manufacturing process | FALSE |
| `REPORT` | Report Only | No limit; result reported for information | FALSE |

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `limit_type_key` | INT | NOT NULL | Surrogate primary key |
| `limit_type_code` | STRING | NOT NULL | AC \| NOR \| PAR \| ALERT \| ACTION \| IPC_LIMIT \| REPORT |
| `limit_type_name` | STRING | NOT NULL | Display name |
| `limit_type_description` | STRING | | Full description |
| `hierarchy_rank` | INT | | Rank in limit hierarchy (1=tightest/NOR, 3=widest/PAR) |
| `is_regulatory` | BOOLEAN | NOT NULL | Appears in regulatory filing |
| `is_active` | BOOLEAN | NOT NULL | Active flag |

---

#### DIM_REGULATORY_CONTEXT — Regulatory Filing Context

**Table:** `l2_2_unified_model.dim_regulatory_context`
**Grain:** One row per regulatory region/submission type combination

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `regulatory_context_key` | INT | NOT NULL | Surrogate primary key |
| `region_code` | STRING | NOT NULL | US \| EU \| JP \| CN \| ROW |
| `region_name` | STRING | NOT NULL | United States \| European Union \| Japan \| China \| Rest of World |
| `submission_type` | STRING | NOT NULL | NDA \| ANDA \| BLA \| MAA \| JNDA \| IND |
| `ctd_module` | STRING | | CTD module reference (e.g., Module 3) |
| `ctd_section_title` | STRING | | CTD section title |
| `regulatory_authority` | STRING | | FDA \| EMA \| PMDA \| NMPA \| Health Canada |
| `is_active` | BOOLEAN | NOT NULL | Active flag |

---

### 4.3 Dimension Tables — MDM

#### DIM_PRODUCT — Pharmaceutical Product

**Table:** `l2_2_unified_model.dim_product`
**Grain:** One row per product (MDM-managed)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `product_key` | BIGINT | NOT NULL | Surrogate primary key |
| `product_id` | STRING | NOT NULL | MDM-resolved product ID |
| `product_name` | STRING | NOT NULL | Drug product name |
| `product_family` | STRING | | Product family grouping |
| `brand_name` | STRING | | Commercial/trade name |
| `dosage_form` | STRING | | Tablet \| Capsule \| Injection \| Solution \| Cream \| Suspension |
| `route_of_administration` | STRING | | ORAL \| IV \| IM \| SC \| TOPICAL |
| `therapeutic_area` | STRING | | Oncology, Cardiology, CNS, etc. |
| `strength` | STRING | | Strength string (e.g., 10 mg, 250 mg/5 mL) |
| `is_active` | BOOLEAN | NOT NULL | Active flag |
| `effective_from` | DATE | | MDM effective start date |
| `effective_to` | DATE | | MDM effective end date (NULL = current) |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |

---

#### DIM_MATERIAL — Drug Substance / Material

**Table:** `l2_2_unified_model.dim_material`
**Grain:** One row per material/substance (MDM-managed)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `material_key` | BIGINT | NOT NULL | Surrogate primary key |
| `material_id` | STRING | NOT NULL | MDM-resolved material ID |
| `material_name` | STRING | NOT NULL | Material name |
| `material_type` | STRING | | API \| EXCIPIENT \| INTERMEDIATE \| PACKAGING \| REFERENCE_STD |
| `cas_number` | STRING | | CAS Registry Number |
| `inn_name` | STRING | | International Nonproprietary Name (INN/USAN) |
| `compendial_name` | STRING | | Compendial name (USP/EP/JP) |
| `grade` | STRING | | Material grade (USP, EP, ACS, NF, FCC) |
| `is_active` | BOOLEAN | NOT NULL | Active flag |
| `effective_from` | DATE | | MDM effective start date |
| `effective_to` | DATE | | MDM effective end date |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |

---

#### DIM_TEST_METHOD — Analytical Test Methods

**Table:** `l2_2_unified_model.dim_test_method`
**Grain:** One row per test method version (MDM-managed)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `test_method_key` | BIGINT | NOT NULL | Surrogate primary key |
| `test_method_id` | STRING | NOT NULL | MDM-resolved method ID |
| `method_name` | STRING | NOT NULL | Method name |
| `method_number` | STRING | | Method document number (e.g., TM-HPLC-001) |
| `method_version` | STRING | | Method version |
| `method_type` | STRING | | COMPENDIAL \| IN_HOUSE \| VALIDATED \| TRANSFER |
| `technique` | STRING | | HPLC \| GC \| UV-VIS \| KF \| IR \| NMR \| ICP-MS \| etc. |
| `compendia_reference` | STRING | | e.g., USP \<621\>, EP 2.2.29 |
| `is_validated` | BOOLEAN | | Method validation status |
| `is_active` | BOOLEAN | NOT NULL | Active flag |
| `effective_from` | DATE | | MDM effective start date |
| `effective_to` | DATE | | MDM effective end date |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |

---

#### DIM_SITE — Manufacturing / Testing Site

**Table:** `l2_2_unified_model.dim_site`
**Grain:** One row per site (MDM-managed)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `site_key` | BIGINT | NOT NULL | Surrogate primary key |
| `site_id` | STRING | NOT NULL | MDM-resolved site ID |
| `site_name` | STRING | NOT NULL | Site name |
| `site_type` | STRING | | MANUFACTURING \| TESTING \| PACKAGING \| DISTRIBUTION |
| `country_code` | STRING | | ISO 3166-1 alpha-2 country code |
| `country_name` | STRING | | Country name |
| `region_code` | STRING | | US \| EU \| JP \| CN \| ROW |
| `is_active` | BOOLEAN | NOT NULL | Active flag |
| `effective_from` | DATE | | MDM effective start date |
| `effective_to` | DATE | | MDM effective end date |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |

---

#### DIM_MARKET — Market / Region

**Table:** `l2_2_unified_model.dim_market`
**Grain:** One row per market/regulatory region

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `market_key` | BIGINT | NOT NULL | Surrogate primary key |
| `market_code` | STRING | NOT NULL | Market code |
| `market_name` | STRING | NOT NULL | Market name |
| `region_code` | STRING | NOT NULL | US \| EU \| JP \| CN \| ROW |
| `region_name` | STRING | NOT NULL | Region display name |
| `regulatory_authority` | STRING | | Primary regulatory authority (FDA, EMA, PMDA) |
| `is_active` | BOOLEAN | NOT NULL | Active flag |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |

---

### 4.4 Dimension Tables — Conformed (Specifications)

#### DIM_SPECIFICATION — Specification Header / Metadata

**Table:** `l2_2_unified_model.dim_specification`
**Grain:** One row per specification version (SCD Type 2)
**Description:** Captures the header-level attributes of a pharmaceutical specification document — its identity, site/market context, lifecycle status, and linkage to product/material. Date fields are stored as surrogate FKs to `dim_date`.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `spec_key` | BIGINT | NOT NULL | Surrogate primary key |
| `source_specification_id` | STRING | NOT NULL | Source system natural key (LIMS record ID) |
| `spec_number` | STRING | NOT NULL | Specification document number (e.g., SP-DS-2024-001) |
| `spec_version` | STRING | NOT NULL | Version string (e.g., 1.0, 2.1, 3.0) |
| `spec_title` | STRING | | Full specification title |
| `spec_type_code` | STRING | NOT NULL | DS \| DP \| RM \| EXCIP \| INTERMED \| IPC \| CCS |
| `spec_type_name` | STRING | | Drug Substance \| Drug Product \| Raw Material \| Excipient \| Intermediate \| In-Process Control \| Container Closure |
| `product_key` | BIGINT | | FK → dim_product |
| `material_key` | BIGINT | | FK → dim_material |
| `site_key` | BIGINT | | FK → dim_site |
| `market_key` | BIGINT | | FK → dim_market |
| `status_code` | STRING | NOT NULL | DRA \| APP \| SUP \| OBS \| ARCH |
| `status_name` | STRING | | Draft \| Approved \| Superseded \| Obsolete \| Archived |
| `stage_code` | STRING | | DEV \| CLI \| COM (Development / Clinical / Commercial) |
| `dosage_form` | STRING | | Dosage form (denormalized from product for convenience) |
| `strength` | STRING | | Strength string (denormalized from product for convenience) |
| `compendia_reference` | STRING | | Compendia basis (USP, EP, JP, BP) |
| `ctd_section` | STRING | | CTD section reference (e.g., 3.2.S.4.1, 3.2.P.5.1) |
| `effective_start_date_key` | INT | | FK → dim_date (specification effective start) |
| `effective_end_date_key` | INT | | FK → dim_date (specification expiry or supersession date) |
| `approval_date_key` | INT | | FK → dim_date (formal approval date) |
| `approved_by` | STRING | | Approving person name/ID |
| `supersedes_spec_key` | BIGINT | | Self-FK to the dim_specification row this record supersedes |
| `is_current` | BOOLEAN | NOT NULL | SCD2 current row flag |
| `valid_from` | TIMESTAMP | NOT NULL | SCD2 validity start |
| `valid_to` | TIMESTAMP | | SCD2 validity end (NULL = current) |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |

**Slowly Changing Dimension:** Type 2 (full history preserved per version)
**Partition:** `spec_type_code`
**Z-Order:** `spec_number, spec_version`

---

#### DIM_SPECIFICATION_ITEM — Individual Specification Tests

**Table:** `l2_2_unified_model.dim_specification_item`
**Grain:** One row per test/item per specification version
**Description:** Each specification contains ordered test items (e.g., Appearance, Identification, Assay, Dissolution). This table captures the test metadata without limit values.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `spec_item_key` | BIGINT | NOT NULL | Surrogate primary key |
| `source_spec_item_id` | STRING | NOT NULL | Source system natural key (LIMS spec item ID) |
| `spec_key` | BIGINT | NOT NULL | FK → dim_specification |
| `test_method_key` | BIGINT | | FK → dim_test_method |
| `uom_key` | INT | | FK → dim_uom (primary result unit) |
| `test_code` | STRING | | Internal test code (e.g., ASS-001) |
| `test_name` | STRING | NOT NULL | Test name (e.g., Assay, Appearance, Dissolution) |
| `analyte_code` | STRING | | Analyte code (for multi-analyte tests) |
| `parameter_name` | STRING | | Parameter name (may differ from test_name) |
| `test_category_code` | STRING | | PHY \| CHE \| MIC \| BIO \| IMP \| STER \| PACK |
| `test_category_name` | STRING | | Physical \| Chemical \| Microbiological \| Biological \| Impurity \| Sterility \| Packaging |
| `test_subcategory` | STRING | | e.g., Related Substances, Residual Solvents, Heavy Metals |
| `criticality_code` | STRING | | CQA \| CCQA \| NCQA \| KQA \| REPORT |
| `sequence_number` | INT | | Display/reporting order within specification |
| `reporting_type` | STRING | | NUMERIC \| PASS_FAIL \| TEXT \| REPORT_ONLY |
| `result_precision` | INT | | Decimal places for numeric result reporting |
| `is_required` | BOOLEAN | | TRUE = mandatory test; FALSE = conditional |
| `compendia_test_ref` | STRING | | Compendia test reference (e.g., USP \<711\>, EP 2.9.3) |
| `stage_applicability` | STRING | | RELEASE \| STABILITY \| IPC \| BOTH |
| `is_current` | BOOLEAN | NOT NULL | SCD2 current row flag |
| `valid_from` | TIMESTAMP | NOT NULL | SCD2 validity start |
| `valid_to` | TIMESTAMP | | SCD2 validity end (NULL = current) |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |

**Partition:** `test_category_code`
**Z-Order:** `spec_key, test_code`

---

### 4.5 Dimension Tables — Analytical Results

#### DIM_BATCH — Manufacturing Batch / Lot

**Table:** `l2_2_unified_model.dim_batch`
**Grain:** One row per batch (for analytical results linking)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `batch_key` | BIGINT | NOT NULL | Surrogate primary key |
| `batch_number` | STRING | NOT NULL | Manufacturing batch / lot number |
| `batch_system_id` | STRING | | Batch system ID |
| `product_key` | BIGINT | | FK → dim_product |
| `site_key` | BIGINT | | FK → dim_site (manufacturing site) |
| `manufacturing_date` | DATE | | Batch manufacturing date |
| `expiry_date` | DATE | | Batch expiry date |
| `batch_size` | DECIMAL(18,4) | | Batch size value |
| `batch_size_unit` | STRING | | Batch size unit (kg, L, units) |
| `batch_status` | STRING | | RELEASED \| QUARANTINE \| REJECTED \| RECALLED |
| `is_active` | BOOLEAN | NOT NULL | Active flag |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |

---

#### DIM_STABILITY_CONDITION — ICH Stability Storage Condition

**Table:** `l2_2_unified_model.dim_stability_condition`
**Grain:** One row per ICH stability storage condition (static reference)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `condition_key` | INT | NOT NULL | Surrogate primary key |
| `condition_code` | STRING | NOT NULL | 25C60RH \| 30C65RH \| 40C75RH \| 5C \| REFRIG \| FREEZER |
| `condition_name` | STRING | NOT NULL | Display name (e.g., 25°C / 60% RH) |
| `temperature_celsius` | DECIMAL(5,1) | | Temperature in Celsius |
| `humidity_pct` | DECIMAL(5,1) | | Relative humidity percentage |
| `ich_condition_type` | STRING | NOT NULL | LONG_TERM \| ACCELERATED \| INTERMEDIATE \| REFRIGERATED \| FROZEN |
| `is_active` | BOOLEAN | NOT NULL | Active flag |

---

#### DIM_TIMEPOINT — Stability Study Time Point

**Table:** `l2_2_unified_model.dim_timepoint`
**Grain:** One row per stability study time point (static reference)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `timepoint_key` | INT | NOT NULL | Surrogate primary key |
| `timepoint_code` | STRING | NOT NULL | T0 \| T1M \| T3M \| T6M \| T9M \| T12M \| T18M \| T24M \| T36M |
| `timepoint_months` | INT | NOT NULL | Time point in months (0, 1, 3, 6, 9, ...) |
| `timepoint_name` | STRING | NOT NULL | Display name (e.g., Initial, 3 Months, 6 Months) |
| `display_order` | INT | NOT NULL | Sort order for display |
| `is_active` | BOOLEAN | NOT NULL | Active flag |

---

#### DIM_INSTRUMENT — Analytical Instrument / Equipment

**Table:** `l2_2_unified_model.dim_instrument`
**Grain:** One row per instrument

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `instrument_key` | BIGINT | NOT NULL | Surrogate primary key |
| `instrument_id` | STRING | NOT NULL | Instrument ID (equipment tag) |
| `instrument_name` | STRING | NOT NULL | Instrument name or model |
| `instrument_type` | STRING | | HPLC \| GC \| IR \| UV_VIS \| DISSOLUTION \| BALANCE \| PH_METER \| OTHER |
| `is_active` | BOOLEAN | NOT NULL | Active flag |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |

---

### 4.6 Fact Tables

#### FACT_SPECIFICATION_LIMIT — Normalized Specification Limits

**Table:** `l2_2_unified_model.fact_specification_limit`
**Grain:** One row per **limit type** per **specification item** per **stage/time point**
**Description:** The central fact of the specification domain. Stores all limit values (NOR, PAR, AC, Alert, Action, IPC) in a normalized structure. A single test item will have multiple rows — one per limit type applicable. Date references use surrogate FKs to `dim_date`. SPC process capability fields (Cpk, sample size) sourced from the recipe system.

**Example:** Assay test in Drug Product Release specification:
- Row 1: limit_type=AC, lower=98.0, upper=102.0, stage=RELEASE
- Row 2: limit_type=NOR, lower=99.0, upper=101.0, stage=RELEASE
- Row 3: limit_type=PAR, lower=97.0, upper=103.0, stage=RELEASE
- Row 4: limit_type=AC, lower=95.0, upper=105.0, stage=STABILITY, time_point=T24M

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `spec_limit_key` | BIGINT | NOT NULL | Surrogate primary key |
| `spec_key` | BIGINT | NOT NULL | FK → dim_specification |
| `spec_item_key` | BIGINT | NOT NULL | FK → dim_specification_item |
| `limit_type_key` | INT | NOT NULL | FK → dim_limit_type |
| `uom_key` | INT | | FK → dim_uom (limit-specific unit) |
| `effective_start_date_key` | INT | | FK → dim_date (limit effective start) |
| `effective_end_date_key` | INT | | FK → dim_date (limit effective end) |
| `lower_limit_value` | DECIMAL(18,6) | | Lower bound numeric value |
| `upper_limit_value` | DECIMAL(18,6) | | Upper bound numeric value |
| `target_value` | DECIMAL(18,6) | | Nominal/target value |
| `limit_range_width` | DECIMAL(18,6) | | Calculated: upper_limit_value − lower_limit_value |
| `lower_limit_operator` | STRING | | NLT \| GT \| GTE \| NONE |
| `upper_limit_operator` | STRING | | NMT \| LT \| LTE \| NONE |
| `limit_text` | STRING | | Non-numeric limit (e.g., "Clear, colorless solution") |
| `limit_description` | STRING | | Full formatted limit expression (e.g., "NLT 98.0% and NMT 102.0%") |
| `limit_basis` | STRING | | AS_IS \| ANHYDROUS \| AS_LABELED \| DRIED_BASIS |
| `stage_code` | STRING | | RELEASE \| STABILITY \| IPC \| BOTH |
| `stability_time_point` | STRING | | T0 \| T3M \| T6M \| T12M \| T24M \| T36M |
| `stability_condition` | STRING | | 25C60RH \| 30C65RH \| 40C75RH \| REFRIG |
| `calculation_method` | STRING | | SPC method: 3_SIGMA \| CPK \| EWMA \| CUSUM \| MANUAL |
| `sample_size` | INT | | SPC sample size used for limit derivation |
| `last_calculated_date_key` | INT | | FK → dim_date (last SPC recalculation) |
| `is_in_filing` | BOOLEAN | | TRUE if this limit appears in the regulatory filing |
| `regulatory_basis` | STRING | | ICH Q6A \| USP \<xxx\> \| EP x.x.x |
| `source_limit_id` | STRING | | Source system natural key |
| `is_current` | BOOLEAN | NOT NULL | Current version flag |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |

**Partition:** `stage_code`
**Z-Order:** `spec_item_key, limit_type_key`

---

#### FACT_ANALYTICAL_RESULT — Stability Analytical Test Results

**Table:** `l2_2_unified_model.fact_analytical_result`
**Grain:** One row per **batch** × **test item** × **stability condition** × **time point**
**Description:** Analytical test results from vendor/Excel stability studies. Linked to the specification dimension for AC comparison and OOS/OOT determination.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `analytical_result_key` | BIGINT | NOT NULL | Surrogate primary key |
| `batch_key` | BIGINT | NOT NULL | FK → dim_batch |
| `spec_key` | BIGINT | | FK → dim_specification (linked spec, if known) |
| `spec_item_key` | BIGINT | | FK → dim_specification_item (linked test item, if known) |
| `condition_key` | INT | | FK → dim_stability_condition |
| `timepoint_key` | INT | | FK → dim_timepoint |
| `instrument_key` | BIGINT | | FK → dim_instrument |
| `uom_key` | INT | | FK → dim_uom |
| `test_date_key` | INT | | FK → dim_date (test/analysis date) |
| `result_value` | DECIMAL(18,6) | | Numeric test result |
| `result_text` | STRING | | Text result (for non-numeric tests) |
| `result_status_code` | STRING | NOT NULL | PASS \| FAIL \| OOS \| OOT \| PENDING \| REPORT |
| `reported_lower_limit` | DECIMAL(18,6) | | Vendor-reported lower limit (for reference) |
| `reported_upper_limit` | DECIMAL(18,6) | | Vendor-reported upper limit (for reference) |
| `reported_target` | DECIMAL(18,6) | | Vendor-reported target |
| `is_oos` | BOOLEAN | | TRUE if result is Out of Specification |
| `is_oot` | BOOLEAN | | TRUE if result is Out of Trend |
| `analyst_name` | STRING | | Analyst who performed the test |
| `reviewer_name` | STRING | | Reviewer who approved the result |
| `lab_name` | STRING | | Laboratory name |
| `report_id` | STRING | | Analytical report or CoA ID |
| `coa_number` | STRING | | Certificate of Analysis number |
| `stability_study_id` | STRING | | Stability study identifier |
| `source_result_id` | STRING | | Source system natural key |
| `is_current` | BOOLEAN | NOT NULL | Current version flag |
| `load_timestamp` | TIMESTAMP | NOT NULL | ETL load timestamp |

**Partition:** `result_status_code`
**Z-Order:** `batch_key, spec_item_key`

---

## 5. L2.2 Denormalized / Semi-Normalized Tables

### DSPEC_SPECIFICATION — Denormalized Specification + Acceptance Criteria

**Table:** `l2_2_unified_model.dspec_specification`
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

**Schema:** `l3_data_product`

### OBT_SPECIFICATION_CTD — One Big Table for CTD Regulatory Filing

**Table:** `l3_data_product.obt_specification_ctd`
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

**Table:** `l3_data_product.obt_acceptance_criteria`
**Grain:** One row per test per specification (AC, NOR, PAR limits pivoted)
**Description:** Focused OBT containing regulatory acceptance criteria with pivoted NOR and PAR limits for hierarchy comparison. Includes computed metrics: `nor_tightness_pct` (NOR width / AC width), `par_vs_ac_factor` (PAR width / AC width), and `is_hierarchy_valid` (PAR ≥ AC ≥ NOR check). Optimized for specification comparison, gap analysis, and CTD 3.2.P.5.6 justification.

**Partitions:** `spec_type_code`

---

### OBT_STABILITY_RESULTS — Stability Analytical Results

**Table:** `l3_data_product.obt_stability_results`
**Grain:** One row per **batch** × **test** × **storage condition** × **time point**
**Description:** Fully denormalized stability analytical results OBT. Combines batch, product/material, specification, test, ICH condition, time point, result, and derived flags into a single flat table for:
- Stability trend analysis and expiry prediction
- OOS/OOT investigation support
- Regulatory stability data packages (ICH Q1A-E)
- BI dashboards (shelf-life, degradation, conformance rate)

Includes both vendor-reported limits and specification AC limits for direct comparison.

| Column Group | Key Columns |
|---|---|
| Batch | batch_number, manufacturing_date, expiry_date, batch_size |
| Product / Material | product_name, dosage_form, strength, material_name, material_type |
| Site | site_name, country_code, lab_name |
| Specification | spec_number, spec_version, spec_type_code |
| Test | test_name, test_code, test_category_code, criticality_code, method_name, technique |
| Stability context | stability_study_id, storage_condition_code, storage_condition_name, ich_condition_type, time_point_code, time_point_months |
| Result | result_value, result_text, uom_code, result_status_code |
| Limits | reported_lower/upper_limit, spec_ac_lower/upper_limit |
| Flags | is_oos, is_oot |
| Audit | analyst_name, reviewer_name, report_id, coa_number, test_date, pull_date |

**Partitions:** `storage_condition_code`

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

**Specification domain:**

| Table | Partition Column(s) | Z-Order Column(s) |
|-------|--------------------|--------------------|
| `dim_specification` | `spec_type_code` | `spec_number, spec_version` |
| `dim_specification_item` | `test_category_code` | `spec_key, test_code` |
| `fact_specification_limit` | `stage_code` | `spec_item_key, limit_type_key` |
| `dspec_specification` | `spec_type_code` | `spec_number, test_code` |
| `obt_specification_ctd` | `spec_type_code` | `spec_number, test_name` |
| `obt_acceptance_criteria` | `spec_type_code` | `spec_number, test_name` |

**Analytical results / stability domain:**

| Table | Partition Column(s) | Z-Order Column(s) |
|-------|--------------------|--------------------|
| `fact_analytical_result` | `result_status_code` | `batch_key, spec_item_key` |
| `obt_stability_results` | `storage_condition_code` | `batch_number, test_name` |

**Source layers:**

| Table | Partition Column(s) |
|-------|---------------------|
| `raw_lims_*` | `_source_system` |
| `raw_process_recipe` | `_source_system` |
| `raw_pdf_specification` | `_source_system` |
| `raw_vendor_analytical_results` | `_source_system` |
| `src_lims_specification` | `spec_type_code` |
| `src_lims_spec_item` | `test_category_code` |
| `src_lims_spec_limit` | `limit_type_code` |
| `src_process_recipe` | `limit_type_code` |
| `src_pdf_specification` | `limit_type_code` |
| `src_vendor_analytical_results` | `storage_condition_code` |

**Optimization notes:**
- Enable Delta Lake `OPTIMIZE` and `VACUUM` on a weekly schedule
- Use liquid clustering for high-cardinality fact tables in Unity Catalog
- Enable Delta Change Data Feed (CDF) on `dim_specification`, `dim_specification_item`, `fact_specification_limit`, and `fact_analytical_result` for incremental downstream processing
- `obt_specification_ctd` should be rebuilt as a full refresh (not incremental) to ensure CTD accuracy
- `obt_stability_results` should be rebuilt as a full refresh per storage condition partition

---

## 11. Data Lineage Summary

### Specification Domain

```
LIMS / Recipe System / PDF/SOP Documents
    │
    ▼ [Ingest — no transform, all STRING]
L1: raw_lims_specification           ← LIMS spec headers
    raw_lims_spec_item               ← LIMS spec tests
    raw_lims_spec_limit              ← LIMS spec limits (all limit types)
    raw_process_recipe               ← Recipe NOR/PAR/Target/Alert/Action + SPC
    raw_pdf_specification            ← Transcribed PDF/SOP specs (flat, one row per test-limit)
    │
    ▼ [Cleanse, type-cast, code mapping, deduplication]
L2.1: src_lims_specification         ← Typed specs with DQ flags
      src_lims_spec_item             ← Typed items with category/criticality mapping
      src_lims_spec_limit            ← Typed limits with operator standardization
      src_process_recipe             ← Typed recipe limits with SPC fields
      src_pdf_specification          ← Typed PDF specs with DQ flags
      │
      ▼ [MDM resolution, surrogate keys, SCD2, cross-source harmonization]
L2.2: dim_specification              ← SCD2 spec header (linked to product/material/site/market)
      dim_specification_item         ← SCD2 test items
      dim_limit_type                 ← Reference: AC/NOR/PAR/ALERT/ACTION/IPC
      dim_test_method                ← MDM: analytical methods
      dim_product                    ← MDM: drug products
      dim_material                   ← MDM: drug substances & materials
      dim_site                       ← MDM: manufacturing/testing sites
      dim_market                     ← MDM: market/regulatory regions
      dim_regulatory_context         ← Reference: regulatory contexts
      dim_uom                        ← Reference: units of measure
      dim_date                       ← Reference: calendar dates
      fact_specification_limit       ← All limit types, normalized, one row per limit-type × item
      dspec_specification            ← Wide pivoted table (all limits as columns per item)
      │
      ▼ [Flatten, filter is_current + is_in_filing, CTD-align]
L3:   obt_specification_ctd          ← CTD-ready OBT (grain: spec × item × limit type)
      obt_acceptance_criteria        ← AC OBT with hierarchy metrics (PAR/AC/NOR pivoted)
```

### Analytical Results / Stability Domain

```
Vendor Excel / CRO LIMS
    │
    ▼ [Ingest — no transform, all STRING]
L1: raw_vendor_analytical_results    ← Stability results (batch × test × condition × timepoint)
    │
    ▼ [Cleanse, type-cast, code mapping, DQ flags]
L2.1: src_vendor_analytical_results  ← Typed stability results with OOS/OOT pre-classification
      │
      ▼ [MDM resolution, dim lookup, surrogate keys]
L2.2: dim_batch                      ← Manufacturing batches (linked to product + site)
      dim_stability_condition        ← Reference: ICH storage conditions
      dim_timepoint                  ← Reference: stability time points
      dim_instrument                 ← Analytical instruments
      fact_analytical_result         ← One result per batch × test × condition × timepoint
      │
      ▼ [Join to spec limits, flatten, compute OOS/OOT]
L3:   obt_stability_results          ← Fully denormalized stability OBT with spec AC limits
```

---

*End of Specification — Version 2.0*
