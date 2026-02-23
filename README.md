# pharma-quality

Unified Data Model for Pharmaceutical Quality — Specifications & Analytical Results (Stability).

## Overview

This repository defines the **Unified Data Model (UDM)** for the Pharmaceutical Quality domain on Databricks (Delta Lake / Unity Catalog), covering two sub-domains:

- **Specifications** — Regulatory-filing-ready data product aligned to ICH CTD Module 3 (3.2.S.4.1 Drug Substance and 3.2.P.5.1 Drug Product specification tables)
- **Analytical Results / Stability** — Stability study results with OOS/OOT detection, ICH condition and time-point structure, and batch-level tracking

## Architecture

The solution follows a layered Medallion architecture:

```
L1 (Raw) → L2.1 (Source Conform) → L2.2 (Unified Model) → L3 (Final Data Product)
```

| Layer | Schema | Tables | Description |
|-------|--------|--------|-------------|
| L1 | `l1_raw` | 6 | Raw ingestion from all source systems, immutable (all STRING) |
| L2.1 | `l2_1_scl` | 6 | Source-specific cleansing, type-casting, DQ flags |
| L2.2 | `l2_2_unified_model` | 18 | Star schema — reference dims, MDM dims, conformed dims, facts, denormalized |
| L3 | `l3_data_product` | 3 | One Big Tables (OBT) — CTD filing, acceptance criteria, stability results |


## Naming Convention

To keep the platform model consistent and professional for enterprise governance, schema naming uses a predictable pattern:

- `l<layer>_<sub_layer>_<domain_or_purpose>` for business-conformed layers (example: `l2_2_unified_model`).
- `l2_1_<source_code>` for source-conform schemas (example: `l2_1_scl`).
- Product-facing L3 schemas emphasize consumption intent (example: `l3_data_product`).

## Repository Structure

```
pharma-quality/
├── docs/
│   ├── unified_data_model_specification.md   # Full data model specification (all tables, business rules)
│   └── er_diagrams.md                        # ER diagrams (Mermaid) — Specification & Stability models
├── notebooks/
│   ├── 00_setup/
│   │   └── create_schemas.sql                # Create catalog schemas
│   ├── 01_ddl/
│   │   ├── 00_drop_all_objects.sql           # Drop all tables (clean rebuild)
│   │   ├── 01_l1_raw_tables.sql              # L1 raw tables (6 tables, all STRING)
│   │   ├── 02_l2_1_source_conform.sql        # L2.1 source conform tables (6 tables)
│   │   ├── 03_l2_2_unified_model.sql         # L2.2 star schema (18 tables)
│   │   └── 04_l3_final_tables.sql            # L3 OBTs (3 tables)
│   ├── 02_seed_data/
│   │   └── 03_seed_e2e_sample.sql            # End-to-end sample data seed
│   ├── 03_data_load/
│   │   ├── 00_populate_*.sql                 # L2.1/L2.2/L3 population scripts
│   │   └── 03_truncate_and_load_l1_to_l3.py # Full pipeline orchestrator
│   ├── 04_validation/
│   │   └── 01_validation_queries.sql         # Data quality validation queries
│   └── run_all.py                            # Notebook orchestrator
├── ddl/                                      # Standalone DDL files (Spec domain only)
│   ├── l1_raw/                               # LIMS raw table DDL
│   ├── l2_1_source_conform/lims/             # LIMS conform DDL
│   ├── l2_2_unified_model/                   # Spec star schema DDL
│   └── l3_final/                             # Spec OBT DDL
├── deploy/
│   ├── deploy.py                             # Schema + DDL deployment
│   ├── seed.py                               # Raw data seeding
│   └── full_deploy.py                        # Full pipeline orchestrator
└── README.md
```

## Key Design Decisions

**Normalized limits fact table:** All limit types (AC, NOR, PAR, Alert, Action, IPC) are stored in a single `fact_specification_limit` table with `limit_type_key` as a differentiator. This avoids schema changes when new limit types are added and enables cross-limit-type comparison queries.

**SCD Type 2 on specifications:** Full version history is preserved — each specification version creates a new row with `valid_from`/`valid_to` timestamps.

**Limit operators as codes:** Pharmaceutical limit operators (NLT = Not Less Than, NMT = Not More Than) are stored explicitly to enable correct CTD text generation (`NLT 98.0% and NMT 102.0%`).

**Three limit tiers:** PAR ≥ AC ≥ NOR. Hierarchy validation is computed in `dspec_specification` and propagated to L3. `obt_acceptance_criteria` exposes `nor_tightness_pct` and `par_vs_ac_factor` for specification justification.

**Date surrogate keys:** All date references in L2.2 tables use integer FKs to `dim_date` (YYYYMMDD format) rather than raw DATE columns, enabling consistent date dimension joining and partition elimination.

**Stability domain co-located in L2.2:** `fact_analytical_result` and its analytical dimensions (`dim_batch`, `dim_stability_condition`, `dim_timepoint`, `dim_instrument`) share the `l2_2_unified_model` schema with specification tables, enabling joined queries across results and limits (OOS/OOT determination) without cross-schema joins.

## CTD Mapping

| CTD Section | Source Table | Filter |
|-------------|-------------|--------|
| 3.2.S.4.1 (DS Specification) | `l3_data_product.obt_specification_ctd` | `spec_type_code='DS'` |
| 3.2.P.5.1 (DP Specification) | `l3_data_product.obt_specification_ctd` | `spec_type_code='DP'` |
| 3.2.P.5.6 (Spec Justification) | `l3_data_product.obt_acceptance_criteria` | AC vs PAR comparison |
| Stability Data Package | `l3_data_product.obt_stability_results` | ICH Q1A-E aligned |

## Documentation

| Document | Description |
|----------|-------------|
| [docs/unified_data_model_specification.md](docs/unified_data_model_specification.md) | Full spec — all 33 tables, column dictionaries, business rules, lineage |
| [docs/er_diagrams.md](docs/er_diagrams.md) | ER diagrams (Mermaid) for Specification and Stability data models |

## Deployment Automation

For schema/DDL updates followed by a full pipeline refresh, use:

```bash
python deploy/full_deploy.py
```

This orchestrator runs:
1. `deploy/deploy.py` (schemas + DDL)
2. `deploy/seed.py` (sample raw seed data)
3. `notebooks/03_data_load/*.sql` (L2/L3 loads)
4. `notebooks/04_validation/01_validation_queries.sql` (validation checks)

Helpful options:
- `--dry-run` to preview commands/statements
- `--skip-seed` if raw data is already loaded
- `--skip-validation` to stop after load
- `--validation-only` to run only validation SELECT queries

