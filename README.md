# pharma-quality

Unified Data Model for Pharmaceutical Quality — Specifications Domain.

## Overview

This repository defines the **Unified Data Model (UDM)** for the Pharmaceutical Quality Specifications domain on Databricks (Delta Lake / Unity Catalog). The primary output is a regulatory-filing-ready data product aligned to ICH CTD Module 3 (3.2.S.4.1 Drug Substance and 3.2.P.5.1 Drug Product specification tables).

## Architecture

The solution follows a layered Medallion architecture:

```
L1 (Raw) → L2.1 (Source Conform) → L2.2 (Unified Model) → L3 (Final Data Product)
```

| Layer | Schema | Description |
|-------|--------|-------------|
| L1 | `l1_raw` | Raw ingestion from source systems, immutable |
| L2.1 | `l2_1_<source>` | Source-specific cleansing and typing |
| L2.2 | `l2_2_unified_model` | Star schema + denormalized tables (this repo) |
| L3 | `l3_data_product` | One Big Table (OBT), CTD-ready final products |


## Naming Convention

To keep the platform model consistent and professional for enterprise governance, schema naming uses a predictable pattern:

- `l<layer>_<sub_layer>_<domain_or_purpose>` for business-conformed layers (example: `l2_2_unified_model`).
- `l2_1_<source_code>` for source-conform schemas (example: `l2_1_scl`).
- Product-facing L3 schemas emphasize consumption intent (example: `l3_data_product`).

## Repository Structure

```
pharma-quality/
├── docs/
│   └── unified_data_model_specification.md   # Full data model specification
├── ddl/
│   ├── l2_2_unified_model/
│   │   ├── dimensions/
│   │   │   ├── dim_specification.sql          # Spec header / metadata (SCD2)
│   │   │   ├── dim_specification_item.sql     # Individual tests per spec (SCD2)
│   │   │   ├── dim_limit_type.sql             # Limit type reference (AC/NOR/PAR/Alert/Action)
│   │   │   ├── dim_test_method.sql            # Analytical methods
│   │   │   ├── dim_product.sql                # Drug product (MDM)
│   │   │   ├── dim_material.sql               # Drug substance / material (MDM)
│   │   │   ├── dim_regulatory_context.sql     # Regulatory filing context
│   │   │   ├── dim_uom.sql                    # Units of measure
│   │   │   └── dim_date.sql                   # Date dimension
│   │   ├── facts/
│   │   │   └── fact_specification_limit.sql   # Normalized limits fact table
│   │   └── denormalized/
│   │       └── dspec_specification.sql        # Pivoted spec + all limit types
│   └── l3_final/
│       ├── obt_specification_ctd.sql          # CTD-aligned OBT (primary regulatory output)
│       └── obt_acceptance_criteria.sql        # AC-focused OBT for comparison/analysis
└── README.md
```

## Key Design Decisions

**Normalized limits fact table:** All limit types (AC, NOR, PAR, Alert, Action, IPC) are stored in a single `fact_specification_limit` table with `limit_type_key` as a differentiator. This avoids schema changes when new limit types are added and enables cross-limit-type comparison queries.

**SCD Type 2 on specifications:** Full version history is preserved — each specification version creates a new row with `valid_from`/`valid_to` timestamps.

**Limit operators as codes:** Pharmaceutical limit operators (NLT = Not Less Than, NMT = Not More Than) are stored explicitly to enable correct CTD text generation (`NLT 98.0% and NMT 102.0%`).

**Three limit tiers:** PAR ≥ AC ≥ NOR. Hierarchy validation is computed in `dspec_specification` and propagated to L3.

## CTD Mapping

| CTD Section | Source Table | Filter |
|-------------|-------------|--------|
| 3.2.S.4.1 (DS Specification) | `l3_data_product.obt_specification_ctd` | `spec_type_code='DS'` |
| 3.2.P.5.1 (DP Specification) | `l3_data_product.obt_specification_ctd` | `spec_type_code='DP'` |
| 3.2.P.5.6 (Spec Justification) | `l3_data_product.obt_acceptance_criteria` | AC vs PAR comparison |

## Documentation

See [docs/unified_data_model_specification.md](docs/unified_data_model_specification.md) for the full specification including:
- Entity relationship diagram
- Complete column data dictionaries
- Business rules and limit definitions
- Partition and optimization strategy
- Data lineage summary
