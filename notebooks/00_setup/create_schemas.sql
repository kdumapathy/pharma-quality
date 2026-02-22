-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Schema Bootstrap — Pharma Quality Unified Data Model
-- MAGIC Creates the Unity Catalog and all layer schemas.
-- MAGIC
-- MAGIC | Layer | Schema | Description |
-- MAGIC |-------|--------|-------------|
-- MAGIC | L1 | `l1_raw` | Raw ingestion from source systems, immutable |
-- MAGIC | L2.1 | `l2_1_lims` | Source-specific cleansing and typing (LIMS) |
-- MAGIC | L2.2 | `l2_2_spec_unified` | Star schema + denormalized tables |
-- MAGIC | L3 | `l3_spec_products` | One Big Table (OBT), CTD-ready final products |

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS pharma_quality;

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS l1_raw
COMMENT 'L1 Raw landing zone — immutable source-faithful ingestion';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS l2_1_lims
COMMENT 'L2.1 Source conform layer — LIMS cleansed, typed, and standardised';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS l2_2_spec_unified
COMMENT 'L2.2 Pharma Quality Unified Data Model — Specifications domain';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS l3_spec_products
COMMENT 'L3 Final Data Products — Specifications domain (CTD-ready OBTs)';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify Schemas
-- MAGIC Confirm all schemas were created successfully.

-- COMMAND ----------

SHOW SCHEMAS IN pharma_quality;
