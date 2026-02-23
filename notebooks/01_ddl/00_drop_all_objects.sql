-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Drop All Objects — Full Package Refresh
-- MAGIC Drops all tables in all four schemas in reverse dependency order so the
-- MAGIC deployment can run a clean, idempotent rebuild from scratch.
-- MAGIC
-- MAGIC **Execution order (reverse dependency):**
-- MAGIC 1. L3 `l3_data_product` — final OBTs (no dependents)
-- MAGIC 2. L2.2 `l2_2_unified_model` — facts, denormalized, dimensions
-- MAGIC 3. L2.1 `l2_1_scl` — source conform tables
-- MAGIC 4. L1 `l1_raw` — raw ingestion tables
-- MAGIC
-- MAGIC > **WARNING:** This notebook drops ALL tables. Run only when a full
-- MAGIC > package deployment and data refresh is intended.

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## L3 — Data Products

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop L3 tables

-- COMMAND ----------

DROP TABLE IF EXISTS l3_data_product.obt_stability_results;

-- COMMAND ----------

DROP TABLE IF EXISTS l3_data_product.obt_acceptance_criteria;

-- COMMAND ----------

DROP TABLE IF EXISTS l3_data_product.obt_specification_ctd;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## L2.2 — Unified Data Model

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop L2.2 fact tables

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.fact_analytical_result;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.fact_specification_limit;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop L2.2 denormalized table

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.dspec_specification;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop L2.2 conformed dimensions

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.dim_specification_item;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.dim_specification;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop L2.2 analytical dimensions

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.dim_batch;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.dim_instrument;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop L2.2 MDM dimensions

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.dim_product;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.dim_material;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.dim_test_method;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.dim_site;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.dim_market;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop L2.2 reference dimensions

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.dim_date;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.dim_uom;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.dim_limit_type;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.dim_regulatory_context;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.dim_stability_condition;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_2_unified_model.dim_timepoint;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## L2.1 — Source Conform Layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop L2.1 tables

-- COMMAND ----------

DROP TABLE IF EXISTS l2_1_scl.src_vendor_analytical_results;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_1_scl.src_pdf_specification;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_1_scl.src_process_recipe;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_1_scl.src_lims_spec_limit;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_1_scl.src_lims_spec_item;

-- COMMAND ----------

DROP TABLE IF EXISTS l2_1_scl.src_lims_specification;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## L1 — Raw Ingestion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop L1 tables

-- COMMAND ----------

DROP TABLE IF EXISTS l1_raw.raw_vendor_analytical_results;

-- COMMAND ----------

DROP TABLE IF EXISTS l1_raw.raw_pdf_specification;

-- COMMAND ----------

DROP TABLE IF EXISTS l1_raw.raw_process_recipe;

-- COMMAND ----------

DROP TABLE IF EXISTS l1_raw.raw_lims_spec_limit;

-- COMMAND ----------

DROP TABLE IF EXISTS l1_raw.raw_lims_spec_item;

-- COMMAND ----------

DROP TABLE IF EXISTS l1_raw.raw_lims_specification;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Verify — All Schemas Empty

-- COMMAND ----------

SHOW TABLES IN l3_data_product;

-- COMMAND ----------

SHOW TABLES IN l2_2_unified_model;

-- COMMAND ----------

SHOW TABLES IN l2_1_scl;

-- COMMAND ----------

SHOW TABLES IN l1_raw;
