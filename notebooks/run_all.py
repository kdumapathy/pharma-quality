# Databricks notebook source
# MAGIC %md
# MAGIC # Pharma Quality — Run All Pipeline
# MAGIC Orchestrator notebook that runs the full pipeline in sequence:
# MAGIC
# MAGIC | Step | Notebook | Description |
# MAGIC |------|----------|-------------|
# MAGIC | 0 | 00_setup/create_schemas | Create catalog and schemas |
# MAGIC | 1 | 01_ddl/01_l1_raw_tables | L1 raw table DDLs (LIMS + Recipe) |
# MAGIC | 2 | 01_ddl/02_l2_1_source_conform | L2.1 source conform DDLs (LIMS + Recipe) |
# MAGIC | 3 | 01_ddl/03_l2_2_unified_model | L2.2 unified model DDLs |
# MAGIC | 4 | 01_ddl/04_l3_final_tables | L3 final product DDLs |
# MAGIC | 5 | 02_seed_data/03_seed_e2e_sample | Raw layer sample data (LIMS + Recipe) |
# MAGIC | 6 | 03_data_load/00_populate_reference_data | Reference dims + dim_date |
# MAGIC | 7 | 03_data_load/00_populate_l2_1 | L1 raw → L2.1 source conform |
# MAGIC | 8 | 03_data_load/00_populate_l2_2_dims_facts | L2.1 → L2.2 dims + facts |
# MAGIC | 9 | 03_data_load/01_populate_dspec | L2.2 denormalized dspec |
# MAGIC | 10 | 03_data_load/02_populate_l3 | L3 OBT final products |
# MAGIC | 11 | 04_validation/01_validation_queries | Validation checks |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import os

base_path = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Create Catalog & Schemas

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/00_setup/create_schemas", timeout_seconds=120)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1-4: Create Tables (DDL)

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/01_ddl/01_l1_raw_tables", timeout_seconds=120)

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/01_ddl/02_l2_1_source_conform", timeout_seconds=120)

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/01_ddl/03_l2_2_unified_model", timeout_seconds=300)

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/01_ddl/04_l3_final_tables", timeout_seconds=120)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Load Raw Sample Data (L1 only)

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/02_seed_data/03_seed_e2e_sample", timeout_seconds=300)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Populate Reference Dimensions

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/03_data_load/00_populate_reference_data", timeout_seconds=300)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Populate L2.1 Source Conform

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/03_data_load/00_populate_l2_1", timeout_seconds=600)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Populate L2.2 Dimensions & Facts

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/03_data_load/00_populate_l2_2_dims_facts", timeout_seconds=600)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Populate Denormalized dspec

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/03_data_load/01_populate_dspec", timeout_seconds=600)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Populate L3 Final Products

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/03_data_load/02_populate_l3", timeout_seconds=600)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Validation

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/04_validation/01_validation_queries", timeout_seconds=300)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete
# MAGIC All steps have been executed successfully. Review the validation notebook output for data quality checks.
