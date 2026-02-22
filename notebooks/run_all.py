# Databricks notebook source
# MAGIC %md
# MAGIC # Pharma Quality — Run All Pipeline
# MAGIC Orchestrator notebook that runs the full pipeline in sequence:
# MAGIC
# MAGIC | Step | Notebook | Description |
# MAGIC |------|----------|-------------|
# MAGIC | 0 | 00_setup/create_schemas | Create catalog and schemas |
# MAGIC | 1 | 01_ddl/01_l1_raw_tables | L1 raw table DDLs |
# MAGIC | 2 | 01_ddl/02_l2_1_source_conform | L2.1 source conform DDLs |
# MAGIC | 3 | 01_ddl/03_l2_2_unified_model | L2.2 unified model DDLs |
# MAGIC | 4 | 01_ddl/04_l3_final_tables | L3 final product DDLs |
# MAGIC | 5 | 02_seed_data/01_seed_reference_data | Reference dimension seed data |
# MAGIC | 6 | 02_seed_data/02_seed_dim_date | Calendar date dimension population |
# MAGIC | 7 | 02_seed_data/03_seed_e2e_sample | End-to-end sample data |
# MAGIC | 8 | 03_data_load/01_populate_dspec | Populate denormalized dspec |
# MAGIC | 9 | 03_data_load/02_populate_l3 | Populate L3 OBT tables |
# MAGIC | 10 | 04_validation/01_validation_queries | Run validation checks |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Set the base path for notebooks relative to this orchestrator
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
# MAGIC ## Step 5-6: Seed Reference Data

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/02_seed_data/01_seed_reference_data", timeout_seconds=120)

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/02_seed_data/02_seed_dim_date", timeout_seconds=300)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Load E2E Sample Data

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/02_seed_data/03_seed_e2e_sample", timeout_seconds=300)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8-9: Data Transformations

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/03_data_load/01_populate_dspec", timeout_seconds=600)

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/03_data_load/02_populate_l3", timeout_seconds=600)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Validation

# COMMAND ----------

dbutils.notebook.run(f"{base_path}/04_validation/01_validation_queries", timeout_seconds=300)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete
# MAGIC All steps have been executed successfully. Review the validation notebook output for data quality checks.
