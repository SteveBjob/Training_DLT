# Databricks notebook source
# MAGIC %md
# MAGIC ## Create Catalog and Schema for dlt
# MAGIC ### Steps to follow
# MAGIC 1. run configuration notebook
# MAGIC 2. create catalog
# MAGIC 3. create bronze silver gold dlt schema

# COMMAND ----------

# MAGIC %md
# MAGIC ####run configuration notebook

# COMMAND ----------

# MAGIC %run "/Users/chanwitt@ais.co.th/DLT/Training1/1_dlt_pipeline/0.configuration"

# COMMAND ----------

print(catalog_name)
print(schema_bronze)
print(schema_silver)
print(schema_gold)
print(schema_dlt)

# COMMAND ----------

# MAGIC %md
# MAGIC ####create catalog

# COMMAND ----------

spark.sql('show catalogs').display()

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"use catalog {catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####create bronze silver gold schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_bronze}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_silver}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_gold}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_dlt}")

# COMMAND ----------


