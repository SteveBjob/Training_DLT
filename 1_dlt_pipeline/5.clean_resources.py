# Databricks notebook source
# MAGIC %md
# MAGIC ## Create Sample data on bronze schema
# MAGIC ### Steps to follow
# MAGIC 1. run configuration notebook
# MAGIC 2. use catalog
# MAGIC 3. drop schema bronze silver gold dlt

# COMMAND ----------

# MAGIC %run "/Users/chanwitt@ais.co.th/DLT/Training1/1_dlt_pipeline/0.configuration"

# COMMAND ----------

print(catalog_name)
print(schema_bronze)
print(schema_silver)
print(schema_gold)
print(schema_dlt)

# COMMAND ----------

spark.sql("show catalogs")

# COMMAND ----------

spark.sql(f"use catalog {catalog_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP schema training_bronze CASCADE;
# MAGIC DROP schema training_silver CASCADE;
# MAGIC DROP schema training_gold CASCADE;
# MAGIC DROP schema schema_dlt CASCADE;

# COMMAND ----------


