# Databricks notebook source
catalog_name = "ctl_bii_dg_test"
schema_bronze = "training_bronze"
schema_silver = "training_silver"
schema_gold = "training_gold"
schema_dlt = "training_dlt"

# COMMAND ----------

sample_df = catalog_name + "." + schema_bronze + "." + "sample_stream_source_managed_table"

# COMMAND ----------


