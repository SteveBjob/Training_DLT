# Databricks notebook source
# MAGIC %run "/Users/chanwitt@ais.co.th/DLT/Training1/1_dlt_pipeline/0.configuration"

# COMMAND ----------

print(catalog_name)
print(schema_bronze)
print(schema_silver)
print(schema_gold)
print(schema_dlt)

# COMMAND ----------

# MAGIC %md
# MAGIC ####bronze

# COMMAND ----------

df = spark.sql(f"select * from {sample_df}")
print(df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ####DLT

# COMMAND ----------

stream_good_record = "stream_good_record"
stream_bad_record = "stream_bad_record"
materialrize_good_record_trf = "materialrize_good_record_transform"
stream_good_record_trf = "stream_good_record_transform"

# COMMAND ----------

dlt_good_df = catalog_name + "." + schema_dlt + "." + stream_good_record
dlt_bad_df = catalog_name + "." + schema_dlt + "." + stream_bad_record
dlt_materialrize_good_record_trf = catalog_name + "." + schema_dlt + "." + materialrize_good_record_trf
dlt_stream_good_record_trf = catalog_name + "." + schema_dlt + "." + stream_good_record_trf

# COMMAND ----------

df = spark.sql(f"select * from {dlt_good_df}")
print(df.count())

# COMMAND ----------

df = spark.sql(f"select * from {dlt_bad_df}")
print(df.count())

# COMMAND ----------

df = spark.sql(f"select * from {dlt_materialrize_good_record_trf}")
print(df.count())

# COMMAND ----------

df = spark.sql(f"select * from {dlt_stream_good_record_trf}")
print(df.count())

# COMMAND ----------


