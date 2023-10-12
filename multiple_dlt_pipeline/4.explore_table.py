# Databricks notebook source
# MAGIC %run "/Users/chanwitt@ais.co.th/DLT/Training1/multiple_dlt_pipeline/0.configuration"

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
# MAGIC ####silver

# COMMAND ----------

stream_good_record = "stream_good_record"
stream_bad_record = "stream_bad_record"

silver_good_df = catalog_name + "." + schema_silver + "." + stream_good_record
silver_bad_df = catalog_name + "." + schema_silver + "." + stream_bad_record

# COMMAND ----------

df = spark.sql(f"select * from {sample_df}")
print(df.count())

# COMMAND ----------

df = spark.sql(f"select * from {sample_df}")
print(df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ####gold

# COMMAND ----------

materialrize_good_record_trf = "materialrize_good_record_transform"
stream_good_record_trf = "stream_good_record_transform"


gold_mat_df = catalog_name + "." + schema_gold + "." + materialrize_good_record_trf
gold_stream_df = catalog_name + "." + schema_gold + "." + stream_good_record_trf

# COMMAND ----------

df = spark.sql(f"select * from {gold_mat_df}")
print(df.count())

# COMMAND ----------

df = spark.sql(f"select * from {gold_stream_df}")
print(df.count())
