# Databricks notebook source
# MAGIC %md
# MAGIC ## Create DLT script for dlt pipeline
# MAGIC ### Steps to follow
# MAGIC 1. create configuration
# MAGIC 2. create dlt get data from source (bronze schema)
# MAGIC 3. create dlt to filtered step 2
# MAGIC 4. create dlt to transform step 3

# COMMAND ----------

import dlt
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ####create configuration

# COMMAND ----------

source = "test_stream_table"

# COMMAND ----------

stream_good_record = "stream_good_record"
stream_bad_record = "stream_bad_record"
materialrize_good_record_trf = "materialrize_good_record_transform"
stream_good_record_trf = "stream_good_record_transform"

# COMMAND ----------

# MAGIC %md
# MAGIC ####create dlt get data from source (bronze schema)

# COMMAND ----------

@dlt.view(name=f"{source}")
def test_stream_table_catalog():
    df = spark.readStream.table("ctl_bii_dg_test.training_bronze.sample_stream_source_managed_table")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ####create dlt to filtered step 2

# COMMAND ----------

@dlt.table(name=f"{stream_good_record}")
def filtered_test_stream_table_good():
    df = dlt.read_stream(f"{source}")
    df = df.filter(col("random_number") >= 0)
    return df

@dlt.table(name=f"{stream_bad_record}")
def filtered_test_stream_table_bad():
    df = dlt.read_stream(f"{source}")
    df = df.filter(col("random_number") < 0)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ####create dlt to transform step 3

# COMMAND ----------

@dlt.table(name=f"{materialrize_good_record_trf}")
def materialrize_transform():
    df = dlt.read(f"{stream_good_record}")
    df = df.withColumn("random_numberx3", col("random_number")*3)
    return df

@dlt.table(name=f"{stream_good_record_trf}")
def stream_transform():
    df = dlt.read_stream(f"{stream_good_record}")
    df = df.withColumn("random_numberx3", col("random_number")*3)
    return df
