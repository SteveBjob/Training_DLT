# Databricks notebook source
# MAGIC %md
# MAGIC ## Create DLT script for dlt pipeline
# MAGIC ### Steps to follow
# MAGIC 1. create configuration
# MAGIC 2. create dlt get data from source (silver schema)
# MAGIC 3. create dlt to transform step 2 (in dlt pipeline setting schema to gold)

# COMMAND ----------

import dlt
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ####create configuration

# COMMAND ----------

source = "stream_good_record"

# COMMAND ----------

materialrize_good_record_trf = "materialrize_good_record_transform"
stream_good_record_trf = "stream_good_record_transform"

# COMMAND ----------

# MAGIC %md
# MAGIC ####create dlt get data from source (silver schema)

# COMMAND ----------

@dlt.view(name=f"{source}")
def test_stream_table_catalog():
    df = spark.readStream.table("ctl_bii_dg_test.training_silver.stream_good_record")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ####create dlt to transform step 2 (in dlt pipeline setting schema to gold)

# COMMAND ----------

@dlt.table(name=f"{stream_good_record_trf}")
def stream_transform():
    df = dlt.read_stream(f"{source}")
    df = df.withColumn("random_numberx3", col("random_number")*3)
    return df
