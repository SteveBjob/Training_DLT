# Databricks notebook source
# MAGIC %md
# MAGIC ## Create Sample data on bronze schema
# MAGIC ### Steps to follow
# MAGIC 1. run configuration notebook
# MAGIC 2. create sample df
# MAGIC 3. save df to bronze schema
# MAGIC 4. append data to df on bronze schema

# COMMAND ----------

import random

# COMMAND ----------

# MAGIC %md
# MAGIC ####run configuration notebook

# COMMAND ----------

# MAGIC %run "/Users/chanwitt@ais.co.th/DLT/Training1/multiple_dlt_pipeline/0.configuration"

# COMMAND ----------

print(sample_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####create sample df

# COMMAND ----------

# Generate 100 random numbers between -100 and 100
random_numbers = [random.randint(-100, 100) for _ in range(100)]
print(random_numbers)

# Create a DataFrame with a single column named 'random_number'
df = spark.createDataFrame([(num,) for num in random_numbers], ["random_number"])

# COMMAND ----------

# MAGIC %md
# MAGIC ####save df to bronze schema

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable(f"{sample_df}")

# COMMAND ----------

df = spark.sql(f"select * from {sample_df}")
print(df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ####append data to df on bronze schema

# COMMAND ----------

# Generate 100 random numbers between -100 and 100
random_numbers = [random.randint(-100, 100) for _ in range(100)]
print(random_numbers)

# Create a DataFrame with a single column named 'random_number'
df = spark.createDataFrame([(num,) for num in random_numbers], ["random_number"])
df.write.mode("append").format("delta").saveAsTable(f"{sample_df}")

# COMMAND ----------

df = spark.sql(f"select * from {sample_df}")
print(df.count())
