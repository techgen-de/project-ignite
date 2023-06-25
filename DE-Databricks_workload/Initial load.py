# Databricks notebook source
# MAGIC %run ./Utils

# COMMAND ----------

df= spark.read.format("CSV").load("abfss://ecommerce@degroup1.dfs.core.windows.net/Bronze/retail_db/customers/part-00000")

# COMMAND ----------

display(df)
