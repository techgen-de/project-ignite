# Databricks notebook source
# MAGIC %run ../common/Utils

# COMMAND ----------


## Delta Table Names
for table_name in table_names:

## Create Product path
  Product_path = f"{SilverPath}/{Product}"
  Product_SilverCheckpointPath = f"{SilverCheckpointPath}/{Product}"
## Create Product delta table from Product data frame


# COMMAND ----------


## Create Category path
Category_path = f"{SilverPath}/{Category}"
Category_SilverCheckpointPath = f"{SilverCheckpointPath}/{Category}"

## Create Category delta table from Category data frame

