# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##Databricks Lakehouse Platform Utilities

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# COMMAND ----------

## Start spark session
spark = SparkSession \
.builder \
.appName("Data Engineering") \
.getOrCreate()

# COMMAND ----------

applicationID = dbutils.secrets.get(scope = "TechgenScope", key="ApplicationId")

authenticationKey = dbutils.secrets.get(scope = "TechgenScope", key="ApplicationSecret")

tenantKey = dbutils.secrets.get(scope = "TechgenScope", key="TenantId")

spark.conf.set("fs.azure.account.auth.type.degroup1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.degroup1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.degroup1.dfs.core.windows.net", applicationID)
spark.conf.set("fs.azure.account.oauth2.client.secret.degroup1.dfs.core.windows.net", authenticationKey)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.degroup1.dfs.core.windows.net", "https://login.microsoftonline.com/9511cbf1-667c-419c-be5a-7f61d032b106/oauth2/token")

# COMMAND ----------

## Data lake house file paths

adlsAccountName = "degroup1"  
adlsContainerName = "ecommerce"
BronzeFolderName = "Bronze"
SilverFolderName = "Silver"
GoldFolderName = "Gold"
mountpoint = "/mnt"

BronzePath = "abfss://" +adlsContainerName+"@"+adlsAccountName+".dfs.core.windows.net/"+BronzeFolderName+"/"
SilverPath = "abfss://" +adlsContainerName+"@"+adlsAccountName+".dfs.core.windows.net/"+SilverFolderName+"/"
GoldPath = "abfss://" +adlsContainerName+"@"+adlsAccountName+".dfs.core.windows.net/"+GoldFolderName+"/"

# COMMAND ----------

dbutils.fs.ls(BronzePath)

# COMMAND ----------

df= spark.read.format("CSV").load("abfss://ecommerce@degroup1.dfs.core.windows.net/Bronze/retail_db/customers/part-00000")

# COMMAND ----------


