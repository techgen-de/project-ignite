# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##Databricks Lakehouse Platform Utilities

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, StructField, StructType, DateType

# COMMAND ----------

## Start spark session
def create_spark_session():
    return SparkSession.builder \
        .appName("EcomDataLoad") \
        .getOrCreate()

# COMMAND ----------

## Start spark session
#spark = SparkSession \
#.builder \
#.appName("Data Engineering") \
#.getOrCreate()

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
Parquet = "parquet_data_2"
SubFolder = "Data"
Checkpoint = "Checkpoint"
mountpoint = "/mnt"
BronzePath = "abfss://" +adlsContainerName+"@"+adlsAccountName+".dfs.core.windows.net/"+BronzeFolderName+"/"+Parquet+"/"
SilverPath = "abfss://" +adlsContainerName+"@"+adlsAccountName+".dfs.core.windows.net/"+SilverFolderName+"/"+SubFolder
GoldPath = "abfss://" +adlsContainerName+"@"+adlsAccountName+".dfs.core.windows.net/"+GoldFolderName+"/"+SubFolder+"/"

# COMMAND ----------

BronzeCheckpointPath = "abfss://" +adlsContainerName+"@"+adlsAccountName+".dfs.core.windows.net/"+BronzeFolderName+"/"+Checkpoint
SilverCheckpointPath = "abfss://" +adlsContainerName+"@"+adlsAccountName+".dfs.core.windows.net/"+SilverFolderName+"/"+Checkpoint
GoldCheckpointPath = "abfss://" +adlsContainerName+"@"+adlsAccountName+".dfs.core.windows.net/"+GoldFolderName+"/"+Checkpoint+"/"

# COMMAND ----------

table_names = ['Product', 'Category', 'Customer', 'Address', 'OrderItems', 'Orders']
