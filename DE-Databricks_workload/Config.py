# Databricks notebook source
# MAGIC %md
# MAGIC ##Databricks Lakehouse Platform Configuration
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *


# COMMAND ----------

dbutils.secrets.list("TechgenScope")

# COMMAND ----------

## Databrick Mount 

adlsAccountName = "degroup1"  
adlsContainerName = "ecommerce"
BronzeFolderName = "Bronze"
SilverFolderName = "Silver"
GoldFolderName = "Gold"
mountpoint = "/mnt"

applicationID = dbutils.secrets.get(scope = "TechgenScope", key="ApplicationId")

authenticationKey = dbutils.secrets.get(scope = "TechgenScope", key="ApplicationSecret")

tenantKey = dbutils.secrets.get(scope = "TechgenScope", key="TenantId")


endpoint = "https://login.microsoftonline.com/9511cbf1-667c-419c-be5a-7f61d032b106/oauth2/token"
BronzePath = "abfss://" +adlsContainerName+"@"+adlsAccountName+".dfs.core.windows.net/"+BronzeFolderName+"/"
SilverPath = "abfss://" +adlsContainerName+"@"+adlsAccountName+".dfs.core.windows.net/"+SilverFolderName+"/"
GoldPath = "abfss://" +adlsContainerName+"@"+adlsAccountName+".dfs.core.windows.net/"+GoldFolderName+"/"


configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": applicationID,
          "fs.azure.account.oauth2.client.secret": authenticationKey,
          "fs.azure.account.oauth2.client.endpoint": endpoint,
          "spark.databricks.sqldw.jdbc.service.principal.client.id": applicationID,
           "spark.databricks.sqldw.jdbc.service.principal.client.secret": authenticationKey
          }



dbutils.fs.mount(
source = BronzePath,
mount_point = mountpoint,
extra_configs = configs)


# COMMAND ----------

dbutils.fs.mounts()
