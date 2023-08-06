# Databricks notebook source
# MAGIC %run ../common/Utils

# COMMAND ----------

orderItems = spark.read\
    .format("CSV") \
    .schema('''order_item_id int, 
            order_item_order_id int, 
            order_item_product_id int, 
            order_item_quantity int,
            order_item_subtotal float,
            order_item_product_price float
         ''') \
    .load("abfss://ecommerce@degroup1.dfs.core.windows.net/Bronze/retail_db/order_items/part-00000")

# COMMAND ----------

orderItems.repartition(1).write.format("delta").mode("overwrite").save("abfss://ecommerce@degroup1.dfs.core.windows.net/Silver/retail_db/orderItems")

# COMMAND ----------

orders = spark.read \
    .format("CSV") \
    .schema('''orders_id int, 
            orders_date date, 
            order_customer_id int, 
            order_status string
         ''') \
    .load("abfss://ecommerce@degroup1.dfs.core.windows.net/Bronze/retail_db/orders/part-00000")

# COMMAND ----------

orders.repartition(1).write.format("delta").mode("overwrite").save("abfss://ecommerce@degroup1.dfs.core.windows.net/Silver/retail_db/orders") 

# COMMAND ----------

orders.select("order_status").distinct().show()

# COMMAND ----------

departments = spark.read \
    .format("CSV") \
    .schema('''departments_id int, 
            departments_name string 
         ''') \
    .load("abfss://ecommerce@degroup1.dfs.core.windows.net/Bronze/retail_db/departments/part-00000")

# COMMAND ----------

display(departments)

# COMMAND ----------

customers = spark.read \
    .format("CSV") \
     .schema('''customer_id int, 
            customer_firstname string, 
            customer_lastname string, 
            customer_age string,
            customer_sex string,
            customer_address string,
            customer_city string,
            customer_citycode string,
            customer_orderid int
         ''') \
    .load("abfss://ecommerce@degroup1.dfs.core.windows.net/Bronze/retail_db/customers/part-00000")

# COMMAND ----------

display(customers)

# COMMAND ----------

categories= spark.read\
    .format("CSV")\
        .schema('''categories_id int, 
            categories_qty int, 
            categories_group string
         ''') \
    .load("abfss://ecommerce@degroup1.dfs.core.windows.net/Bronze/retail_db/categories/part-00000")

# COMMAND ----------

display(categories)

# COMMAND ----------

products = spark.read \
    .format("CSV") \
    .schema('''product_id int, 
            product_quantity int, 
            product_size string,
            product_owner string, 
            product_price float,
            product_site string
         ''') \
    .load("abfss://ecommerce@degroup1.dfs.core.windows.net/Bronze/retail_db/products/part-00000")

# COMMAND ----------

pip install msal

# COMMAND ----------

import msal
import json

# COMMAND ----------

!pip install azure-identity azure-storage-file-datalake


# COMMAND ----------

pip install --upgrade pip

# COMMAND ----------

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql import SparkSession

# COMMAND ----------

applicationID = dbutils.secrets.get(scope = "TechgenScope", key="ApplicationId")

authenticationKey = dbutils.secrets.get(scope = "TechgenScope", key="ApplicationSecret")

tenantKey = dbutils.secrets.get(scope = "TechgenScope", key="TenantId")

credential = ClientSecretCredential(tenant_id, applicationID, authenticationKey)

# COMMAND ----------

scope = ['https://database.windows.net/.default']
applicationID = dbutils.secrets.get(scope = "TechgenScope", key="ApplicationId")

authenticationKey = dbutils.secrets.get(scope = "TechgenScope", key="ApplicationSecret")

tenantKey = dbutils.secrets.get(scope = "TechgenScope", key="TenantId")
authority = "https://login.windows.net/" + tenantKey

context = msal.ConfidentialClientApplication(applicationID,authenticationKey,authority)
token = context.acquire_token_for_client(scopes=scope)
access_token = token["access_token"]

# COMMAND ----------

synapse_sql_url = "jdbc:sqlserver://de-group-ondemand.sql.azuresynapse.net"
database_name = "DataExplorationDB"
db_table = "dbo.Test "
encrypt = "true"
host_name_in_certificate = "*.sql.azuresynapse.net"

# COMMAND ----------

Df = spark.read \
.format("com.microsoft.sqlserver.jdbc.spark") \
.option("url", synapse_sql_url) \
.option("dbtable", db_table) \
.option("databaseName", database_name) \
.option("accessToken", access_token) \
.option("encrypt", "true") \
.option("hostNameInCertificate", host_name_in_certificate) \
.load()
             

# COMMAND ----------

display(Df)

# COMMAND ----------

df.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .option("url", synapse_sql_url) \
    .option("dbtable", table_name) \
    .option("databaseName", database_name) \
    .option("accessToken", access_token) \
    .option("encrypt", "true") \
    .option("hostNameInCertificate", host_name_in_certificate) \
    .mode("overwrite") \
    .save()


# COMMAND ----------

delta_file_path = "abfss://ecommerce@degroup1.dfs.core.windows.net/Silver/retail_db/orderItems"


# COMMAND ----------

spark = SparkSession.builder.appName("DeltaTableCreation").getOrCreate()
delta_df = spark.read.format("delta").load(delta_file_path)


# COMMAND ----------

synapse_workspace = "de-group"
sql_pool = "de-group-ondemand.sql.azuresynapse.net"
table_name = "OrderItems"

# Connect to Synapse SQL pool
synapse_sql_url = f"jdbc:sqlserver://{synapse_workspace}.sql.azuresynapse.net:1433;database={sql_pool};"
synapse_sql_properties = {
    "user": "sunny",
    "password": "Abcd4321",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# COMMAND ----------

# Save Delta DataFrame as an external table
delta_df.write \
    .format("synapse.sql") \
    .option("url", synapse_sql_url) \
    .option("dbTable", table_name) \
    .options(**synapse_sql_properties) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

delta_df.write \
    .format("com.databricks.spark.sqldw") \
    .option("url", synapse_sql_url) \
    .option("forwardSparkAzureStorageCredentials", "true") \
    .option("dbTable", table_name) \
    .options(**synapse_sql_properties) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

Df = spark.read \
             .format("com.microsoft.sqlserver.jdbc.spark") \ ## this is from the spark sql connector we installed in step 1
             .option("url", synapse_sql_url) \
             .option("dbtable", db_table) \
             .option("databaseName", database_name) \
             .option("accessToken", access_token) \
             .option("encrypt", "true") \
             .option("hostNameInCertificate", host_name_in_certificate) \
             .load()
             
display(Df)
