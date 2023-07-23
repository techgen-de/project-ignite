# Databricks notebook source
# MAGIC %run ../common/Utils

# COMMAND ----------

confluentClusterName = "DE_cluster"
confluentBootstrapServers = "pkc-lgwgm.eastus2.azure.confluent.cloud:9092"
confluentApiKey = dbutils.secrets.get(scope= "TechgenScope", key = "confluentApiKey")
confluentSecret = dbutils.secrets.get(scope= "TechgenScope", key = "confluentSecret")
confluentRegistryApiKey = ""
confluentRegistrySecret = ""
confluentTopicName = "ecommerce_orders"
schemaRegistryUrl = "https://psrc-gq7pv.westus2.azure.confluent.cloud"
detlaTablePath = ""
checkpointPath = ""

# COMMAND ----------

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", confluentBootstrapServers) \
    .option("subscribe", confluentTopicName) \
    .option("startingOffsets", "earliest") \
    .load()


# Extract the value column containing the actual data
df = df.selectExpr("CAST(value AS STRING)")

# Write the streaming data to ADLS
df.writeStream \
    .format("csv") \
    .option("header", "true") \
    .option("processingTime", "60 seconds") \
    .option("checkpointLocation", BronzeCheckpointPath) \
    .start(BronzePath)
