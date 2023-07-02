# Databricks notebook source
# install dependencies

# COMMAND ----------

pip install faker

# COMMAND ----------

pip install confluent-kafka

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
C=checkpointPath = ""

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("EcommerceDataProcessor") \
    .getOrCreate()

# Kafka configurations
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_INPUT_TOPIC = 'ecommerce_orders'
KAFKA_OUTPUT_TOPIC = 'processed_orders'

# Read data from Kafka using structured streaming
input_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_INPUT_TOPIC) \
    .load()


# COMMAND ----------

# Install dependencies: pip install faker confluent-kafka

import time
import random
import json
from faker import Faker
from confluent_kafka import Producer

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'confluentBootstrapServers'
KAFKA_TOPIC = 'ecommerce_orders'

# Faker setup
fake = Faker()

def generate_order():
    return {
        "OrderID": fake.uuid4(),
        "CustomerID": fake.uuid4(),
        "Status": random.choice(["Pending", "Shipped", "Delivered"]),
        "ProductID": fake.uuid4(),
        "Category": random.choice(["Electronics", "Clothing", "Books", "Beauty", "Toys"]),
        "ProductName": fake.word(),
        "Brand": fake.word(),
        "Websites": random.choice(["Amazon", "eBay", "Walmart", "Alibaba"]),
        "Location": fake.city(),
        "Price": round(random.uniform(10, 500), 2),
        "Quantity": random.randint(1, 5),
        "OrderDate": fake.date(),
        "ShippingAddress": fake.address(),
        "PaymentMethod": random.choice(["Credit Card", "PayPal", "Cash on Delivery"])
    }

def delivery_callback(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {str(err)}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_messages(producer, topic):
    while True:
        order = generate_order()
        producer.produce(topic, json.dumps(order), callback=delivery_callback)
        producer.flush()
        time.sleep(1)

if __name__ == "__main__":
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
    }
    producer = Producer(conf)

    try:
        produce_messages(producer, KAFKA_TOPIC)
    except KeyboardInterrupt:
        producer.flush(10)
        producer.close()

