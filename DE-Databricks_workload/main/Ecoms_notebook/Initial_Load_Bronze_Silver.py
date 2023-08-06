# Databricks notebook source
# MAGIC %run ../../common/Utils

# COMMAND ----------

def read_ecom_data(spark, schemaEcom, bronze_path, silver_checkpoint_path):
    return (spark.readStream
            .format("parquet")
            .schema(schemaEcom)
            .option("checkpointLocation", silver_checkpoint_path)
            .load(bronze_path))

# COMMAND ----------



def create_product_df(ecom_data):
    return ecom_data.select(
        col("ProductID").alias("product_id"),
        col("CategoryID").alias("categories_id"),
        col("ProductName").alias("product_name"),
        col("Brand").alias("product_brand"),
        col("Price").alias("product_price"),
        col("Websites").alias("product_site"),
        col("Inventory").alias("inventory")
    )

def create_delta_table(streaming_df, table_path):
    return (streaming_df.writeStream
            .format('delta')
            .option('checkpointLocation', table_path)
            .outputMode('append')
            .trigger(once=True)
            .start(table_path))

def create_category_df(ecom_data):
    return ecom_data.select(
        col("CategoryID").alias("categories_id"),
        col("Category").alias("categories")
    )

def create_customer_df(ecom_data):
    return ecom_data.select(
        col("CustomerID").alias("customer_id"),
        col("customerName").alias("customer_name"),
        col("Age").alias("customer_age"),
        col("Sex").alias("customer_sex")
    )

def create_address_df(ecom_data):
    return ecom_data.select(
        col("CustomerID").alias("customer_id"),
        col("ShippingAddress").alias("address"),
        col("Location").alias("Location"),
        col("Cite").alias("customer_city")
    )

def create_order_items_df(ecom_data):
    return ecom_data.select(
        col("OrderItemsID").alias("order_item_id"),
        col("OrderID").alias("order_item_order_id"),
        col("ProductID").alias("order_item_product_id"),
        col("Quantity").alias("order_item_quantity"),
        col("Price").alias("order_item_product_price"),
        col("TotalPrice").alias("order_item_totalproduct_price")
    )

def create_orders_df(ecom_data):
    return ecom_data.select(
        col("OrderID").alias("order_id"),
        col("OrderDate").alias("order_date"),
        col("CustomerID").alias("order_customer_id"),
        col("Status").alias("order_status"),
        col("PaymentMethod").alias("Payment_method")
    )


# COMMAND ----------


