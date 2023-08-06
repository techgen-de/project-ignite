# Databricks notebook source
# MAGIC %run ../../common/Utils

# COMMAND ----------

schemaEcom = StructType([
        StructField("OrderID", StringType()),
        StructField("OrderItemsID", StringType()),
        StructField("CustomerID", StringType()),
        StructField("Status", StringType()),
        StructField("ProductID", StringType()),
        StructField("Category", StringType()),
        StructField("CategoryID", StringType()),
        StructField("Cite", StringType()),
        StructField("Sex", StringType()),
        StructField("Age", IntegerType()),
        StructField("ProductName", StringType()),
        StructField("Brand", StringType()),
        StructField("Websites", StringType()),
        StructField("customerName", StringType()),
        StructField("Inventory", StringType()),
        StructField("Location", StringType()),
        StructField("Price", FloatType()),
        StructField("TotalPrice", FloatType()),
        StructField("Quantity", IntegerType()),
        StructField("OrderDate", DateType()),
        StructField("ShippingAddress", StringType()),
        StructField("PaymentMethod", StringType())
    ])


# COMMAND ----------

## Read raw data fron bronze

Ecom_data = (spark.readStream
            .format("parquet")
            .schema(schemaEcom)
            .option("checkpointLocation", SilverCheckpointPath)
            .load(BronzePath))

# COMMAND ----------

display(Ecom_data)

# COMMAND ----------

  
  
    # Product
    Product = Ecom_data.select(col("ProductID").alias("product_id"),
                               col("CategoryID").alias("categories_id"),
                               col("ProductName").alias("product_name"),
                               col("Brand").alias("product_brand"),
                               col("Price").alias("product_price"),
                               col("Websites").alias("product_site"),
                               col("Inventory").alias("inventory"))
    create_delta_table(Product, "Product", "product_id")

    # Category
    Category = Ecom_data.select(col("CategoryID").alias("categories_id"),
                                col("Category").alias("categories"))
    create_delta_table(Category, "Category", "categories_id")

    # Customer
    Customer = Ecom_data.select(col("CustomerID").alias("customer_id"),
                                col("customerName").alias("customer_name"),
                                col("Age").alias("customer_age"),
                                col("Sex").alias("customer_sex"))
    create_delta_table(Customer, "Customer", "customer_id")

    # Address
    Address = Ecom_data.select(col("CustomerID").alias("customer_id"),
                               col("ShippingAddress").alias("address"),
                               col("Location").alias("Location"),
                               col("Cite").alias("customer_city"))
    create_delta_table(Address, "Address", "customer_id")

    # OrderItems
    OrderItems = Ecom_data.select(col("OrderItemsID").alias("order_item_id"),
                                  col("OrderID").alias("order_item_order_id"),
                                  col("ProductID").alias("order_item_product_id"),
                                  col("Quantity").alias("order_item_quantity"),
                                  col("Price").alias("order_item_product_price"),
                                  col("TotalPrice").alias("order_item_totalproduct_price"))
    create_delta_table(OrderItems, "OrderItems", "order_item_id")

    # Orders
    Orders = Ecom_data.select(col("OrderID").alias("order_id"),
                              col("OrderDate").alias("order_date"),
                              col("CustomerID").alias("order_customer_id"),
                              col("Status").alias("order_status"),
                              col("PaymentMethod").alias("Payment_method"))
    create_delta_table(Orders, "Orders", "order_id")


def create_delta_table(streaming_df, table_name, primary_key=None):
    delta_query = streaming_df.writeStream \
        .format("delta") \
        .option("checkpointLocation", SilverCheckpointPath) \
        .outputMode("append")
    
    if primary_key:
        delta_query.option("mergeSchema", "true") \
            .trigger(once=True) \
            .foreachBatch(lambda df, batchId: merge_delta(df, table_name, primary_key)) \
            .start(f"{SilverPath}/{table_name}")
    else:
        delta_query.trigger(once=True) \
            .start(f"{SilverPath}/{table_name}")

def merge_delta(df, table_name, primary_key):
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, f"{SilverPath}/{table_name}")
    delta_table.alias("target") \
        .merge(df.alias("source"), f"target.{primary_key} = source.{primary_key}") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()


# COMMAND ----------

##Develop Ecoms data processing
def create_delta_table(streaming_df, table_name, primary_key=None):
    delta_query = streaming_df.writeStream \
        .format("delta") \
        .option("checkpointLocation", SilverCheckpointPath) \
        .outputMode("append")
    
    if primary_key:
        delta_query.option("mergeSchema", "true") \
                   .trigger(once=True) \
                   .foreachBatch(lambda df, batchId: merge_delta(df, table_name, primary_key)) \
                   .start(f"{SilverPath}/{table_name}")
    else:
        delta_query.trigger(once=True) \
                   .start(f"{SilverPath}/{table_name}")

def merge_delta(df, table_name, primary_key):
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, f"{SilverPath}/{table_name}")
    delta_table.alias("target") \
              .merge(df.alias("source"), f"target.{primary_key} = source.{primary_key}") \
              .whenMatchedUpdateAll() \
              .whenNotMatchedInsertAll() \
              .execute()


    # Product
    Product = Ecom_data.select(col("ProductID").alias("product_id"),
                               col("CategoryID").alias("categories_id"),
                               col("ProductName").alias("product_name"),
                               col("Brand").alias("product_brand"),
                               col("Price").alias("product_price"),
                               col("Websites").alias("product_site"),
                               col("Inventory").alias("inventory"))
    create_delta_table(Product, "Product", "product_id")

    # Category
    Category = Ecom_data.select(col("CategoryID").alias("categories_id"),
                                col("Category").alias("categories"))
    create_delta_table(Category, "Category", "categories_id")

    # Customer
    Customer = Ecom_data.select(col("CustomerID").alias("customer_id"),
                                col("customerName").alias("customer_name"),
                                col("Age").alias("customer_age"),
                                col("Sex").alias("customer_sex"))
    create_delta_table(Customer, "Customer", "customer_id")

    # Address
    Address = Ecom_data.select(col("CustomerID").alias("customer_id"),
                               col("ShippingAddress").alias("address"),
                               col("Location").alias("Location"),
                               col("Cite").alias("customer_city"))
    create_delta_table(Address, "Address", "customer_id")

    # OrderItems
    OrderItems = Ecom_data.select(col("OrderItemsID").alias("order_item_id"),
                                  col("OrderID").alias("order_item_order_id"),
                                  col("ProductID").alias("order_item_product_id"),
                                  col("Quantity").alias("order_item_quantity"),
                                  col("Price").alias("order_item_product_price"),
                                  col("TotalPrice").alias("order_item_totalproduct_price"))
    create_delta_table(OrderItems, "OrderItems", "order_item_id")

    # Orders
    Orders = Ecom_data.select(col("OrderID").alias("order_id"),
                              col("OrderDate").alias("order_date"),
                              col("CustomerID").alias("order_customer_id"),
                              col("Status").alias("order_status"),
                              col("PaymentMethod").alias("Payment_method"))
    create_delta_table(Orders, "Orders", "order_id")

##if __name__ == "__main__":
    

