# Databricks notebook source
# MAGIC %run ../common/Utils

# COMMAND ----------

schemaEcom = StructType([
    StructField("OrderID", StringType()),
    StructField("OrderItemsID", StringType()),
    StructField("CustomerID", StringType()),
    StructField("Status", StringType()),
    StructField("ProductID", StringType()),
    StructField("Category", StringType()),
    StructField("CategoryID", StringType()),
    StructField("Sex", StringType()),
    StructField("Age", IntegerType()),
    StructField("ProductName", StringType()),
    StructField("Brand", StringType()),
    StructField("Websites", StringType()),
    StructField("customerName", StringType()),
    StructField("Inventory", StringType()),
    StructField("Location", StringType()),
    StructField("City", StringType()),
    StructField("Price", FloatType()),
    StructField("TotalPrice", FloatType()),
    StructField("Quantity", IntegerType()),
    StructField("OrderDate", DateType()),
    StructField("ShippingAddress", StringType()),
    StructField("PaymentMethod", StringType())
])

# COMMAND ----------

## Read Ecoms event data from Bronze layer

Ecom_data = (spark.readStream.format("parquet")\
    .schema(schemaEcom)\
    .option("checkpointLocation", SilverCheckpointPath)\
    .load(BronzePath))

# COMMAND ----------

## Create Product dataframe from ecoms payload
Product = Ecom_data.select(col("ProductID").alias("product_id"), \
col("CategoryID").alias("categories_id"), col("ProductName").alias("product_name"), \
col("Brand").alias("product_brand"), col("Price").alias("product_price"), \
col("Websites").alias("product_site"), col("Inventory").alias("inventory"))

## Delta Table Names
for table_name in table_names:

## Create Product path
  Product_path = f"{SilverPath}/{Product}"
  Product_SilverCheckpointPath = f"{SilverCheckpointPath}/{Product}"
## Create Product delta table from Product data frame
def updateDeltaTableProduct(batch_df, batchID):
    Product_Table.alias('t')\
    .merge(batch_df.alias('s'),'t.product_id=s.product_id')\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

ProductQuery = Product.writeStream.format('delta') \ \
.foreachBatch(updateDeltaTableProduct) \
.option('checkpointLocation', Product_SilverCheckpointPath) \
.outputMode('append') \
.trigger(once=True) \
.start()

# COMMAND ----------

### Category code

## Create Category dataframe from ecoms payload
Category = Ecom_data.select(col("CategoryID").alias("categories_id"), \
col("Category").alias("categories"))


## Create Category path
Category_path = f"{SilverPath}/{Category}"
Category_SilverCheckpointPath = f"{SilverCheckpointPath}/{Category}"

## Create Category delta table from Category data frame
def updateDeltaTableCategory(batch_df, batchID):
    Category_Table.alias('t')\
    .merge(batch_df.alias('s'),'t.categories_id=s.categories_id')\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

CategoryQuery = Category.writeStream.format('delta') \ \
.foreachBatch(updateDeltaTableCategory) \
.option('checkpointLocation', Category_SilverCheckpointPath) \
.outputMode('append') \
.trigger(once=True) \
.start()

# COMMAND ----------

### Customer code
## Create Customer dataframe from ecoms payload
Customer = Ecom_data.select(col("CustomerID").alias("customer_id"), \
col("customerName").alias("customer_name"), col("Age").alias("customer_age"), \
col("Sex").alias("customer_sex"))


## Create Customer path
Customer_path = f"{SilverPath}/{Customer}"
Customer_SilverCheckpointPath = f"{SilverCheckpointPath}/{Customer}"

## Create Customer delta table from Customer data frame
def updateDeltaTableCustomer(batch_df, batchID):
    Category_Table.alias('t')\
    .merge(batch_df.alias('s'),'t.customer_id=s.customer_id')\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

CustomerQuery = Customer.writeStream.format('delta') \ \
.foreachBatch(updateDeltaTableCustomer) \
.option('checkpointLocation', Customer_SilverCheckpointPath) \
.outputMode('append') \
.trigger(once=True) \
.start()

# COMMAND ----------

### Address code

## Create Address dataframe from ecoms payload
Address = Ecom_data.select(col("CustomerID").alias("customer_id"), \
col("ShippingAddress").alias("address"), col("Location").alias("Location"), \
col("City").alias("customer_city"))


## Create Address path
Address_path = f"{SilverPath}/{Address}"
Address_SilverCheckpointPath = f"{SilverCheckpointPath}/{Address}"

## Create Address delta table from Address data frame
def updateDeltaTableAddress(batch_df, batchID):
    Address_Table.alias('t')\
    .merge(batch_df.alias('s'),'t.customer_id=s.customer_id')\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

AddressQuery = Address.writeStream.format('delta') \ \
.foreachBatch(updateDeltaTableAddress) \
.option('checkpointLocation', Address_SilverCheckpointPath) \
.outputMode('append') \
.trigger(once=True) \
.start()

# COMMAND ----------

### OrderItems code

## Create orderItems dataframe from ecoms payload
OrderItems = Ecom_data.select(col("OrderItemsID").alias("order_item_id"), \
col("OrderID").alias("order_item_order_id"), col("ProductID").alias("order_item_product_id"), \
col("Quantity").alias("order_item_quantity"), col("Price").alias("order_item_product_price"), \
col("TotalPrice").alias("order_item_totalproduct_price"))


## Create orderItems path
OrderItems_path = f"{SilverPath}/{OrderItems}"
OrderItems_SilverCheckpointPath = f"{SilverCheckpointPath}/{OrderItems}"

## Create OrderItems delta table from OrderItems data frame
def updateDeltaTableOrderItems(batch_df, batchID):
    Address_Table.alias('t')\
    .merge(batch_df.alias('s'),'t.order_item_id=s.order_item_id')\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

OrderItemsQuery = OrderItems.writeStream.format('delta') \ \
.foreachBatch(updateDeltaTableOrderItems) \
.option('checkpointLocation', OrderItems_SilverCheckpointPath) \
.outputMode('append') \
.trigger(once=True) \
.start()

# COMMAND ----------

### Orders code

## Create orders dataframe from ecoms payload
Orders = Ecom_data.select(col("OrderID").alias("order_id"), \
col("OrderDate").alias("order_date"), col("CustomerID").alias("order_customer_id"), \
col("Status").alias("order_status"), col("PaymentMethod").alias("Payment_method"))


## Create orders path
Orders_path = f"{SilverPath}/{Orders}"
Orders_SilverCheckpointPath = f"{SilverCheckpointPath}/{Orders}"

## Create orders delta table from orders data frame
def updateDeltaTableOrders(batch_df, batchID):
    Orders_Table.alias('t')\
    .merge(batch_df.alias('s'),'t.order_id=s.order_id')\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

OrdersQuery = Orders.writeStream.format('delta') \ \
.foreachBatch(updateDeltaTableOrders) \
.option('checkpointLocation', Orders_SilverCheckpointPath) \
.outputMode('append') \
.trigger(once=True) \
.start()
