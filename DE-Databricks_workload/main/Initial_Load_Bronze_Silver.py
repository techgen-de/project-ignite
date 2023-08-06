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

display(Ecom_data)

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
ProductQuery = Product.writeStream.format('delta') \
.option('checkpointLocation', Product_SilverCheckpointPath) \
.outputMode('append') \
.trigger(once=True) \
.start(Product_path)

# COMMAND ----------

### Category code

## Create Category dataframe from ecoms payload
Category = Ecom_data.select(col("CategoryID").alias("categories_id"), \
col("Category").alias("categories"))


## Create Category path
Category_path = f"{SilverPath}/{Category}"
Category_SilverCheckpointPath = f"{SilverCheckpointPath}/{Category}"

## Create Category delta table from Category data frame
CategoryQuery = Category.writeStream.format('delta') \
.option('checkpointLocation', Category_SilverCheckpointPath) \
.outputMode('append') \
.trigger(once=True) \
.start(Category_path)

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
CustomerQuery = Customer.writeStream.format('delta') \
.option('checkpointLocation', Customer_SilverCheckpointPath) \
.outputMode('append') \
.trigger(once=True) \
.start(Customer_path)

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
AddressQuery = Address.writeStream.format('delta') \
.option('checkpointLocation', Address_SilverCheckpointPath) \
.outputMode('append') \
.trigger(once=True) \
.start(Address_path)

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
OrderItemsQuery = OrderItems.writeStream.format('delta') \
.option('checkpointLocation', OrderItems_SilverCheckpointPath) \
.outputMode('append') \
.trigger(once=True) \
.start(OrderItems_path)

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
OrdersQuery = Orders.writeStream.format('delta') \
.option('checkpointLocation', Orders_SilverCheckpointPath) \
.outputMode('append') \
.trigger(once=True) \
.start(Orders_path)

# COMMAND ----------

Product = spark.read.format("delta").load(Product_path)
display(Product)

# COMMAND ----------

Product.distinct().count()

# COMMAND ----------

Category = spark.read.format("delta").load(Category_path)
display(Category)

# COMMAND ----------

Category.count()

# COMMAND ----------

Category.distinct().count()

# COMMAND ----------

Customer = spark.read.format("delta").load(Customer_path)
display(Customer)

# COMMAND ----------

Customer.count()

# COMMAND ----------

Customer.distinct().count()

# COMMAND ----------

Address = spark.read.format("delta").load(Address_path)
display(Address)

# COMMAND ----------

Address.count()

# COMMAND ----------

Address.distinct().count()

# COMMAND ----------

OrderItems = spark.read.format("delta").load(OrderItems_path)
display(OrderItems)

# COMMAND ----------

OrderItems.count()

# COMMAND ----------

OrderItems.distinct().count()

# COMMAND ----------

Orders = spark.read.format("delta").load(Orders_path)
display(Orders)

# COMMAND ----------

Orders.count()

# COMMAND ----------

Orders.distinct().count()
