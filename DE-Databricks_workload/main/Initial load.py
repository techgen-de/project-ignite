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

display(df)

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

display(orders)

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

display(products)
