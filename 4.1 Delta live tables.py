# Databricks notebook source
# MAGIC %md ##Bronze Layer Tables

# COMMAND ----------

# MAGIC %md ####orders_raw

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
datasets_path = 'dbfs:/mnt/demo-datasets/bookstore'

# COMMAND ----------


# create or refresh streaming live table orders_raw
# comment "The raw books orders, ingested from orders-raw"
# as select * from cloud_files('${datasets_path}/orders-raw','parquet',map("cloudFiles.inferColumnTypes", "true"))

@dlt.table(
    comment='The raw books orders, ingested from orders-raw'
)
def orders_raw():
    return(
        spark.readStream.format('cloudFiles')
            .option('cloudFiles.format','parquet')
            .option('cloudFiles.inferColumnTypes','True')
            .load(f"{datasets_path}/orders-raw")

    ) 

# COMMAND ----------

# MAGIC %md ####customers

# COMMAND ----------

# create or refresh live table customers
# comment "The customers lookup table, ingested from customer-json"
# as select * from json.`${datasets_path}/customers-json`

@dlt.table(
    comment='The customers lookup table, ingested from customer-json'
)
def customers():
    return (
        spark.read.json(f'{datasets_path}/customers-json')
    )

# COMMAND ----------

# MAGIC %md ##Silver Layer Tables
# MAGIC
# MAGIC ####orders_cleaned

# COMMAND ----------


# create or refresh streaming live table orders_cleaned (
#   constraint valid_order_number expect(order_id is not null) on violation drop row
# )
# comment "The cleaned books orders with valid order_id"
# as
# select order_id,quantity,o.customer_id,c.profile:first_name as f_name,c.profile:last_name as l_name, cast(from_unixtime(order_timestamp,'yyyy-MM-dd HH:mm:ss') as timestamp) order_timestamp,o.books,c.profile:address:country as country
# from stream (live.orders_raw) o
# left join live.customers c
# on o.customer_id = c.customer_id

@dlt.table(
    comment='The cleaned books orders with valid order_id'
)
@dlt.expect_or_drop('valid_order_number','order_id is not null')
def orders_cleaned ():
    orders_raw = dlt.read_stream('orders_raw')
    customers = dlt.read('customers')
    return (
        orders_raw.join(customers,['customer_id'],'left')
        .selectExpr("order_id","quantity","customer_id","profile:first_name as f_name","profile:last_name as l_name","cast(from_unixtime(order_timestamp,'yyyy-MM-dd HH:mm:ss') as timestamp) order_timestamp" ,"books","profile:address:country as country")
    )


# COMMAND ----------

# MAGIC %md ##Gold Tables

# COMMAND ----------

# %sql
# CREATE OR REFRESH LIVE TABLE cn_daily_customer_books
# COMMENT "Daily number of books per customer in China"
# AS
#   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
#   FROM LIVE.orders_cleaned
#   WHERE country = "China"
#   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

@dlt.table(
    comment ='Daily number of books per customer in China'
)
def cn_daily_customer_books():
    return (
        dlt.read('orders_cleaned').filter("country = 'China'")
        .selectExpr('customer_id','f_name','l_name','date_trunc("DD", order_timestamp) order_date','quantity')
        .groupBy('customer_id','f_name','l_name','order_date').agg(sum('quantity').alias('books_count'))
        
    )

