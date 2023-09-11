# Databricks notebook source
# MAGIC %run /Repos/aman.anku.kumar@gmail.com/databricksDEprac/Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

(spark.readStream
    .format('cloudFiles')
    .option('cloudFiles.format','parquet')
    .option('cloudFiles.schemaLocation','dbfs:/mnt/demo/checkpoints/orders_raw')
    .load(f"{dataset_bookstore}/orders-raw")
    .createOrReplaceTempView('orders_raw_temp')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view orders_tmp as(
# MAGIC   select *,current_timestamp() arrival_time,input_file_name() source_file
# MAGIC   from orders_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_tmp

# COMMAND ----------

(spark.table('orders_tmp')
    .writeStream
    .format('delta')
    .option('checkpointLocation','dbfs:/mnt/demo/checkpoint/orders_bronze')
    .outputMode('append')
    .table('orders_bronze')

)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_bronze

# COMMAND ----------

load_new_data()

# COMMAND ----------

(spark.read
    .format('json')
    .load(f"{dataset_bookstore}/customers-json")
    .createOrReplaceTempView('customers_lookup')    
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_lookup

# COMMAND ----------

(spark.readStream
    .table('orders_bronze')
    .createOrReplaceTempView('orders_bronze_tmp')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view orders_enriched_tmp as (
# MAGIC   select order_id,quantity,o.customer_id,c.profile:first_name as f_name,c.profile:last_name as l_name,
# MAGIC   cast(from_unixtime(order_timestamp,'yyyy-MM-dd HH:mm:ss') as timestamp) order_timestamp, books
# MAGIC   from orders_bronze_tmp o
# MAGIC   inner join customers_lookup c
# MAGIC   on o.customer_id = c.customer_id
# MAGIC   where quantity > 0 
# MAGIC )

# COMMAND ----------

(spark.table('orders_enriched_tmp')
    .writeStream
    .format('delta')
    .option('checkpointLocation','dbfs:/mnt/demo/checkpoints/orders_silver')
    .outputMode('append')
    .table('orders_silver')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_silver

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_silver 

# COMMAND ----------

(spark.readStream
 .table('orders_silver')
 .createOrReplaceTempView('orders_silver_tmp')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view daily_customer_books_temp as(
# MAGIC   select customer_id,f_name,l_name,date_trunc('DD',order_timestamp) order_date,sum(quantity) books_counts
# MAGIC   from orders_silver_tmp
# MAGIC   group by customer_id,f_name,l_name,date_trunc('DD',order_timestamp)
# MAGIC )

# COMMAND ----------

(spark.table('daily_customer_books_temp')
    .writeStream
    .format('delta')
    .outputMode('complete')
    .option('checkpointLocation','dbfs:/mnt/demo/checkpoints/daily_customer_books')
    .trigger(availableNow=True)
    .table('daily_customer_books')
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from daily_customer_books

# COMMAND ----------

load_new_data(all=True)

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------


