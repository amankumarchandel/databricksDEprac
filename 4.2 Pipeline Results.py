# Databricks notebook source

files = dbutils.fs.ls('dbfs:/mnt/demo/dlt/demo_bookstore')
display(files)

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/demo/dlt/demo_bookstore/system/events")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/mnt/demo/dlt/demo_bookstore/system/events`

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/demo/dlt/demo_bookstore/tables")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_bookstore_dlt_db.cn_daily_customer_books

# COMMAND ----------


