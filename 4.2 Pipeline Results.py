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

df = spark.read.format('delta').load('dbfs:/mnt/demo/dlt/demo_bookstore/system/events')

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView('event_data')

# COMMAND ----------

latest_update_id = spark.sql("""
    select origin.update_id
    from event_data
    where event_type = 'create_update'
    order by timestamp desc limit 1
""").first().update_id

print(latest_update_id)
spark.conf.set('latest_update_id',latest_update_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT row_expectations.dataset as dataset,
# MAGIC        row_expectations.name as expectation,
# MAGIC        SUM(row_expectations.passed_records) as passing_records,
# MAGIC        SUM(row_expectations.failed_records) as failing_records
# MAGIC FROM
# MAGIC   (SELECT explode(
# MAGIC             from_json(details :flow_progress :data_quality :expectations,
# MAGIC                       "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>")
# MAGIC           ) row_expectations
# MAGIC    FROM event_data
# MAGIC    WHERE event_type = 'flow_progress' AND 
# MAGIC          origin.update_id = '${latest_update.id}'
# MAGIC   )
# MAGIC GROUP BY row_expectations.dataset, row_expectations.name

# COMMAND ----------


