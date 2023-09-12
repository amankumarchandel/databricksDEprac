# Databricks notebook source
# MAGIC %md ##Bronze Layer Tables

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
datasets_path = spark.conf.get('datasets_path')


# COMMAND ----------

# %sql
# create or refresh streaming live table books_bronze
# comment "The raw books data, ingested from CDC feed"
# as select * from cloud_files("${datasets_path}/books-cdc","json")

@dlt.table(
    comment='The raw books data, ingested from CDC feed'
)
def books_bronze():
    return(
        spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format','json')
        .option('cloudFiles.inferColumnTypes','True')
        .load(f"{datasets_path}/books-cdc")
    )

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ##Silver Layer Tables

# COMMAND ----------

# %sql
# create or refresh streaming live table books_silver;

# apply changes into live.books_silver
# from stream live.books_bronze
# keys(book_id)
# apply as delete when row_status = 'DELETE'
# sequence by row_time
# columns * except (row_status, row_time)

dlt.create_streaming_table('books_silver')

dlt.apply_changes(
    target = 'books_silver',
    source = 'books_bronze',
    keys =['book_id'],
    sequence_by = col('row_time'),
    apply_as_deletes = expr("row_status = 'DELETE'"),
    except_column_list=['row_status','row_time'] 
)

# COMMAND ----------

# MAGIC %md ##Gold Layer Tables

# COMMAND ----------

# %sql
# create live table author_counts_state
# comment "Number of books per author"
# as select author,count(*) as books_count,current_timestamp() updated_time
# from live.books_silver
# group by author

@dlt.table(
    comment = 'Number of books per author'
)
def author_counts_state():
    return(
        dlt.read('books_silver')
        .selectExpr('author','current_timestamp() updated_time')
        .groupBy('author','updated_time').agg(count('author').alias('books_count'))
    )

# COMMAND ----------

# MAGIC %md ##DLT Views
# MAGIC

# COMMAND ----------

# %sql
# create live view books_sales
#   as select b.title,o.quantity
#   from (
#     select *, explode(books) as book
#     from live.orders_cleaned) o
#   inner join live.books_silver b
#   on o.book.book_id = b.book_id;
@dlt.view()
def books_sales():
    orders_cleaned = dlt.read('orders_cleaned')
    orders_cleaned = orders_cleaned.withColumn('book',explode(col('books')))
    orders_cleaned = orders_cleaned.withColumn('book_id',expr("book.book_id"))
    books_silver = dlt.read('books_silver')
    return (
                orders_cleaned.join(books_silver,['book_id'])
    )
  
