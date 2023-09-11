-- Databricks notebook source
-- MAGIC %md ##Bronze Layer Tables

-- COMMAND ----------

create or refresh streaming live table books_bronze
comment "The raw books data, ingested from CDC feed"
as select * from cloud_files("${datasets_path}/books-cdc","json")

-- COMMAND ----------

-- MAGIC %md ##Silver Layer Tables

-- COMMAND ----------

create or refresh streaming live table books_silver;

apply changes into live.books_silver
from stream live.books_bronze
keys(book_id)
apply as delete when row_status = 'DELETE'
sequence by row_time
columns * except (row_status, row_time)

-- COMMAND ----------

-- MAGIC %md ##Gold Layer Tables

-- COMMAND ----------

create live table author_counts_state
comment "Number of books per author"
as select author,count(*) as books_count,current_timestamp() updated_time
from live.books_silver
group by author

-- COMMAND ----------

-- MAGIC %md ##DLT Views
-- MAGIC

-- COMMAND ----------

create live view books_sales
  as select b.title,o.quantity
  from (
    select *, explode(books) as book
    from live.orders_cleaned) o
  inner join live.books_silver b
  on o.book.book_id = b.book_id;

  
