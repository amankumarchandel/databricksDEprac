-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://dalhussein.blob.core.windows.net/course-resources/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run /Repos/aman.anku.kumar@gmail.com/databricksDEprac/Includes/Copy-Datasets

-- COMMAND ----------

create table orders as
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

select * from orders

-- COMMAND ----------

create or replace table orders as
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

describe history orders

-- COMMAND ----------

insert overwrite orders
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

describe history orders

-- COMMAND ----------

insert overwrite orders
select *,current_timestamp() from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

insert into orders
select * from parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

select count(*) from orders

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS 
SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
  UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates
  (book_id STRING, title STRING,author STRING,category STRING,price DOUBLE)
  USING CSV
  OPTIONS(
    path = "${dataset.bookstore}/books-csv-new",
    header = "true",
    delimiter = ";"
);

SELECT * FROM books_updates

-- COMMAND ----------

MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN
INSERT *

-- COMMAND ----------

MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN
INSERT *

-- COMMAND ----------


