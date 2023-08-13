-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://dalhussein.blob.core.windows.net/course-resources/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run /Repos/aman.anku.kumar@gmail.com/databricksDEprac/Includes/Copy-Datasets

-- COMMAND ----------

select * from customers

-- COMMAND ----------

describe customers

-- COMMAND ----------

select customer_id, profile:first_name, profile:address:country
from customers

-- COMMAND ----------

select from_json(profile) as profile_struct
from customers;

-- COMMAND ----------

select profile
from customers limit 1

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_customers AS
  SELECT customer_id, from_json(profile,schema_of_json('{"first_name":"Dniren","last_name":"Abby","gender":"Female","address":{"street":"768 Mesta Terrace","city":"Annecy","country":"France"}}')) AS profile_struct
  FROM customers;

  SELECT * FROM parsed_customers

-- COMMAND ----------

describe parsed_customers

-- COMMAND ----------

select customer_id, profile_struct.first_name, profile_struct.address.country
from parsed_customers

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_final AS
  SELECT customer_id, profile_struct.*
  FROM parsed_customers;
SELECT * FROM customers_final  

-- COMMAND ----------

SELECT order_id, customer_id, books
 FROM orders

-- COMMAND ----------

select order_id, customer_id, explode(books) as book
from orders

-- COMMAND ----------

SELECT customer_id,
  collect_set(order_id) AS orders_set,
  collect_set(books.book_id) AS books_set
FROM orders
GROUP BY customer_id  

-- COMMAND ----------

SELECT customer_id,
  collect_set(books.book_id) AS before_flatten,
  array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
  FROM orders
  GROUP BY customer_id

-- COMMAND ----------

CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM (
    SELECT *, explode(books) AS book
    FROM orders
) o  
INNER JOIN books b  
ON o.book.book_id = b.book_id;

SELECT * FROM orders_enriched

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW orders_updates
AS SELECT * FROM PARQUET.`${dataset.bookstore}/orders-new`;

SELECT * FROM orders
UNION
SELECT * FROM orders_updates

-- COMMAND ----------

SELECT * FROM orders
INTERSECT
SELECT * FROM orders_updates

-- COMMAND ----------

SELECT * FROM orders
MINUS
SELECT * FROM orders_updates

-- COMMAND ----------

create or replace table transactions as

select * from (
  select
    customer_id,
    book.book_id as book_id,
    book.quantity as quantity
    from orders_enriched
) PIVOT (
  sum(quantity) for book_id in (
    'B01','B02','B03','B04','B05','B06',
    'B07','B08','B09','B10','B11','B12'
  )
);

select * from transactions

-- COMMAND ----------


