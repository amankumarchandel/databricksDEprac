-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://dalhussein.blob.core.windows.net/course-resources/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run /Repos/aman.anku.kumar@gmail.com/databricksDEprac/Includes/Copy-Datasets

-- COMMAND ----------

select * from orders

-- COMMAND ----------

select 
  order_id,
  books,
  filter (books,i->i.quantity >=2) as multiple_copies
from orders  

-- COMMAND ----------

select order_id,multiple_copies
from(
  select
    order_id,
    filter(books, i ->i.quantity >=2) as multiple_copies
  from orders
)
where size(multiple_copies)>0;

-- COMMAND ----------

select 
order_id,
books,
transform(books,
  b -> cast(b.subtotal * 0.8 as int)
 ) as subtotal_after_discount
 from orders;

-- COMMAND ----------

create or replace function get_url(email string) returns string
return concat("https://www.",split(email,"@")[1])

-- COMMAND ----------

select email,get_url(email) as domain
from customers

-- COMMAND ----------

describe function get_url

-- COMMAND ----------

describe function extended get_url

-- COMMAND ----------

create function site_type(email STRING) RETURNS STRING
RETURN case
          when email like "%.com" then "Commercial business"
          when email like "%.org" then "Non-profits organization"
          when email like "%.edu" then "Educational institution"
          else concat("Unknown extension for domian: ",split(email,"@")[1])
       end;   

-- COMMAND ----------

select email,site_type(email) as domain_category
from customers

-- COMMAND ----------

drop function get_url;
drop function site_type;

-- COMMAND ----------


