-- Databricks notebook source
CREATE TABLE IF NOT EXISTS smartphones
(id INT, name STRING, brand STRING, year INT);

INSERT INTO smartphones
VALUES (1, 'iPhone 14', 'Apple', 2022),
      (2, 'iPhone 13', 'Apple', 2021),
      (3, 'iPhone 6', 'Apple', 2014),
      (4, 'iPad Air', 'Apple', 2013),
      (5, 'Galaxy S22', 'Samsung', 2022),
      (6, 'Galaxy Z Fold', 'Samsung', 2022),
      (7, 'Galaxy S9', 'Samsung', 2016),
      (8, '12 Pro', 'Xiaomi', 2022),
      (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
      (10, 'Redmi Note 11', 'Xiaomi', 2021)

-- COMMAND ----------

show tables

-- COMMAND ----------

create view view_apple_phones
as select *
  from smartphones
  where brand = 'Apple';

-- COMMAND ----------

select * from view_apple_phones;

-- COMMAND ----------

show tables;

-- COMMAND ----------

create temporary view temp_view_phones_brands
  as select distinct brand
      from smartphones;

-- COMMAND ----------

select * from temp_view_phones_brands;


-- COMMAND ----------

show tables;

-- COMMAND ----------

create global temporary view global_temp_view_latest_phones
as select * from smartphones
    where year > 2020
    order by year desc;

-- COMMAND ----------

select * from global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

show tables;

-- COMMAND ----------


