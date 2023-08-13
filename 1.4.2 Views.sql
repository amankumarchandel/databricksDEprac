-- Databricks notebook source
show tables;

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

select * from global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

drop table smartphones;

drop view view_apple_phones;
drop view global_temp.global_temp_view_latest_phones;

-- COMMAND ----------


