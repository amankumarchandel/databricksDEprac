-- Databricks notebook source
create table managed_default
(width int, length int, height int);

insert into managed_default
values(3,2,1)

-- COMMAND ----------

describe extended managed_default

-- COMMAND ----------

create table external_default
(width int, length int, height int)
location 'dbfs:/mnt/demo/external_default';

insert into external_default
values (3,2,1)

-- COMMAND ----------

describe extended external_default

-- COMMAND ----------

drop table managed_default

-- COMMAND ----------

drop table external_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

-- COMMAND ----------

create schema new_default

-- COMMAND ----------

describe database extended new_default

-- COMMAND ----------

use new_default;

create table managed_new_default
(width int, length int, height int);

insert into managed_new_default
values(3,2,1);


-- COMMAND ----------

create table external_new_default
(width int, length int, height int)
location 'dbfs:/mnt/demo/external_new_default';

insert into external_new_default
values(3,2,1);

-- COMMAND ----------

describe extended managed_new_default

-- COMMAND ----------

describe extended external_new_default

-- COMMAND ----------

drop table managed_new_default;
drop table external_new_default;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_new_default'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_new_default'

-- COMMAND ----------

create schema custom
location 'dbfs:/Shared/schemas/custom.db'

-- COMMAND ----------

describe database extended custom

-- COMMAND ----------

use custom;

create table managed_custom
(width int, length int, height int);

insert into managed_custom
values(3,2,1);


------------------------

create table external_custom
(width int, length int, height int)
location 'dbfs:/mnt/demo/external_custom';

insert into external_custom
values(3,2,1);


-- COMMAND ----------

describe extended managed_custom

-- COMMAND ----------

describe extended external_custom

-- COMMAND ----------

drop table managed_custom;
drop table external_custom;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/Shared/schemas/custom.db/managed_custom'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_custom'

-- COMMAND ----------


