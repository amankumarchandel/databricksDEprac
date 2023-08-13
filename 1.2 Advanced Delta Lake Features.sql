-- Databricks notebook source
select * from employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

select * 
from employees version as of 1


-- COMMAND ----------

select * from employees@v1

-- COMMAND ----------

delete from employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

restore table employees to version as of 2

-- COMMAND ----------

select * from employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

optimize employees 
zorder by id

--- no modification is done as we are using 1 cpu as cluster configurate for num of files are alredy 1

-- COMMAND ----------

describe history employees

-- we wont be able to see optimize transection as there is no change in our table

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

vacuum employees
--- by default the retention period is 7 days .. so no files will be deleted

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC To change retention data to desired number of days
-- MAGIC update your cluster configuration under advance section set properties as below
-- MAGIC `set spark.databricks.delta.retentionDurationCheck.enabled = false`
-- MAGIC `deltaTable.logRetentionDuration = "interval days"`
-- MAGIC `deltaTable.deletedFileRetentionDuration = "interval days"`

-- COMMAND ----------

--- just for demo purpose 
set spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum employees retain 0 hours

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

select * from employees@v1

-- COMMAND ----------

drop table employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------


