# Databricks notebook source
# MAGIC %run /Repos/aman.anku.kumar@gmail.com/databricksDEprac/Includes/Copy-Datasets

# COMMAND ----------

(spark.readStream
    .table("books")
    .createOrReplaceTempView("books_streaming_tmp_vw")
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_streaming_tmp_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC select author, count(book_id) as total_books
# MAGIC from books_streaming_tmp_vw
# MAGIC group by author

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_streaming_tmp_vw
# MAGIC order by author

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view author_counts_tmp_vw as (
# MAGIC select author,count(book_id) as total_books
# MAGIC from books_streaming_tmp_vw
# MAGIC group by author
# MAGIC )

# COMMAND ----------

(spark.table('author_counts_tmp_vw')
    .writeStream
    .trigger(processingTime = '4 seconds')
    .outputMode('complete')
    .option('checkpointLocation','dbfs:/mnt/demo/author_counts_checkpoint')
    .table('author_count')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_count

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into books
# MAGIC values ('B19','Introduction to Modeling and Simulation','Mark W. Spong','Computer Science',25),
# MAGIC ('B20','Robot Modeling and Control','Mark W. Spong','Computer Science',30),
# MAGIC ('B21','Turing Vision: The Birth of Computer Science','Chris Bernhardt','Computer Science',35)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into books
# MAGIC values ('B16','Hands-On Deep Learning Algorithm with Python','Sudharsan Ravichandiran','Computer Science',25),
# MAGIC ('B17','Neural Network Methods in Natural Language Processing','Yoav Golderg','Computer Science',30),
# MAGIC ('B18','Understanding digital signal processing','Richard Lyons','Computer Science',35)

# COMMAND ----------

(spark.table('author_counts_tmp_vw')
    .writeStream
    .trigger(availableNow = True)
    .outputMode('complete')
    .option('checkpointLocation','dbfs:/mnt/demo/author_counts_checkpoint')
    .table('author_count')
    .awaitTermination()
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_count

# COMMAND ----------


