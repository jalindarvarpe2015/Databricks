# Databricks notebook source
# MAGIC %sql
# MAGIC -- Merge (Upsert) using Pyspark and spark sql
# MAGIC -- upsert : when we have to update target tabke based on latest incoming source data and compare 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *  

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("contact_no", IntegerType(), True)
])


# COMMAND ----------

data = [(1000, "Michael", "Columbus", "USA",657854324)]
df = spark.createDataFrame(data=data , schema= schema)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_employee(
# MAGIC   emp_id INT,
# MAGIC   name STRING,
# MAGIC   city STRING,
# MAGIC   country STRING,
# MAGIC   contact_no int
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION "/FileStore/tables/delta_merge "

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee

# COMMAND ----------

# MAGIC %md
# MAGIC # Method 1 - Spark SQL

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dim_employee as target 
# MAGIC USING source_view as source 
# MAGIC  ON target.emp_id = source.emp_id
# MAGIC  WHEN MATCHED 
# MAGIC THEN UPDATE SET 
# MAGIC   target.name = source.name,
# MAGIC   target.city = source.city,
# MAGIC   target.country = source.country,
# MAGIC   target.contact_no = source.contact_no
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT(emp_id, name, city,country,contact_no) VALUES (emp_id, name, city,country,contact_no)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee

# COMMAND ----------

data = [(1000, "Michael", "Chicago", "USA",657854324), (2000,"Nancy", "New York", "USA", 876534567)]
df = spark.createDataFrame(data=data , schema= schema)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("source_view")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dim_employee as target 
# MAGIC USING source_view as source 
# MAGIC  ON target.emp_id = source.emp_id
# MAGIC  WHEN MATCHED 
# MAGIC THEN UPDATE SET 
# MAGIC   target.name = source.name,
# MAGIC   target.city = source.city,
# MAGIC   target.country = source.country,
# MAGIC   target.contact_no = source.contact_no
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT(emp_id, name, city,country,contact_no) VALUES (emp_id, name, city,country,contact_no)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee