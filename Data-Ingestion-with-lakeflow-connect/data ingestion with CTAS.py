# Databricks notebook source
# MAGIC %md
# MAGIC Notebook 1 — CTAS via PySpark ()

# COMMAND ----------


catalog = "hive_metastore_1"     # Unity Catalog catalog
schema  = "default"              # schema
volume  = "sample_data"          # volume name
source_file = f"/Volumes/{catalog}/{schema}/{volume}/titanic.csv"

target_table = "titanic_ctas_delta"  # final Delta table name


# COMMAND ----------


from pyspark.sql.types import *

titanic_schema = StructType([
    StructField("PassengerId", IntegerType(), True),
    StructField("Survived", IntegerType(), True),
    StructField("Pclass", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Sex", StringType(), True),
    StructField("Age", DoubleType(), True),
    StructField("SibSp", IntegerType(), True),
    StructField("Parch", IntegerType(), True),
    StructField("Ticket", StringType(), True),
    StructField("Fare", DoubleType(), True),
    StructField("Cabin", StringType(), True),
    StructField("Embarked", StringType(), True),
])


# COMMAND ----------


df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("mode", "PERMISSIVE")                 # keep malformed rows
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(titanic_schema)                       # optional but recommended
    .load(source_file)
)

df.createOrReplaceTempView("titanic_src_vw")


# COMMAND ----------


spark.sql(f"""
CREATE TABLE {catalog}.{schema}.{target_table}
AS
SELECT
  PassengerId, Survived, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked,
  _corrupt_record
FROM titanic_src_vw
""")
``


# COMMAND ----------

# MAGIC %md
# MAGIC Notebook 2 — Pure SQL CTAS (Databricks SQL)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Adjust names as needed
# MAGIC USE CATALOG hive_metastore_1;
# MAGIC USE SCHEMA default;
# MAGIC
# MAGIC -- Create a staging table over the CSV file
# MAGIC CREATE OR REPLACE TABLE titanic_csv_stg
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   'header' = 'true',
# MAGIC   'mode' = 'PERMISSIVE',
# MAGIC   'columnNameOfCorruptRecord' = '_corrupt_record'
# MAGIC )
# MAGIC LOCATION '/Volumes/hive_metastore_1/default/sample_data/titanic.csv';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE titanic_ctas_delta
# MAGIC AS
# MAGIC SELECT
# MAGIC   PassengerId, Survived, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked,
# MAGIC   _corrupt_record
# MAGIC FROM titanic_csv_stg;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS rows FROM titanic_ctas_delta;
# MAGIC SELECT * FROM titanic_ctas_delta LIMIT 10;
# MAGIC