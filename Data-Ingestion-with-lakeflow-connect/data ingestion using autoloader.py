# Databricks notebook source
# MAGIC %md
# MAGIC Data Ingestion with Auto Loader (CSV → Delta)

# COMMAND ----------


catalog = "hive_metastore_1"
schema  = "default"
volume  = "sample_data"

source_dir = f"/Volumes/{catalog}/{schema}/{volume}/"   # directory for Auto Loader
checkpoint_path = f"/Volumes/{catalog}/{schema}/{volume}/_chk_titanic_autoloader"
bronze_path     = f"/Volumes/{catalog}/{schema}/{volume}/_bronze_titanic_delta"

target_table = "titanic_autoloader_delta"



# COMMAND ----------



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


bronze_df = spark.read.format("delta").load(bronze_path)
display(bronze_df.filter(bronze_df["_rescued"].isNotNull()))


# COMMAND ----------


silver_path = f"/Volumes/{catalog}/{schema}/{volume}/_silver_titanic_delta"
silver_df = bronze_df.drop("_rescued")  # clean data

silver_df.write.mode("overwrite").format("delta").save(silver_path)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{target_table}_silver
USING DELTA
LOCATION '{silver_path}'
""")
