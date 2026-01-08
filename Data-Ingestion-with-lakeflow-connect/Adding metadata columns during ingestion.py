# Databricks notebook source
# DBTITLE 1,Imports & Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.functions import (current_timestamp,lit,col,input_file_name)


# COMMAND ----------

# DBTITLE 1,Define Common Parameters
# Source details
SOURCE_CATALOG = "hive_metastore"
SOURCE_DB = "raw_db"
SOURCE_TABLE = "orders"

# Target details
TARGET_DB = "silver_db"
TARGET_TABLE = "orders_enriched"

# Metadata
PIPELINE_NAME = "orders_ingestion_pipeline"
SOURCE_SYSTEM = "ERP"


# COMMAND ----------

# DBTITLE 1,Read Data from Catalog (Hive / Glue)
source_df = spark.read.table(f"{SOURCE_CATALOG}.{SOURCE_DB}.{SOURCE_TABLE}")

source_df.printSchema()
source_df.show(5)


# COMMAND ----------

# DBTITLE 1,Redefine / Transform Data
transformed_df = (
    source_df
    .withColumnRenamed("orderid", "order_id")
    .withColumnRenamed("orderdate", "order_date")
    .withColumn("order_amount", col("order_amount").cast("double"))
    .filter(col("order_id").isNotNull())
)


# COMMAND ----------

# DBTITLE 1,Add Metadata Columns
final_df = (
    transformed_df
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("pipeline_name", lit(PIPELINE_NAME))
    .withColumn("source_system", lit(SOURCE_SYSTEM))
    .withColumn("load_date", current_timestamp().cast("date"))
)


# COMMAND ----------

# DBTITLE 1,Data Quality Check
record_count = final_df.count()

if record_count == 0:
    raise Exception("No records to ingest!")


# COMMAND ----------

# DBTITLE 1,Write Data to Target Catalog Table
spark.read.table(f"{TARGET_DB}.{TARGET_TABLE}") \
     .orderBy(col("ingestion_ts").desc()) \
     .show(10)
