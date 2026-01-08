# Databricks notebook source
# MAGIC %md
# MAGIC working with schema mismatch in Spark, the standard and production-safe approach is to use a rescued data column (Spark / Databricks feature).

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.types import *
from pyspark.sql.functions import (current_timestamp,lit,input_file_name)


# COMMAND ----------

# DBTITLE 1,Pipeline Configuration
# Source
RAW_PATH = "/mnt/raw/orders"

# Target
BRONZE_DB = "bronze"
VALID_TABLE = "orders_valid"
INVALID_TABLE = "orders_invalid"

# Metadata
PIPELINE_NAME = "orders_bronze_ingestion"
SOURCE_SYSTEM = "ERP"


# COMMAND ----------

# DBTITLE 1,Expected Schema
expected_schema = StructType([
    StructField("orderid", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("orderdate", DateType(), True),
    StructField("order_amount", DoubleType(), True)
])


# COMMAND ----------

# DBTITLE 1,Read Data with Rescued Column
bronze_df = (
    spark.read
    .format("json")
    .schema(expected_schema)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_rescued_data")
    .load(RAW_PATH)
)


# COMMAND ----------

# DBTITLE 1,Add Metadata Columns
bronze_enriched_df = (
    bronze_df
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("pipeline_name", lit(PIPELINE_NAME))
    .withColumn("source_system", lit(SOURCE_SYSTEM))
    .withColumn("source_file", input_file_name())
)


# COMMAND ----------

# DBTITLE 1,Separate Valid & Invalid Records
valid_df = bronze_enriched_df.filter("_rescued_data IS NULL")
invalid_df = bronze_enriched_df.filter("_rescued_data IS NOT NULL")


# COMMAND ----------

# DBTITLE 1,Create Bronze Database (If Needed)
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bronze;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Write Valid Records
(
    valid_df
    .write
    .mode("append")
    .format("delta")
    .saveAsTable(f"{BRONZE_DB}.{VALID_TABLE}")
)


# COMMAND ----------

# DBTITLE 1,Write Invalid / Drifted Records
(
    invalid_df
    .write
    .mode("append")
    .format("delta")
    .saveAsTable(f"{BRONZE_DB}.{INVALID_TABLE}")
)


# COMMAND ----------

# DBTITLE 1,Validation Queries
# MAGIC %sql
# MAGIC SELECT count(*) AS valid_count FROM bronze.orders_valid;
# MAGIC
# MAGIC SELECT count(*) AS invalid_count FROM bronze.orders_invalid;
# MAGIC
# MAGIC SELECT _rescued_data FROM bronze.orders_invalid LIMIT 10;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Parse Rescued Data
from pyspark.sql.functions import from_json

invalid_df.select(
    "orderid",
    from_json("_rescued_data", MapType(StringType(), StringType())).alias("rescued_fields")
).show(truncate=False)


# COMMAND ----------

