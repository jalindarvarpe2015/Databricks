# Databricks notebook source

# --- Config ---
catalog = "hive_metastore_1"      # example catalog (adjust if needed)
schema  = "default"               # schema in the catalog
volume  = "sample_data"           # volume name
table_name = "titanic_csv_ingested"  # target Delta table

# Source: if single file
source_file = f"/Volumes/{catalog}/{schema}/{volume}/titanic.csv"

# Alternative: if many CSVs in the folder (recommended for Auto Loader)
source_dir = f"/Volumes/{catalog}/{schema}/{volume}/"   # directory

# Bronze checkpoint & output paths (under Volumes to keep it simple)
checkpoint_path = f"/Volumes/{catalog}/{schema}/{volume}/_chk_titanic_autoloader"
bronze_path     = f"/Volumes/{catalog}/{schema}/{volume}/_bronze_titanic_delta"


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

# MAGIC %sql
# MAGIC
# MAGIC # Use Auto Loader on the directory, filter the exact file if needed
# MAGIC df_bronze = (
# MAGIC     spark.readStream.format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "csv")
# MAGIC     .option("header", "true")
# MAGIC     .option("inferSchema", "false")  # using explicit schema above
# MAGIC     .schema(titanic_schema)
# MAGIC     # Rescued column captures unexpected fields / malformed records
# MAGIC     .option("cloudFiles.rescuedDataColumn", "_rescued")
# MAGIC     # If you only want the specific file:
# MAGIC     .option("cloudFiles.fileNamePattern", "titanic.csv")  # applies when source_dir has more files
# MAGIC     .load(source_dir)
# MAGIC )
# MAGIC
# MAGIC # Write to Bronze Delta (append mode for streaming)
# MAGIC (
# MAGIC     df_bronze.writeStream
# MAGIC     .format("delta")
# MAGIC     .option("checkpointLocation", checkpoint_path)
# MAGIC     .outputMode("append")
# MAGIC     .start(bronze_path)
# MAGIC )
# MAGIC

# COMMAND ----------


# Create managed Delta table in UC from the Bronze path
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name}
USING DELTA
LOCATION '{bronze_path}'
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC from pyspark.sql.functions import col, map_keys, map_values, to_json
# MAGIC
# MAGIC bronze_df = spark.read.format("delta").load(bronze_path)
# MAGIC
# MAGIC # Show rows where rescued data exists
# MAGIC rescued_df = bronze_df.filter(col("_rescued").isNotNull())
# MAGIC display(rescued_df.select("*", to_json(col("_rescued")).alias("rescued_json")))
# MAGIC
# MAGIC # Optional: list keys present in rescued data
# MAGIC display(
# MAGIC     rescued_df.select(map_keys(col("_rescued")).alias("rescued_keys")).limit(20)
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC silver_df = bronze_df
# MAGIC
# MAGIC # Example: if a column named "ExtraNote" drifted into _rescued
# MAGIC silver_df = silver_df.withColumn(
# MAGIC     "ExtraNote",
# MAGIC     col("_rescued").getItem("ExtraNote")  # extract a specific key from rescued map
# MAGIC )
# MAGIC
# MAGIC # Write Silver as a new Delta folder/table
# MAGIC silver_path = f"/Volumes/{catalog}/{schema}/{volume}/_silver_titanic_delta"
# MAGIC
# MAGIC silver_df.write.mode("overwrite").format("delta").save(silver_path)
# MAGIC
# MAGIC spark.sql(f"""
# MAGIC CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name}_silver
# MAGIC USING DELTA
# MAGIC LOCATION '{silver_path}'
# MAGIC """)
# MAGIC ``
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC bad_path = f"/Volumes/{catalog}/{schema}/{volume}/_bad_records_titanic"
# MAGIC
# MAGIC batch_df = (
# MAGIC     spark.read.format("csv")
# MAGIC     .option("header", "true")
# MAGIC     .option("mode", "PERMISSIVE")         # keep malformed rows, place bad fields in _corrupt_record
# MAGIC     .option("columnNameOfCorruptRecord", "_corrupt_record")
# MAGIC     .option("badRecordsPath", bad_path)   # store JSON descriptors of bad rows
# MAGIC     .schema(titanic_schema)
# MAGIC     .load(source_file)                    # single file
# MAGIC )
# MAGIC
# MAGIC batch_df.write.mode("overwrite").format("delta").save(bronze_path)
# MAGIC
# MAGIC spark.sql(f"""
# MAGIC CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name}
# MAGIC USING DELTA
# MAGIC LOCATION '{bronze_path}'
# MAGIC """)
# MAGIC
# MAGIC display(
# MAGIC     spark.read.format("delta").load(bronze_path).select("*", "_corrupt_record")
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- DLT SQL example (run in a DLT pipeline)
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE titanic_bronze
# MAGIC TBLPROPERTIES ("pipelines.autoOptimize.zOrderCols" = "Survived,Pclass")
# MAGIC AS SELECT * FROM STREAM read_files(
# MAGIC   format => "cloudFiles",
# MAGIC   path => "/Volumes/hive_metastore_1/default/sample_data/",
# MAGIC   options => map(
# MAGIC     "cloudFiles.format", "csv",
# MAGIC     "header", "true",
# MAGIC     "cloudFiles.rescuedDataColumn", "_rescued",
# MAGIC     "cloudFiles.fileNamePattern", "titanic.csv"
# MAGIC   )
# MAGIC );
# MAGIC
# MAGIC CREATE OR REFRESH LIVE TABLE titanic_silver
# MAGIC AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   -- Example: pull ExtraNote from rescued map
# MAGIC   _rescued["ExtraNote"] AS ExtraNote
# MAGIC FROM LIVE.titanic_bronze;
# MAGIC ``
# MAGIC