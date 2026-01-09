# Databricks notebook source
# MAGIC %md
# MAGIC Notebook — Data Ingestion with COPY INTO (CSV → Delta)

# COMMAND ----------


catalog = "hive_metastore_1"
schema  = "default"
volume  = "sample_data"

# Source paths
source_file = f"/Volumes/{catalog}/{schema}/{volume}/titanic.csv"
source_dir  = f"/Volumes/{catalog}/{schema}/{volume}/"   # directory if you have more files

# Target table
target_table = "titanic_copyinto_delta"


# COMMAND ----------

# DBTITLE 1,Create Target Delta Table (empty)
# MAGIC %sql
# MAGIC
# MAGIC -- Use UC context
# MAGIC USE CATALOG hive_metastore_1;
# MAGIC USE SCHEMA default;
# MAGIC
# MAGIC -- Create an empty Delta table with the desired schema
# MAGIC CREATE OR REPLACE TABLE titanic_copyinto_delta (
# MAGIC   PassengerId INT,
# MAGIC   Survived    INT,
# MAGIC   Pclass      INT,
# MAGIC   Name        STRING,
# MAGIC   Sex         STRING,
# MAGIC   Age         DOUBLE,
# MAGIC   SibSp       INT,
# MAGIC   Parch       INT,
# MAGIC   Ticket      STRING,
# MAGIC   Fare        DOUBLE,
# MAGIC   Cabin       STRING,
# MAGIC   Embarked    STRING,
# MAGIC   _corrupt_record STRING  -- optional column for corrupt rows when using PERMISSIVE mode
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,Preview with VALIDATE (Dry Run)
# MAGIC %sql
# MAGIC
# MAGIC -- Validate which files would be loaded (dry run)
# MAGIC COPY INTO titanic_copyinto_delta
# MAGIC FROM '/Volumes/hive_metastore_1/default/sample_data/'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS (
# MAGIC   'header' = 'true',
# MAGIC   'mode'   = 'PERMISSIVE',                  -- keep malformed rows in _corrupt_record
# MAGIC   'columnNameOfCorruptRecord' = '_corrupt_record'
# MAGIC )
# MAGIC COPY_OPTIONS (
# MAGIC   'mergeSchema' = 'false',                  -- set true only if adding columns is desired
# MAGIC   'force'       = 'false'                   -- set true to re-load files even if already loaded
# MAGIC )
# MAGIC PATTERN '.*titanic\.csv'                    -- only titanic.csv from the folder
# MAGIC VALIDATE;
# MAGIC ``
# MAGIC

# COMMAND ----------

# DBTITLE 1,Ingest with COPY INTO
# MAGIC %sql
# MAGIC
# MAGIC -- Perform the actual ingestion
# MAGIC COPY INTO titanic_copyinto_delta
# MAGIC FROM '/Volumes/hive_metastore_1/default/sample_data/'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS (
# MAGIC   'header' = 'true',
# MAGIC   'mode'   = 'PERMISSIVE',
# MAGIC   'columnNameOfCorruptRecord' = '_corrupt_record',
# MAGIC   'recursiveFileLookup' = 'false',          -- set true to scan subdirs
# MAGIC   'timestampFormat' = 'yyyy-MM-dd HH:mm:ss',-- set if you have timestamp columns
# MAGIC   'dateFormat'      = 'yyyy-MM-dd'          -- set if you have date columns
# MAGIC )
# MAGIC COPY_OPTIONS (
# MAGIC   'mergeSchema' = 'false',
# MAGIC   'force'       = 'false'                   -- set true to reload all files (idempotency override)
# MAGIC )
# MAGIC PATTERN '.*titanic\.csv';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS rows FROM titanic_copyinto_delta;
# MAGIC SELECT * FROM titanic_copyinto_delta LIMIT 10;
# MAGIC