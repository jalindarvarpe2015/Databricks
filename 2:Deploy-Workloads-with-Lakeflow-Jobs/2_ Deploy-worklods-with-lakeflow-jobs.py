# Databricks notebook source
# MAGIC %md
# MAGIC **Job schedule ans triggers **
# MAGIC
# MAGIC A trigger a rule that automatically starts a job run based on a specific condition or schedule 
# MAGIC
# MAGIC **Common** trigger types include :-
# MAGIC
# MAGIC 1) Time -based schedules 
# MAGIC 2) Contineius (always -on) execution
# MAGIC 3) file arrival triiger 
# MAGIC 4) Manual Trigger 
# MAGIC 5) Table Update 
# MAGIC
# MAGIC Trigger **enable automation ** so jobs can run without manual intervention 

# COMMAND ----------

# MAGIC %md
# MAGIC **Types of Triggers **
# MAGIC
# MAGIC **1) Scheduled Trigger**
# MAGIC - Automatically rubs job at set times or intervals, such as hourly, daily using the UI 
# MAGIC - you can schedule jobs using cron expression for automated execution 
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC **File arrival Trigger** 
# MAGIC
# MAGIC - Automatically trigger jobs when new files are detected in a specified storage location with support for 
# MAGIC - aws s3, azure storage, GCP GS and databricks volume
# MAGIC
# MAGIC - Enabled event-deiven processing to start data worjflows upon file arrival
# MAGIC
# MAGIC - ideal for automating jobs with unpredictable or irregular data ingestion patterns 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Continuous Trigger**
# MAGIC
# MAGIC - Continuously runs job by starting a new run as soon as the previous one finishes or fails 
# MAGIC
# MAGIC - built-in retry logic is automatically managed by databricks 
# MAGIC
# MAGIC - ideal for streaming worklods

# COMMAND ----------

# MAGIC %md
# MAGIC **Manual Triggers**
# MAGIC
# MAGIC - Manual (None) trigger lets job run on demand without any schedule or event 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Table Upate Trigger **
# MAGIC
# MAGIC - Table update trigger automatically starts a job when specified tables are uploaded 