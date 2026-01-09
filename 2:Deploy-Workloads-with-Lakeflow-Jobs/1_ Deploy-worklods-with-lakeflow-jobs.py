# Databricks notebook source
# MAGIC %md
# MAGIC -  Lakeflow coer job concept
# MAGIC
# MAGIC - creating and scheduling jobs
# MAGIC
# MAGIC - advanced lakeflow job features

# COMMAND ----------

# MAGIC %md
# MAGIC Building blocks for lakeflow jobs
# MAGIC
# MAGIC A **job** is primary resource of 
# MAGIC - scheduling 
# MAGIC - coordinating
# MAGIC -running operations such as data processing etl analytics ,machine learning worklods within a databricks environment
# MAGIC
# MAGIC
# MAGIC A **task** is a single unit of work within a job that execute a specific workload such as notebook , script query and more
# MAGIC
# MAGIC each job consict of one or more task, which are individual units of work that make up the job 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Job can be executed on different compute 
# MAGIC
# MAGIC 1) **Interactive clusters** : 
# MAGIC - development , ad-hoc analysis , and exploration
# MAGIC - interactive or all purpose cluster can be shared by multiple users
# MAGIC - it is best for performing ad-hoc analysis , data exploration and development
# MAGIC - interactive should not be used in productiona as they are not cost efficient 
# MAGIC
# MAGIC
# MAGIC 2) **Job Cluster** :-
# MAGIC - job clusters are approximately 50% cheaper as they terminate when job ends, reducing teh resources usage and costs
# MAGIC
# MAGIC
# MAGIC 3) **serverlesss**:-
# MAGIC - serverless workflows are fully managed service that are operationally simpler and more reliable 
# MAGIC - they provide you with faster clusters ans autoscaling capabilities providing you witha better user experience for a lower cost
# MAGIC
# MAGIC 4) **SQLWarehouse**:-
# MAGIC - purpose -built for a sql queries, dashboards and bi , can be attached to notebook as well
# MAGIC
# MAGIC - high concurrency + autoacaling via intelligent workloads management for consistient low latency
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC TASK ORCHESTRATION
# MAGIC
# MAGIC A dag is a conceptual representation of series of activities, including data processing flows
# MAGIC
# MAGIC **D**irected - unambiguous direction of each edge
# MAGIC
# MAGIC **A**cyclic - contains no cycle
# MAGIC
# MAGIC **G**raph - collection of vertices conected by edge
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **_common worklodds pattern_**
# MAGIC
# MAGIC 1) **sequence** 
# MAGIC - data transformation/processsig/cleaning
# MAGIC - bronze/silver/gold
# MAGIC
# MAGIC 2)**Funnel** :-
# MAGIC - multiple data sources
# MAGIC - data collection
# MAGIC
# MAGIC
# MAGIC 3) **fan-out, star pattern** :-
# MAGIC
# MAGIC - single data source
# MAGIC - data ingestion and distribution
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Common task configuration options
# MAGIC
# MAGIC - Parameters and dynamic value reference 
# MAGIC - Notification alerts 
# MAGIC - Retries
# MAGIC
# MAGIC All can be set at the job level or at the individual Task level

# COMMAND ----------

# MAGIC %md
# MAGIC Parameter overview
# MAGIC
# MAGIC **Task Parameter :-**
# MAGIC - A key value pair defined at the task level
# MAGIC
# MAGIC **Job parameters :**
# MAGIC - A key value pair defined at the job level and pushed down all tasks
# MAGIC
# MAGIC
# MAGIC Job parameter override task parameter when same key exists 

# COMMAND ----------

# MAGIC %md
# MAGIC **Accessing parameter values in Task**
# MAGIC
# MAGIC catalog_name = "mar_uc_cat_dev"
# MAGIC schema = "defualt"
# MAGIC
# MAGIC - catalog = dbutils.widgets.get("catalog_name")
# MAGIC
# MAGIC - schema = dbutils.widgets.get("schema_name ")

# COMMAND ----------

# MAGIC %md
# MAGIC **Dynamically set Task values with code** 
# MAGIC
# MAGIC dbutils.jobs.taskValues.set(
# MAGIC    key = "catalog_name",
# MAGIC    value = "dbacademy"
# MAGIC  )
# MAGIC
# MAGIC - Task values are dynamically key value pair that task create and share during workflow executing
# MAGIC
# MAGIC
# MAGIC **Accessing a parameter from another task**
# MAGIC
# MAGIC dbutils.jobs.taskValues.get(
# MAGIC    taskKey = "task-name",
# MAGIC    key = "catalog_name"
# MAGIC  )
# MAGIC
# MAGIC - Your reference specific upstream task parameter value into downstream tasks
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Notification Configuration**
# MAGIC
# MAGIC - A notificationalert is sent after successfully completion of the job
# MAGIC
# MAGIC - This setting can be adjusted in the job details section (right side pane) 
# MAGIC
# MAGIC **Task level notification : -**
# MAGIC
# MAGIC - A notification alert will be sent when the task is successfully completed 
# MAGIC
# MAGIC - This notification setting can be customized at the task level
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Retry Policy**
# MAGIC
# MAGIC Apolicy that determine when and how many times failed runs are **retried**