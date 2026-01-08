# Databricks notebook source
## Databricks Certification ##

Delta Lake :-

It all begins with optimized storage using Delta lake , parquet or Iceberg 

✅ 1. Delta Lake Format
Use Delta Lake instead of raw Parquet/CSV.
Benefits:

-- ACID transactions for reliability.
-- Schema evolution and enforcement.
-- Time travel for historical queries.
-- Efficient updates and deletes

✅ 2. Data Partitioning
-- Partition large tables by frequently queried columns (e.g., date, region).
-- Reduces scan size and improves query performance.


✅ 3. Optimize Command
-- OPTIMIZE in Databricks compacts small files into larger ones.
-- Improves read performance by reducing file overhead.
-- OPTIMIZE my_table ZORDER BY (customer_id, date)


✅ 4. Auto Compaction & Vacuum
Auto Compaction: Automatically merges small files during writes.
VACUUM: Removes old snapshots and files to free storage.

-------------------------------------------------------------------------------------------------------------------------

Unity Catalog :- 

-- Build on top of this storage layer is unified governance with Unity catalog
-- Unity catalog is a centralized data catalog that provides access control, auditing, data lineage, quality monitoring
   and data discovery across Databricks workspaces 

------------------------------------------------------------------------------------------------------------------------

LakeFlow :-

Lakeflow provides a unified platform for data ingestion, transformation, and orchestration and include following component 

-LakeFlow Connect:- 
a set of efficient ingestion connectionrs that simplify data,ingestion from popular enterprises applications,
databases, cloud storage and local files 

-Lakeflow Declaratiove pipilines:-
a framework for building batch and streaming data pipilines using SQL and python , designed to accelerate ETL development. 

-Lakeflow Jobs:-
A workflow automation tool for databricks that orchestrates data processing workloads. it enables coordinations of multiple tasks
within complex workflow, allowing for the scheduling, optimization and management of repetable processes

-----------------------------------------------------------------------------------------------------------------------

Data Ingestion from cloud storage :-
Three primary methods for ingesting data from cloud storage to delta tables

-- create table as (CTAS)
-- Copy into 
-- Autoloader


Data ingestion from cloud storage :
1) Method 1 - Batch - create table as (ctas)
2) Method 2 - Incremental Batch - COPY INTO (legacy)
3) Methos 3 - Incremental Batch or Streaming - Auto Loader 



-----------------------------------------------------------------------------------------

Ingesting Enterprise data Overview

1) Lakeflow connect managed connectors :
-simplify the process of ingestion data from a wide variety of enterprise databases and applications.
-Provides a easy to use interface(UI) (or you can use the API)
-fully managed by databrekis , reducing the need for manual configuration or custome code 

The first method for ingesting enterprise data is by using lakeflow connect manage connectors 
- there connectors are built into databricks and are designed to simplify the process of ingesting data from a wide variety of enterprise databases and applocations


