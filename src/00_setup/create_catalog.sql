-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1. Creating Widget

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT "fintech_dev";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Creating Catalog

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalogo);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Creating Schemas

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalogo);
DROP SCHEMA IF EXISTS default CASCADE;
CREATE SCHEMA IF NOT EXISTS landing;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Creating Volumes

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS IDENTIFIER(:catalogo).landing.raw_files;
CREATE VOLUME IF NOT EXISTS IDENTIFIER(:catalogo).ops.checkpoints;