# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
dbutils.widgets.text("catalogo", "fintech_dev")
catalogo = dbutils.widgets.get("catalogo")
print(catalogo)

# COMMAND ----------

display(dbutils.fs.rm(f'/Volumes/{catalogo}/landing/raw_files/transacoes/daily/', recurse=True))

# COMMAND ----------

display(dbutils.fs.rm(f'/Volumes/{catalogo}/landing/raw_files/clientes/daily/', recurse=True))

# COMMAND ----------

display(dbutils.fs.rm(f'/Volumes/{catalogo}/landing/raw_files/cartoes/daily/', recurse=True))

# COMMAND ----------

display(dbutils.fs.rm(f'/Volumes/{catalogo}/landing/raw_files/kaggle/prepared/gabarito_defeitos/', recurse=True))

# COMMAND ----------

display(dbutils.fs.rm(f'/Volumes/{catalogo}/ops/checkpoints/bronze_transacoes/', recurse=True))

# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS {catalogo}.bronze.raw_transacoes')