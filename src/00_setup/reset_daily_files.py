# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
dbutils.widgets.text("catalogo", "fintech_dev")
catalogo = dbutils.widgets.get("catalogo")
print(catalogo)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Excluindo arquivos daily

# COMMAND ----------

display(dbutils.fs.rm(f'/Volumes/{catalogo}/landing/raw_files/transacoes/daily/', recurse=True))

# COMMAND ----------

display(dbutils.fs.rm(f'/Volumes/{catalogo}/landing/raw_files/clientes/daily/', recurse=True))

# COMMAND ----------

display(dbutils.fs.rm(f'/Volumes/{catalogo}/landing/raw_files/cartoes/daily/', recurse=True))

# COMMAND ----------

display(dbutils.fs.rm(f'/Volumes/{catalogo}/landing/raw_files/kaggle/prepared/gabarito_defeitos/', recurse=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Excluindo checkpoint + tabela bronze (transacoes)

# COMMAND ----------

display(dbutils.fs.rm(f'/Volumes/{catalogo}/ops/checkpoints/bronze_transacoes/', recurse=True))

# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS {catalogo}.bronze.raw_transacoes')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Excluindo checkpoint + tabela bronze (clientes)

# COMMAND ----------

display(dbutils.fs.rm(f'/Volumes/{catalogo}/ops/checkpoints/bronze_clientes/', recurse=True))

# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS {catalogo}.bronze.raw_clientes')