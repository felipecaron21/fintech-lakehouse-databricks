# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
dbutils.widgets.text("catalogo", "fintech_dev")
catalogo = dbutils.widgets.get("catalogo")
print(catalogo)

# COMMAND ----------

origem = f'/Volumes/{catalogo}/landing/raw_files/clientes/'

# COMMAND ----------

checkpoint = f'/Volumes/{catalogo}/ops/checkpoints/bronze_clientes/'

# COMMAND ----------

tabela_destino = f'{catalogo}.bronze.raw_clientes'

# COMMAND ----------

from pyspark.sql import functions as F

df_clientes_read = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", checkpoint)
    .option("cloudFiles.inferColumnTypes", "false")
    .option("cloudFiles.partitionColumns", "")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(origem))

df_clientes = (df_clientes_read
    .withColumn("_arquivo_origem", F.col("_metadata.file_path"))
    .withColumn("_ingerido_em", F.current_timestamp()))

# COMMAND ----------

(df_clientes.writeStream
    .option("checkpointLocation", checkpoint)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(tabela_destino)
    .awaitTermination())

# COMMAND ----------

from pyspark.sql import functions as F

df_validacao = spark.table(f'{catalogo}.bronze.raw_clientes')

display(df_validacao
    .groupBy("_arquivo_origem")
    .count()
    .orderBy(F.col("count").desc()))

print(df_validacao.count())

# COMMAND ----------

df_validacao.printSchema()