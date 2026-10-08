# Databricks notebook source
dbutils.widgets.text("catalogo", "fintech_dev")
catalogo = dbutils.widgets.get("catalogo")
print(catalogo)

# COMMAND ----------

origem = f'/Volumes/{catalogo}/landing/raw_files/kaggle/mcc_codes.json'

# COMMAND ----------

tabela_destino = f'{catalogo}.bronze.raw_mcc_codes'

# COMMAND ----------

from pyspark.sql import functions as F

df_mcc_codes = (spark.read
    .text(origem, wholetext=True)
    .select(
        F.explode(F.from_json("value", "MAP<STRING, STRING>"))
        .alias("mcc", "descricao"),
        F.col("_metadata.file_path").alias("_arquivo_origem"))
    .withColumn("_ingerido_em", F.current_timestamp()))

# COMMAND ----------

(df_mcc_codes.write
    .mode("overwrite")
    .saveAsTable(tabela_destino))

# COMMAND ----------

display(spark.sql("DESCRIBE DETAIL fintech_dev.bronze.raw_mcc_codes"))

print(spark.table(f'{catalogo}.bronze.raw_mcc_codes').count())

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY fintech_dev.bronze.raw_mcc_codes"))