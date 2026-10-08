# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
dbutils.widgets.text("catalogo", "fintech_dev")
catalogo = dbutils.widgets.get("catalogo")
print(catalogo)

# COMMAND ----------

origem = f'/Volumes/{catalogo}/landing/raw_files/kaggle/train_fraud_labels.json'

# COMMAND ----------

tabela_destino = f'{catalogo}.bronze.raw_fraud_labels'

# COMMAND ----------

print(dbutils.fs.head(origem, 500))

# COMMAND ----------

from pyspark.sql import functions as F

df_fraud_labels = (spark.read
    .option("multiLine", True)
    .schema("target MAP<STRING, STRING>")
    .json(origem)
    .select(
        F.explode("target").alias("transaction_id", "is_fraud"),
        F.col("_metadata.file_path").alias("_arquivo_origem"))
    .withColumn("_ingerido_em", F.current_timestamp()))

# COMMAND ----------

(df_fraud_labels.write
    .mode("overwrite")
    .saveAsTable(tabela_destino))

# COMMAND ----------

display(spark.sql("DESCRIBE DETAIL fintech_dev.bronze.raw_fraud_labels"))