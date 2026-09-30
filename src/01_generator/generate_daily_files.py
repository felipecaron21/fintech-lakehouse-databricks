# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %pip install faker

# COMMAND ----------

dbutils.widgets.text("catalogo", "fintech_dev")
catalogo = dbutils.widgets.get("catalogo")
print(catalogo)


dbutils.widgets.text("data_simulada", "2017-01-01")
data_simulada = dbutils.widgets.get("data_simulada")
print(data_simulada)

# COMMAND ----------

from datetime import datetime, date

data = datetime.strptime(data_simulada, "%Y-%m-%d").date()
data_min = datetime.strptime("2017-01-01", "%Y-%m-%d").date()
data_max = datetime.strptime("2019-10-31", "%Y-%m-%d").date()

if not (data_min <= data <= data_max):
    raise ValueError(f'A data {data} está fora do intervalo aceito: [{data_min} - {data_max}]')

# COMMAND ----------

schema_transactions = """
    id STRING,
    date STRING,
    client_id STRING,
    card_id STRING,
    amount STRING,
    use_chip STRING,
    merchant_id STRING,
    merchant_city STRING,
    merchant_state STRING,
    zip STRING,
    mcc STRING,
    errors STRING
"""

df_transactions_data = (spark.read
                        .schema(schema_transactions)
                        .option("header", True)
                        .csv(f'/Volumes/{catalogo}/landing/raw_files/kaggle/transactions_data.csv/')
)

display(df_transactions_data.limit(20))

# COMMAND ----------

from pyspark.sql import functions as F

df_transactions_data_file = (
    df_transactions_data
        .filter(F.substring("date", 1, 10) == data_simulada)
)

display(df_transactions_data_file.limit(20))
print(df_transactions_data_file.count())