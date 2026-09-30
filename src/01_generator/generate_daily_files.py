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