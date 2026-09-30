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
# MAGIC ###1. Definindo schema e df

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

# MAGIC %md
# MAGIC ###2.Filtrando o backfill

# COMMAND ----------

from pyspark.sql import functions as F

df_transactions_data_backfill = (
    df_transactions_data
        .filter(F.col("date") < "2017-01-01")
        .withColumn("ano", F.substring("date", 1, 4))
)

display(df_transactions_data_backfill.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Validando o filtro de data do backfill

# COMMAND ----------

display(df_transactions_data_backfill
        .groupBy("ano")
        .count()
        .orderBy("ano")
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ###4. Criando o objeto aninhado (struct)

# COMMAND ----------

from pyspark.sql import functions as F

df_transactions_struct = df_transactions_data_backfill.select(
    "id",
    "date",
    "ano",
    "client_id",
    "card_id",
    "amount",
    "use_chip",
    "errors",
    F.struct(
        F.col("merchant_id"),
        F.col("merchant_city"),
        F.col("merchant_state"),
        F.col("zip"),
        F.col("mcc")
    ).alias("merchant")
)

df_transactions_struct.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###5. Gravando de forma particionada em JSON

# COMMAND ----------

(df_transactions_struct
    .repartition("ano")
    .write
    .mode("overwrite")
    .partitionBy("ano")
    .json(f'/Volumes/{catalogo}/landing/raw_files/transacoes/backfill/')
 )

# COMMAND ----------

display(dbutils.fs.ls(f"/Volumes/{catalogo}/landing/raw_files/transacoes/backfill/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###6.Validando os arquivos JSON gravados

# COMMAND ----------

path_transactions = (f'/Volumes/{catalogo}/landing/raw_files/transacoes/backfill/')

df_transactions = spark.read.json(path_transactions)

df_transactions.printSchema()

display(df_transactions
        .groupBy("ano")
        .count()
        .orderBy("ano")
)

# COMMAND ----------

display(dbutils.fs.ls(f"/Volumes/{catalogo}/landing/raw_files/transacoes/backfill/ano=2010/"))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Resultado : transações do backfill
# MAGIC
# MAGIC - **Período:** 2010-01-01 a 2016-12-31 (ADR-04).
# MAGIC - **Leitura:** schema explícito com as 12 colunas como `STRING`. O gerador não interpreta o dado: valores como `$77.00` e `58523.0` chegam intactos na landing, e a limpeza é responsabilidade da Silver.
# MAGIC - **Estrutura do JSON:** nomes originais do Kaggle, com os dados do estabelecimento aninhados num objeto `merchant` (`merchant_id`, `merchant_city`, `merchant_state`, `zip`, `mcc`).
# MAGIC - **Gravação:** `repartition("ano")` + `partitionBy("ano")` + `mode("overwrite")` em `landing/raw_files/transacoes/backfill/`. Resultado: uma pasta `ano=XXXX/` por ano, com **um único arquivo** JSON Lines em cada uma. O `overwrite` torna o backfill idempotente.
# MAGIC - **Validação:** releitura da pasta com contagem por ano. Os 7 anos batem exatamente com a origem (9.351.849 transações no total).