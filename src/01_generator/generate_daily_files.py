# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "faker",
# ]
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

df_transactions_data = (spark.read
                        .parquet(f'/Volumes/{catalogo}/landing/raw_files/kaggle/prepared/transacoes_incremental/')
)

display(df_transactions_data.limit(20))

# COMMAND ----------

from pyspark.sql import functions as F

var_month = data_simulada[:7]

df_transactions_data_file = (
    df_transactions_data
        .filter(F.col("month_year") == var_month)
        .filter(F.substring("date", 1, 10) == data_simulada)
)

display(df_transactions_data_file.limit(20))
print(df_transactions_data_file.count())

# COMMAND ----------

from pyspark.sql import functions as F

df_transactions_daily_struct = df_transactions_data_file.select(
    "id",
    "date",
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
    ).alias("merchant"),
    F.lit(data_simulada).alias("date_partition"),
)

df_transactions_daily_struct.printSchema()

# COMMAND ----------

path_daily_file = (f'Volumes/{catalogo}/landing/raw_files/transacoes/daily/')

(df_transactions_daily_struct 
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("partitionOverWriteMode", "dynamic")
    .partitionBy("date_partition")
    .json(path_daily_file)
 )

# COMMAND ----------

display(dbutils.fs.ls(f'/Volumes/{catalogo}/landing/raw_files/transacoes/daily/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resultado : transações do dia
# MAGIC
# MAGIC - **Fonte:** Parquet particionado por `month_year` (ADR-06). O gerador lê só a partição do mês e filtra o dia da `data_simulada` (~1,1 s).
# MAGIC - **Contrato do arquivo:** igual ao backfill — nomes do Kaggle, todos os valores como texto, estabelecimento aninhado em `merchant`.
# MAGIC - **Gravação:** JSON em `landing/raw_files/transacoes/daily/`, particionado por `date_partition`, com *dynamic partition overwrite* configurado na própria gravação (ADR-07).
# MAGIC - **Validação:**
# MAGIC   - 3 dias gerados → 3 partições (2017-01-01: 3.989; 2017-01-02: 3.494; 2017-01-03: 3.930).
# MAGIC   - Reexecução de 2017-01-01 → continua com 3.989 linhas, e os outros dias não foram alterados.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mudanças de clientes

# COMMAND ----------

# --- Mudanças de clientes do dia (ADR-08) ---
from client_changes import reconstruir_estado
from identity import gerar_identidade

# 1. Estado inicial no D0: clientes do Kaggle + identidade sintética (ADR-05)
schema_users = """
    id STRING, current_age STRING, retirement_age STRING, birth_year STRING,
    birth_month STRING, gender STRING, address STRING, latitude STRING,
    longitude STRING, per_capita_income STRING, yearly_income STRING,
    total_debt STRING, credit_score STRING, num_credit_cards STRING
"""

df_users_kaggle = (spark.read
    .schema(schema_users)
    .option("header", True)
    .csv(f"/Volumes/{catalogo}/landing/raw_files/kaggle/users_data.csv"))

clientes_d0 = {}
for linha in df_users_kaggle.collect():
    registro = linha.asDict()
    registro.update(gerar_identidade(registro["id"], registro["gender"]))
    clientes_d0[registro["id"]] = registro

# 2. Replay do D0 até a data simulada (só em memória)
estado, mudados_no_dia = reconstruir_estado(data, clientes_d0)
print(f"{data_simulada}: {len(mudados_no_dia)} clientes com mudança")

# 3. Grava o registro completo dos clientes que mudaram hoje (mesma ordem de colunas do backfill)
colunas_clientes = [
    "id", "name", "ssn", "email", "current_age", "retirement_age", "birth_year",
    "birth_month", "gender", "address", "latitude", "longitude", "per_capita_income",
    "yearly_income", "total_debt", "credit_score", "num_credit_cards",
]

if mudados_no_dia:
    registros_do_dia = [estado[id_cliente] for id_cliente in mudados_no_dia]

    df_clientes_dia = (spark.createDataFrame(registros_do_dia)
        .select(*colunas_clientes)
        .withColumn("date_partition", F.lit(data_simulada)))

    (df_clientes_dia
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .option("header", True)
        .partitionBy("date_partition")
        .csv(f"/Volumes/{catalogo}/landing/raw_files/clientes/daily/"))

    display(df_clientes_dia)
else:
    print("Dia sem mudanças de clientes: nenhum arquivo gravado.")