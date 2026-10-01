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

# --- Transações do dia com defeitos de qualidade + gabarito (ADR-09) ---
# Substitui as células de transações do dia (leitura filtrada, select do merchant e gravação).
from datetime import date, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from transaction_defects import GeradorDeDefeitos, D0, DATA_COLUNA_NOVA

# 1. Fonte: transações do dia e dos 14 dias anteriores (necessárias para atrasadas e reenviadas)
inicio_janela = max(D0, data - timedelta(days=14))
dias_janela = [inicio_janela + timedelta(days=k) for k in range((data - inicio_janela).days + 1)]
meses_janela = sorted({d.strftime("%Y-%m") for d in dias_janela})

df_janela = (spark.read
    .parquet(f"/Volumes/{catalogo}/landing/raw_files/kaggle/prepared/transacoes_incremental/")
    .filter(F.col("month_year").isin(meses_janela))                       # partition pruning
    .filter(F.substring("date", 1, 10).between(str(inicio_janela), str(data)))
    .drop("month_year")
    .orderBy(F.col("id").cast("long")))                                    # ordem estável para o sorteio

transacoes_por_dia = {d: [] for d in dias_janela}
for linha in df_janela.collect():
    transacoes_por_dia[date.fromisoformat(linha["date"][:10])].append(linha.asDict())

# 2. Aplica os defeitos (determinístico: mesmo dia → mesmo arquivo e mesmo gabarito)
linhas_arquivo, gabarito = GeradorDeDefeitos(transacoes_por_dia).gerar_dia(data)

# 3. Schema do arquivo: device_type só existe a partir da DATA_COLUNA_NOVA
campos = [StructField(nome, StringType()) for nome in
          ["id", "date", "client_id", "card_id", "amount", "use_chip", "errors"]]
campos.append(StructField("merchant", StructType([StructField(nome, StringType()) for nome in
              ["merchant_id", "merchant_city", "merchant_state", "zip", "mcc"]])))
if data >= DATA_COLUNA_NOVA:
    campos.append(StructField("device_type", StringType()))

df_transacoes_dia = (spark.createDataFrame(linhas_arquivo, StructType(campos))
    .withColumn("date_partition", F.lit(data_simulada)))

# 4. Grava o arquivo do dia (ignoreNullFields=False: o defeito aparece como "amount": null)
(df_transacoes_dia
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .option("ignoreNullFields", False)
    .partitionBy("date_partition")
    .json(f"/Volumes/{catalogo}/landing/raw_files/transacoes/daily/"))

# 5. Grava o gabarito do dia (fora das pastas lidas pelo pipeline)
schema_gabarito = StructType([StructField(nome, StringType()) for nome in
                              ["date_partition", "tipo_defeito", "transaction_id", "detalhe"]])
if gabarito:
    (spark.createDataFrame(gabarito, schema_gabarito)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .option("header", True)
        .partitionBy("date_partition")
        .csv(f"/Volumes/{catalogo}/landing/raw_files/kaggle/prepared/gabarito_defeitos/"))

# 6. Resumo
print(f"{data_simulada}: {len(transacoes_por_dia[data])} transações na origem, "
      f"{len(linhas_arquivo)} no arquivo")
display(spark.createDataFrame(gabarito, schema_gabarito).groupBy("tipo_defeito").count()
        if gabarito else spark.createDataFrame([], schema_gabarito))

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
from pyspark.sql import functions as F
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mundanças de cartões

# COMMAND ----------

# --- Mudanças de cartões do dia (ADR-08, SCD1 na Gold) ---
from pyspark.sql import functions as F
from card_changes import reconstruir_estado as reconstruir_estado_cartoes

# 1. Estado inicial no D0: cartões do Kaggle, como chegaram (tudo como texto)
schema_cards = """
    id STRING, client_id STRING, card_brand STRING, card_type STRING,
    card_number STRING, expires STRING, cvv STRING, has_chip STRING,
    num_cards_issued STRING, credit_limit STRING, acct_open_date STRING,
    year_pin_last_changed STRING, card_on_dark_web STRING
"""

df_cards_kaggle = (spark.read
    .schema(schema_cards)
    .option("header", True)
    .csv(f"/Volumes/{catalogo}/landing/raw_files/kaggle/cards_data.csv"))

cartoes_d0 = {linha["id"]: linha.asDict() for linha in df_cards_kaggle.collect()}

# 2. Replay do D0 até a data simulada (só em memória)
estado_cartoes, cartoes_mudados_no_dia = reconstruir_estado_cartoes(data, cartoes_d0)
print(f"{data_simulada}: {len(cartoes_mudados_no_dia)} cartões com mudança")

# 3. Grava o registro completo dos cartões que mudaram hoje (mesma ordem de colunas do backfill)
colunas_cartoes = [
    "id", "client_id", "card_brand", "card_type", "card_number", "expires", "cvv",
    "has_chip", "num_cards_issued", "credit_limit", "acct_open_date",
    "year_pin_last_changed", "card_on_dark_web",
]

if cartoes_mudados_no_dia:
    registros_do_dia = [estado_cartoes[id_cartao] for id_cartao in cartoes_mudados_no_dia]

    df_cartoes_dia = (spark.createDataFrame(registros_do_dia)
        .select(*colunas_cartoes)
        .withColumn("date_partition", F.lit(data_simulada)))

    (df_cartoes_dia
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .option("header", True)
        .partitionBy("date_partition")
        .csv(f"/Volumes/{catalogo}/landing/raw_files/cartoes/daily/"))

    display(df_cartoes_dia)
else:
    print("Dia sem mudanças de cartões: nenhum arquivo gravado.")

# COMMAND ----------

df_transacoes_dia.printSchema()