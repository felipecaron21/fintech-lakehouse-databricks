# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "faker",
# ]
# ///
# MAGIC %pip install faker==40.40.0

# COMMAND ----------

dbutils.widgets.text("catalogo", "fintech_dev")
catalogo = dbutils.widgets.get("catalogo")
print(catalogo)

# COMMAND ----------

# MAGIC %md
# MAGIC #transactions_data

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

# COMMAND ----------

# MAGIC %md
# MAGIC # users_data

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Definindo schema e df

# COMMAND ----------

schema_users = """
    id STRING,
    current_age STRING,
    retirement_age STRING,
    birth_year STRING,
    birth_month STRING,
    gender STRING,
    address STRING,
    latitude STRING,
    longitude STRING,
    per_capita_income STRING,
    yearly_income STRING,
    total_debt STRING,
    credit_score STRING,
    num_credit_cards STRING
"""

df_users_data = (spark.read
                .schema(schema_users)
                .option("header", True)
                .csv(f'/Volumes/{catalogo}/landing/raw_files/kaggle/users_data.csv/')
)

print(df_users_data.count())

# COMMAND ----------

from faker import Faker

fake = Faker("en_US")

# 1. Spark → Python: id e gênero de cada cliente
clientes = [(linha["id"], linha["gender"])
            for linha in df_users_data.select("id", "gender").collect()]

# 2. Identidade sintética por cliente, com semente = id
#    Ordem fixa das chamadas: ssn → primeiro nome → sobrenome → domínio do e-mail
identidades = []
for id_cliente, genero in clientes:
    fake.seed_instance(int(id_cliente))

    ssn = fake.ssn()
    primeiro_nome = fake.first_name_female() if genero == "Female" else fake.first_name_male()
    sobrenome = fake.last_name()
    dominio = fake.free_email_domain()          # ex.: gmail.com, yahoo.com, hotmail.com

    identidades.append({
        "id": id_cliente,
        "ssn": ssn,
        "name": f"{primeiro_nome} {sobrenome}",
        "email": f"{primeiro_nome}.{sobrenome}{id_cliente}@{dominio}".lower(),
    })

# 3. Python → Spark, e join com os clientes originais
df_identidades = spark.createDataFrame(identidades)

df_users_backfill = df_users_data.join(df_identidades, on="id", how="left")

display(df_users_backfill.select("id", "gender", "name", "ssn", "email").limit(10))
print(df_users_backfill.count())

# COMMAND ----------

(df_users_backfill
    .select("id", 
            "name", 
            "ssn", 
            "email", 
            "current_age", 
            "retirement_age", 
            "birth_year", 
            "birth_month", 
            "gender", 
            "address",
            "latitude",
            "longitude",
            "per_capita_income",
            "yearly_income",
            "total_debt",
            "credit_score",
            "num_credit_cards",
            )
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(f'/Volumes/{catalogo}/landing/raw_files/clientes/backfill/')
 )



# COMMAND ----------

display(dbutils.fs.ls(f'/Volumes/{catalogo}/landing/raw_files/clientes/backfill/'))

# COMMAND ----------

df_conferencia_users = (spark.read
    .option("header", True)
    .csv(f'/Volumes/{catalogo}/landing/raw_files/clientes/backfill/'))

df_conferencia_users.printSchema()
display(df_conferencia_users.limit(10))
print(df_conferencia_users.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ### Resultado : clientes do backfill
# MAGIC
# MAGIC - **Leitura:** schema explícito com as 14 colunas originais como `STRING` (o gerador não interpreta o dado).
# MAGIC - **Identidade sintética (ADR-05):** `ssn`, `name` e `email` gerados com Faker `en_US` 40.40.0 (versão fixada).
# MAGIC   - **Semente = `id` do cliente:** a mesma pessoa recebe sempre a mesma identidade, em qualquer execução.
# MAGIC   - **Ordem fixa das chamadas:** `ssn` → primeiro nome → sobrenome → domínio do e-mail. Mudar a ordem muda o resultado.
# MAGIC   - Nome coerente com o `gender`; e-mail derivado do nome + `id` (único por cliente).
# MAGIC - **Gravação:** CSV com cabeçalho, `coalesce(1)` (um único arquivo) e `overwrite`, em `landing/raw_files/clientes/backfill/`.
# MAGIC - **Validação:** releitura com 2.000 linhas e 17 colunas, sem deslocamento de colunas.