# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

dbutils.widgets.text("catalogo", "fintech_dev")
catalogo = dbutils.widgets.get("catalogo")
print(catalogo)

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Explore users_data.csv

# COMMAND ----------

#Montando o caminho
path_users_data = f'/Volumes/{catalogo}/landing/raw_files/kaggle/users_data.csv/'

#Carregando o arquivo
df_users_data = spark.read.csv(path_users_data, header=True, inferSchema=True)

#Estrutura do DataFrame
df_users_data.printSchema()

#Visualizar as primeiras linhas
display(df_users_data.limit(10))
df_users_data.count()

# COMMAND ----------

display(df_users_data.select("per_capita_income", "yearly_income", "total_debt").limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

if df_users_data.count() == (df_users_data.select("id").distinct().count()):
    print("Valores únicos!")
else:
    print("Valores repetidos!")

df_users_data.filter(F.col("id").isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resultados : users_data.csv
# MAGIC
# MAGIC - **Linhas:** 2.000 clientes.
# MAGIC - **Chave:** `id` é única e não tem nulos (validado com `count` x `distinct().count()` e filtro de nulos). Vira `client_id` no projeto.
# MAGIC - **Tipos incorretos:** `per_capita_income`, `yearly_income` e `total_debt` foram lidas como `string` por causa do `$` nos valores (ex.: `$29278`).
# MAGIC - **Tratamento:** remoção do `$` e conversão para numérico na **Silver**. A Bronze mantém o valor original, para permitir reprocessamento.

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Explore cards_data.csv

# COMMAND ----------

path_cards_data = f'/Volumes/{catalogo}/landing/raw_files/kaggle/cards_data.csv/'

df_cards_data = spark.read.csv(path_cards_data, header=True, inferSchema=True)

df_cards_data.printSchema()

display(df_cards_data.limit(10))
print(df_cards_data.count())

# COMMAND ----------

display(df_cards_data.select("expires", "credit_limit", "acct_open_date", "has_chip", "card_on_dark_web").limit(10))

# COMMAND ----------

df_cards_orfao = df_cards_data.join(df_users_data, df_cards_data["client_id"] == df_users_data["id"], "left_anti")
print(df_cards_orfao.count())


# COMMAND ----------

from pyspark.sql import functions as F

if df_cards_data.count() == (df_cards_data.select("id").distinct().count()):
    print("Valores únicos!")
else:
    print("Valores repetidos!")

df_cards_data.filter(F.col("id").isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Resultados : cards_data.csv
# MAGIC
# MAGIC - **Linhas:** 6.146 cartões (média de ~3 por cliente).
# MAGIC - **Chave:** `id` é única e não tem nulos. Vira `card_id` no projeto.
# MAGIC - **Relacionamento:** `client_id` → `users_data.id`. Nenhum cartão órfão (validado com `left_anti` join).
# MAGIC - **Tipos incorretos:**
# MAGIC   - `credit_limit`: `string` por causa do `$`. Converter para numérico.
# MAGIC   - `expires` e `acct_open_date`: `string` no formato `MM/AAAA`. Converter para data, conforme o significado: `acct_open_date` → primeiro dia do mês; `expires` → último dia do mês (o cartão vale até o fim do mês de validade).
# MAGIC   - `has_chip` e `card_on_dark_web`: `string` com sim/não. Converter para booleano.
# MAGIC   - `card_number`: lida como `long`, mas é um identificador. Tratar como `string`.
# MAGIC - **Tratamento dos tipos:** na **Silver**.
# MAGIC - **Dados sensíveis (PCI DSS):**
# MAGIC   - `card_number` (PAN): mascarar (4 últimos dígitos) ou tokenizar.
# MAGIC   - `cvv`: **não pode ser armazenado** depois da autorização da compra.
# MAGIC   - O ponto do pipeline em que isso será tratado (exceção à regra da Bronze) será decidido na Etapa 2.