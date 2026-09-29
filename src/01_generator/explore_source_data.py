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

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Explore transactions_data.csv

# COMMAND ----------

path_transactions_data = f'/Volumes/{catalogo}/landing/raw_files/kaggle/transactions_data.csv/'

df_transactions_data = spark.read.csv(path_transactions_data, header=True, inferSchema=False)

df_transactions_data.printSchema()

display(df_transactions_data.limit(10))
print(df_transactions_data.count())

# COMMAND ----------

from pyspark.sql import functions as F

if (df_transactions_data.select("id").distinct().count()) == (13305915):
    print("Valores únicos!")
else:
    print("Valores repetidos!")

df_transactions_data.filter(F.col("id").isNull()).count()

# COMMAND ----------

df_transacoes_clientes_orfaos = df_transactions_data.join(df_users_data, df_transactions_data["client_id"] == df_users_data["id"], "left_anti")
print(f'Quantidade de clientes orfaos em transaçoes: {df_transacoes_clientes_orfaos.count()}')

df_transacoes_cards_orfaos = df_transactions_data.join(df_cards_data, df_transactions_data["card_id"] == df_cards_data["id"], "left_anti")
print(f'Quantidade de cards orfaos em transaçoes: {df_transacoes_cards_orfaos.count()}')

# COMMAND ----------

from pyspark.sql import functions as F

display(df_transactions_data
        .groupBy("use_chip")
        .count()
        .orderBy(F.col("count").desc())
        )

# COMMAND ----------

from pyspark.sql import functions as F

display(df_transactions_data
        .groupBy("errors")
        .count()
        .orderBy(F.col("count").desc())
        )

# COMMAND ----------

from pyspark.sql import functions as F

display(df_transactions_data.agg(
            (F.min("date").alias("min_date")),
            (F.max("date").alias("max_date"))
        )
)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Resultados : transactions_data.csv
# MAGIC
# MAGIC - **Linhas:** 13.305.915 transações.
# MAGIC - **Período:** 2010-01-01 00:01 a 2019-10-31 23:59 (quase 10 anos).
# MAGIC - **Chave:** `id` é única e não tem nulos. Vira `transaction_id` no projeto.
# MAGIC - **Relacionamentos:** `card_id` → `cards_data.id` e `client_id` → `users_data.id`. Nenhuma transação órfã (validado com `left_anti` join).
# MAGIC - **Categoria:** `mcc` → `mcc_codes.json`. O `merchant_id` não tem uma tabela de estabelecimentos, então estabelecimento **não vira dimensão**.
# MAGIC - **Tipos (leitura sem `inferSchema`, tudo como `string`):**
# MAGIC   - `date`: timestamp do evento, em formato ISO.
# MAGIC   - `amount`: tem `$`. Converter para numérico.
# MAGIC   - `zip`: é um identificador. Manter como `string` e remover o sufixo `.0`.
# MAGIC - **Valores negativos em `amount`:** prováveis **estornos** (validado: há pares com o mesmo cartão, cliente e estabelecimento, com valores de sinais opostos). Os dados não permitem distinguir estorno de chargeback. Impacto na Gold: volume bruto x líquido, e estornos fora do ticket médio.
# MAGIC - **`use_chip`:** modo de captura — Swipe (tarja magnética, 6,97 mi), Chip (4,78 mi), Online (CNP, 1,56 mi).
# MAGIC - **`errors`:** vazio em 13,09 mi transações. **Premissa:** `errors` vazio = aprovada; preenchido = recusada (cerca de 211 mil, ~1,6%). O status será uma coluna derivada na Silver.
# MAGIC   - **Atributo multivalorado:** vários erros concatenados no mesmo texto (ex.: `Bad PIN,Insufficient Balance`). Tratamento na Silver (array, tabela filha ou flags) a decidir.
# MAGIC   - A soma das recusas por tipo de erro é maior que o total de recusas, porque uma transação pode ter mais de um erro.

# COMMAND ----------

# MAGIC %md
# MAGIC ###4. Explore mcc_codes.json

# COMMAND ----------

print(dbutils.fs.head(f'/Volumes/{catalogo}/landing/raw_files/kaggle/mcc_codes.json/', 500))

# COMMAND ----------

df_mcc_codes = spark.read.option("multiline", True).json(f'/Volumes/{catalogo}/landing/raw_files/kaggle/mcc_codes.json/')

df_mcc_codes_unpivot = df_mcc_codes.unpivot(
    ids=[],
    values=df_mcc_codes.columns,
    variableColumnName="mcc",
    valueColumnName="descricao"
)

display(df_mcc_codes_unpivot.limit(10))
print(df_mcc_codes_unpivot.count())



# COMMAND ----------

df_mcc_categorias = df_transactions_data.join(df_mcc_codes_unpivot, df_transactions_data["mcc"] == df_mcc_codes_unpivot["mcc"], "left_anti")

display(df_mcc_categorias.count())

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Resultados : mcc_codes.json
# MAGIC
# MAGIC - **Formato:** JSON **multiline**, com um único objeto em que cada **chave** é um código MCC e cada **valor** é a descrição da categoria. A leitura padrão (JSON Lines) falha e gera `_corrupt_record`; é preciso usar `multiLine`.
# MAGIC - **Forma:** lido como 1 linha x 109 colunas (formato largo). Convertido com **unpivot** para o formato longo (`mcc`, `descricao`), uma linha por categoria.
# MAGIC - **Linhas:** 109 categorias. `mcc` é único por construção (chaves de um objeto JSON não se repetem).
# MAGIC - **Relacionamento:** `transactions_data.mcc` → `mcc`. Nenhuma transação sem categoria (validado com `left_anti` join).
# MAGIC - **Vira:** `dim_categoria` na Gold.

# COMMAND ----------

# MAGIC %md
# MAGIC ###5. Explore train_fraud_labels.json

# COMMAND ----------

print(dbutils.fs.head(f'/Volumes/{catalogo}/landing/raw_files/kaggle/train_fraud_labels.json/', 500))

# COMMAND ----------

from pyspark.sql import functions as F

schema_fraud_labels = "target MAP<STRING, STRING>"

df_fraud_labels_raw = (spark.read
                       .schema(schema_fraud_labels)
                       .json(f'/Volumes/{catalogo}/landing/raw_files/kaggle/train_fraud_labels.json/')
                       )

df_fraud_labels = df_fraud_labels_raw.select(
F.explode("target").alias("transaction_id", "is_fraud")
)

display(df_fraud_labels.limit(10))
print(df_fraud_labels.count())

# COMMAND ----------

from pyspark.sql import functions as F

display(df_fraud_labels
        .groupBy("is_fraud")
        .count()
        .orderBy(F.col("count").desc())
        )

# COMMAND ----------

df_fraud_rotulos_orfaos = df_fraud_labels.join(df_transactions_data, df_fraud_labels["transaction_id"] == df_transactions_data["id"], "left_anti")

display(df_fraud_rotulos_orfaos.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resultados — train_fraud_labels.json
# MAGIC
# MAGIC - **Formato:** um objeto com uma única chave `target`, cujo valor é um objeto que mapeia `transaction_id` → `"Yes"`/`"No"`. O arquivo inteiro está numa única linha.
# MAGIC - **Leitura:** com **schema explícito** (`target MAP<STRING, STRING>`) e `explode` para gerar uma linha por transação. Com inferência, cada ID viraria um campo de `struct` (milhões de campos).
# MAGIC - **Linhas:** 8.914.963 transações rotuladas (~67% das 13.305.915). Cerca de 4,4 milhões de transações **não têm rótulo**.
# MAGIC - **Relacionamento:** `transaction_id` → `transactions_data.id`. Nenhum rótulo órfão (validado com `left_anti` join; join entre duas tabelas grandes, sem broadcast).
# MAGIC - **Distribuição:** 8.901.631 `No` e 13.332 `Yes`. **Taxa de fraude ≈ 0,15%** (1 a cada ~670) — desbalanceamento de classes típico de fraude.
# MAGIC - **Impacto na modelagem:** `flag_fraude` na `fato_transacoes` com três estados: `true`, `false` e `null` (**não avaliada**). Tratar `null` como `false` aumentaria o denominador e subestimaria a taxa de fraude em cerca de um terço.
# MAGIC - **Hipótese a validar:** o recorte dos rótulos é temporal ou aleatório.