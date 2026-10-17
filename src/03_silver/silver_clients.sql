-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Tipagem e Estrutura

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE clientes_tipadas
COMMENT 'tabela de cliente tipada e estruturada'
AS SELECT
    id,
    name,
    ssn,
    email,
    CAST(current_age AS INT) AS current_age,
    CAST(retirement_age AS INT) AS retirement_age,
    MAKE_DATE(CAST(birth_year AS INT), CAST(birth_month AS INT), 1) AS data_nascimento,
    gender,
    address,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    CAST(REPLACE(per_capita_income, '$', '') AS DECIMAL(12, 2)) AS renda_per_capita,
    CAST(REPLACE(yearly_income, '$', '') AS DECIMAL(12, 2)) AS renda_anual,
    CAST(REPLACE(total_debt, '$', '') AS DECIMAL(12, 2)) AS divida_total,
    CAST(credit_score AS INT) AS credit_score,
    CAST(num_credit_cards AS INT) AS num_credit_cards,
    _rescued_data,
    _arquivo_origem,
    _ingerido_em
FROM STREAM(fintech_dev.bronze.raw_clientes)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Regras e Validações

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE clientes_validadas (
    CONSTRAINT id_preenchido EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT range_credit_score EXPECT (credit_score BETWEEN 300 AND 850),
    CONSTRAINT gender_valido EXPECT (gender IN ('Male', 'Female')),
    CONSTRAINT data_nascimento_preenchido EXPECT (data_nascimento IS NOT NULL),
    CONSTRAINT sem_dado_resgatado EXPECT (_rescued_data IS NULL)
)
COMMENT 'tabela de clientes com regras e validacoes'
AS SELECT *
FROM STREAM(clientes_tipadas)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Atualização com AUTO CDC + SCD TYPE 2

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE clientes
COMMENT 'tabela de clientes';

CREATE FLOW fluxo_clientes AS AUTO CDC INTO clientes
FROM STREAM(clientes_validadas)
KEYS (id)
SEQUENCE BY STRUCT (_ingerido_em, _arquivo_origem)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (_arquivo_origem, _ingerido_em, _rescued_data);