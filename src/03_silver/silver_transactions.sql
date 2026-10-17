-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Tipagem e Estrutura

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE transacoes_tipadas
COMMENT 'tabela de transacoes tipada e estruturada'
AS SELECT
    t.id,
    CAST(t.date AS TIMESTAMP) AS data_transacao,
    t.client_id,
    t.card_id,
    CAST(REPLACE(t.amount, '$', '') AS DECIMAL(12, 2)) AS valor,
    valor < 0 AS flag_estorno,
    t.use_chip,
    t.merchant:merchant_id AS merchant_id,
    t.merchant:merchant_city AS merchant_city,
    t.merchant:merchant_state AS merchant_state,
    LPAD(SPLIT_PART(t.merchant:zip, '.', 1), 5, '0') AS merchant_zip,
    t.merchant:mcc AS merchant_mcc,
    SPLIT(t.errors, ',') AS errors,
    c.id IS NOT NULL AS cartao_existe,
    t._rescued_data,
    t._arquivo_origem,
    t._ingerido_em
FROM STREAM(fintech_dev.bronze.raw_transacoes) t
LEFT JOIN cartoes c
    ON t.card_id = c.id


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Regras e Validação

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE transacoes_validadas (
    CONSTRAINT id_preenchido EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valor_preenchido EXPECT (valor IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT data_preenchida EXPECT (data_transacao IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT use_chip_valido EXPECT (use_chip IN ('Online Transaction', 'Swipe Transaction', 'Chip Transaction')) ON VIOLATION DROP ROW,
    CONSTRAINT sem_dado_resgatado EXPECT (_rescued_data IS NULL),
    CONSTRAINT zip_ausente_justificado EXPECT (merchant_zip IS NOT NULL 
        OR use_chip = 'Online Transaction'
        OR COALESCE(LENGTH(merchant_state), 0) > 2),
    CONSTRAINT cartao_existente EXPECT (cartao_existe = true) ON VIOLATION DROP ROW
)
COMMENT 'tabela de transacoes com regras e validacoes'
AS SELECT *
FROM STREAM(transacoes_tipadas)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Quarentena

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE transacoes_quarentena
COMMENT 'tabela de quarentena das transacoes que cairam nas regras de DROP'
AS SELECT
    *,
    CASE
        WHEN id IS NULL THEN 'id_nulo'
        WHEN valor IS NULL THEN 'valor_nulo'
        WHEN data_transacao IS NULL THEN 'data_nulo'
        WHEN cartao_existe = false THEN 'cartao_inexistente'
        WHEN use_chip IS NULL
            OR use_chip NOT IN ('Online Transaction', 'Swipe Transaction', 'Chip Transaction') THEN 'use_chip_accepted_values'
    END AS motivo
FROM STREAM(transacoes_tipadas)
WHERE id IS NULL
    OR valor IS NULL
    OR data_transacao IS NULL
    OR cartao_existe = false
    OR use_chip IS NULL
    OR use_chip NOT IN ('Online Transaction', 'Swipe Transaction', 'Chip Transaction')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Deduplicando com AUTO CDC + SCD TYPE 1

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE transacoes
COMMENT 'tabela de transacoes';

CREATE FLOW fluxo_transacoes AS AUTO CDC INTO transacoes
FROM STREAM(transacoes_validadas)
KEYS (id)
SEQUENCE BY (_ingerido_em)
STORED AS SCD TYPE 1;