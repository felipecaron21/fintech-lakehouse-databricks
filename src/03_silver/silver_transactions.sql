-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Tipagem e Estrutura

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE transacoes_tipadas
COMMENT 'tabela de transacoes tipada e estruturada'
AS SELECT
    id,
    CAST(date AS TIMESTAMP) AS data_transacao,
    client_id,
    card_id,
    CAST(REPLACE(amount, '$', '') AS DECIMAL(12, 2)) AS valor,
    valor < 0 AS flag_estorno,
    use_chip,
    merchant:merchant_id AS merchant_id,
    merchant:merchant_city AS merchant_city,
    merchant:merchant_state AS merchant_state,
    LPAD(SPLIT_PART(merchant:zip, '.', 1), 5, '0') AS merchant_zip,
    merchant:mcc AS merchant_mcc,
    SPLIT(errors, ',') AS errors,
    _rescued_data,
    _arquivo_origem,
    _ingerido_em
FROM STREAM(fintech_dev.bronze.raw_transacoes)


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
        OR COALESCE(LENGTH(merchant_state), 0) > 2)
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
        WHEN use_chip IS NULL
            OR use_chip NOT IN ('Online Transaction', 'Swipe Transaction', 'Chip Transaction') THEN 'use_chip_accepted_values'
    END AS motivo
FROM STREAM(transacoes_tipadas)
WHERE id IS NULL
    OR valor IS NULL
    OR data_transacao IS NULL
    OR use_chip IS NULL
    OR use_chip NOT IN ('Online Transaction', 'Swipe Transaction', 'Chip Transaction')