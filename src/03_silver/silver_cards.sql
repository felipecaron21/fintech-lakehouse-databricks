-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Tipagem e Estrutura

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE cartoes_tipadas
COMMENT 'tabela de cartoes tipada e estruturada'
AS SELECT
    id,
    client_id,
    card_brand,
    card_type,
    LAST_DAY(TRY_TO_DATE(expires, 'MM/yyyy')) AS data_expiracao,
    CAST(has_chip AS BOOLEAN) AS possui_chip,
    CAST(num_cards_issued AS INT) AS num_cards_issued,
    CAST(REPLACE(credit_limit, '$', '') AS DECIMAL(12, 2)) AS limite_credito,
    TRY_TO_DATE(acct_open_date, 'MM/yyyy') AS data_abertura_conta,
    CAST(year_pin_last_changed AS INT) AS ano_troca_pin,
    CAST(card_on_dark_web AS BOOLEAN) AS na_dark_web,
    card_bin,
    card_last4,
    _arquivo_origem,
    _ingerido_em
FROM STREAM(fintech_dev.bronze.raw_cartoes)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Regras e Validação

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE cartoes_validadas (
    CONSTRAINT id_preenchido EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT card_type_valido EXPECT (card_type IN ('Credit', 'Debit', 'Debit (Prepaid)')),
    CONSTRAINT data_expiracao_valida EXPECT (data_expiracao IS NOT NULL),
    CONSTRAINT data_abertura_valida EXPECT (data_abertura_conta IS NOT NULL)
)
COMMENT 'tabela de cartoes com regras e validacoes'
AS SELECT *
FROM STREAM(cartoes_tipadas)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Atualização com AUTO CDC + SCD TYPE 1

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE cartoes
COMMENT 'tabela de cartoes';

CREATE FLOW fluxo_cartoes AS AUTO CDC INTO cartoes
FROM STREAM(cartoes_validadas)
KEYS (id)
SEQUENCE BY STRUCT(_ingerido_em, _arquivo_origem)
STORED AS SCD TYPE 1;
