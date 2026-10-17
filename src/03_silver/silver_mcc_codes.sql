-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Tipagem e Estrutura

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW mcc_codes_tipadas
COMMENT 'tabela de mcc codes tipada e estruturada'
AS SELECT
    mcc,
    descricao,
    _arquivo_origem,
    _ingerido_em
FROM fintech_dev.bronze.raw_mcc_codes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Regras e Validação

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW mcc_codes (
    CONSTRAINT mcc_preenchido EXPECT (mcc IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'tabela de mcc codes com regras e validacoes'
AS SELECT *
FROM mcc_codes_tipadas
