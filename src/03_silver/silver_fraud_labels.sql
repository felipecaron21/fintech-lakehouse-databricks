-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Tipagem e Estrutura

-- COMMAND ----------

CREATE TEMPORARY VIEW fraud_label_tipadas
COMMENT 'view de fraude tipada e estruturada'
AS SELECT
    transaction_id,
    CAST(is_fraud AS BOOLEAN) AS is_fraud,
    _arquivo_origem,
    _ingerido_em
FROM fintech_dev.bronze.raw_fraud_labels


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Regras e Validação

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW fraud_labels (
    CONSTRAINT transaction_id_preenchido EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT is_fraud_valido EXPECT (is_fraud IS NOT NULL)
)
COMMENT 'tabela de fraude com regras e validacoes'
AS SELECT *
FROM fraud_label_tipadas