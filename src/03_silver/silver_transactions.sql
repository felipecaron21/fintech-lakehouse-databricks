-- Databricks notebook source
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
