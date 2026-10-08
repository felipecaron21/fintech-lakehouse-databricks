-- Databricks notebook source
CREATE WIDGET TEXT catalogo DEFAULT 'fintech_dev';

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalogo);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bronze.raw_cartoes

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE caminho_cartoes STRING;
SET VARIABLE caminho_cartoes = '/Volumes/' || :catalogo || '/landing/raw_files/cartoes/';

DECLARE OR REPLACE VARIABLE comando_copy STRING;
SET VARIABLE comando_copy = 
    "COPY INTO bronze.raw_cartoes
FROM (
    SELECT
    * EXCEPT (cvv, card_number),
    substring(card_number, 1, 6) AS card_bin,
    right(card_number, 4) AS card_last4,
    _metadata.file_path AS _arquivo_origem,
    current_timestamp AS _ingerido_em
FROM '" || caminho_cartoes || "'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'recursiveFileLookup' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')";

EXECUTE IMMEDIATE comando_copy;

-- COMMAND ----------

SELECT
    COUNT(*) AS registros
FROM bronze.raw_cartoes
GROUP BY _arquivo_origem;