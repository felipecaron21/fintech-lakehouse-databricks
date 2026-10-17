-- Databricks notebook source
CREATE OR REPLACE VIEW fintech_dev.ops.metricas_qualidade
COMMENT 'Métricas das expectations do pipeline_silver por execução, tabela e regra'
AS
WITH regras AS (
  SELECT
    timestamp,
    origin.update_id AS id_execucao,
    explode(
      from_json(
        details:flow_progress.data_quality.expectations,
        'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>'
      )
    ) AS e
  FROM fintech_dev.ops.event_log_silver
  WHERE event_type = 'flow_progress'
    AND details:flow_progress.data_quality.expectations IS NOT NULL
)
SELECT
  id_execucao,
  MAX(timestamp)                                AS momento,
  regexp_extract(e.dataset, '([^.]+)$', 1)      AS tabela,   -- mudança 1: só o nome, sem catálogo e schema
  e.name                                        AS regra,
  SUM(e.passed_records)                         AS linhas_aprovadas,
  SUM(e.failed_records)                         AS linhas_violadas,
  ROUND(100.0 * SUM(e.failed_records)
        / NULLIF(SUM(e.passed_records) + SUM(e.failed_records), 0), 4) AS pct_violacao
FROM regras
GROUP BY ALL                                                  -- mudança 2: agrupa pelas colunas não agregadas