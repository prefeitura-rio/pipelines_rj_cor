SELECT
  DISTINCT
  SAFE_CAST(id_estacao AS STRING) id_estacao, 
  estacao,
  SAFE_CAST(latitude AS FLOAT64) latitude,
  SAFE_CAST(longitude AS FLOAT64) longitude,
  situacao,
  tipo_estacao,
  entidade_responsavel,
  SAFE_CAST(data_inicio_operacao AS DATE) data_inicio_operacao,
  SAFE_CAST(data_fim_operacao AS DATE) data_fim_operacao,
  SAFE_CAST(data_atualizacao AS DATE) data_atualizacao
FROM `rj-cor.clima_estacao_meteorologica_staging.estacoes_inmet` 
ORDER BY id_estacao