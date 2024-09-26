SELECT
  DISTINCT
  SAFE_CAST(id_estacao AS STRING) id_estacao, 
  estacao,
  SAFE_CAST(latitude AS FLOAT64) latitude,
  SAFE_CAST(longitude AS FLOAT64) longitude,
  SAFE_CAST(altitude AS FLOAT64) altitude,
  SAFE_CAST(data_atualizacao AS DATE) data_atualizacao
FROM `rj-cor.clima_estacao_meteorologica_staging.estacoes_redemet` 
ORDER BY id_estacao