SELECT
  DISTINCT
  SAFE_CAST(id_estacao AS STRING) id_estacao, 
  estacao,
  SAFE_CAST(latitude AS FLOAT64) latitude,
  SAFE_CAST(longitude AS FLOAT64) longitude
FROM `rj-cor.clima_pluviometro_staging.estacoes_cemaden`
ORDER BY SAFE_CAST(id_estacao AS INT)