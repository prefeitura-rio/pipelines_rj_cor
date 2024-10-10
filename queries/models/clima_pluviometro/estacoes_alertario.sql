SELECT
  DISTINCT
  SAFE_CAST(id_estacao AS STRING) id_estacao,
  estacao,
  SAFE_CAST(latitude AS FLOAT64) latitude,
  SAFE_CAST(longitude AS FLOAT64) longitude,
  SAFE_CAST(cota AS FLOAT64) cota,
  SAFE_CAST(x AS FLOAT64) x,
  SAFE_CAST(y AS FLOAT64) y,
  endereco,
  situacao,
  SAFE_CAST(data_inicio_operacao AS DATETIME) data_inicio_operacao,
  SAFE_CAST(data_fim_operacao AS DATETIME) data_fim_operacao,
  SAFE_CAST(data_atualizacao AS DATETIME) data_atualizacao
FROM `rj-cor.clima_pluviometro_staging.estacoes_alertario`
ORDER BY CAST(id_estacao AS INT)