SELECT
  DISTINCT
  intensidade_vento,
  SAFE_CAST(velocidade_minima AS FLOAT64) velocidade_minima,
  SAFE_CAST(velocidade_maxima AS FLOAT64) velocidade_maxima,
  SAFE_CAST(data_atualizacao AS DATE) data_atualizacao
FROM `rj-cor.clima_estacao_meteorologica_staging.intensidade_vento`