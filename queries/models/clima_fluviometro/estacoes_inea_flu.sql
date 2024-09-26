SELECT
  CAST(id_estacao          as	STRING) id_estacao,
  CAST(estacao             as	STRING) estacao,
  CAST(tipo_estacao                as	STRING) tipo_estacao,
  CAST(reservatorio_monitorado      as STRING) reservatorio_monitorado, 
  CAST(regiao_hidrografica as	STRING) regiao_hidrografica,
  CAST(bacia							 as	STRING) bacia,
  CAST(latitude						 as	FLOAT64) latitude,
  CAST(longitude					 as	FLOAT64) longitude,
  CAST(altitude						 as	STRING) altitude,
  CAST(data_implantacao 	 as	DATE) data_implantacao

FROM `rj-cor.clima_fluviometro_staging.estacoes_inea` 
ORDER BY CAST(id_estacao As int)