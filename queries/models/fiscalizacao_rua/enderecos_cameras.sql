SELECT
  id_camera,
  endereco_camera,
  SAFE_CAST(latitude as FLOAT64) latitude,
  SAFE_CAST(longitude as FLOAT64) longitude,
  posicao_camera
FROM `rj-cor.fiscalizacao_rua_staging.enderecos_cameras`
