{{
    config(
        materialized='incremental',
        unique_key="primary_key",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        post_hook='CREATE OR REPLACE TABLE `rj-cor.clima_estacao_meteorologica_staging.meteorologia_inmet_last_partition` AS (SELECT CURRENT_DATETIME("America/Sao_Paulo") AS data_particao)'
    )
}}

SELECT 
    DISTINCT
    CONCAT(id_estacao, '_', data, ' ', horario) AS primary_key,
    SAFE_CAST(
        REGEXP_REPLACE(id_estacao, r'\.0$', '') AS STRING
        ) id_estacao,
    SAFE_CAST(pressao AS FLOAT64) pressao,
    SAFE_CAST(pressao_minima AS FLOAT64) pressao_minima,
    SAFE_CAST(pressao_maxima AS FLOAT64) pressao_maxima,
    SAFE_CAST(temperatura_orvalho AS FLOAT64) temperatura_orvalho,
    SAFE_CAST(temperatura_orvalho_minimo AS FLOAT64) temperatura_orvalho_minimo,
    SAFE_CAST(temperatura_orvalho_maximo AS FLOAT64) temperatura_orvalho_maximo,
    SAFE_CAST(SAFE_CAST(umidade AS FLOAT64) AS INT64) umidade,
    SAFE_CAST(SAFE_CAST(umidade_minima AS FLOAT64) AS INT64) umidade_minima,
    SAFE_CAST(SAFE_CAST(umidade_maxima AS FLOAT64) AS INT64) umidade_maxima,
    SAFE_CAST(temperatura AS FLOAT64) temperatura,
    SAFE_CAST(temperatura_minima AS FLOAT64) temperatura_minima,
    SAFE_CAST(temperatura_maxima AS FLOAT64) temperatura_maxima,
    SAFE_CAST(rajada_vento_max AS FLOAT64) rajada_vento_max,
    SAFE_CAST(SAFE_CAST(direcao_vento AS FLOAT64) AS INT64) direcao_vento,
    SAFE_CAST(velocidade_vento AS FLOAT64) velocidade_vento,
    SAFE_CAST(radiacao_global AS FLOAT64) radiacao_global,
    SAFE_CAST(acumulado_chuva_1_h AS FLOAT64) acumulado_chuva_1_h,
    SAFE_CAST(
        SAFE.PARSE_TIME('%H:%M:%S', horario) AS TIME
        ) horario,
    SAFE_CAST(data  AS DATE) data_particao,
FROM `rj-cor.clima_estacao_meteorologica_staging.meteorologia_inmet`


{% if is_incremental() %}

{% set max_partition = run_query(
    "SELECT DATE(gr) FROM (
        SELECT IF(
            max(data_particao) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(data_particao)
            ) as gr 
        FROM `rj-cor.clima_estacao_meteorologica_staging.meteorologia_inmet_last_partition`
        )
    ").columns[0].values()[0] %}

WHERE
    ano >= SAFE_CAST(EXTRACT(YEAR FROM SAFE_CAST("{{ max_partition }}" AS DATE)) AS STRING) AND
    mes >= SAFE_CAST(EXTRACT(MONTH FROM SAFE_CAST("{{ max_partition }}" AS DATE)) AS STRING) AND
    dia >= SAFE_CAST(EXTRACT(DAY FROM SAFE_CAST("{{ max_partition }}" AS DATE)) AS STRING)

AND
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CONCAT(data, ' ', horario)) AS DATETIME
    ) > ("{{ max_partition }}")

{% endif %}
