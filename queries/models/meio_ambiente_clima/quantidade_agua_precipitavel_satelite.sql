{{
    config(
        materialized='incremental',
        unique_key="primary_key",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        post_hook='CREATE OR REPLACE TABLE `rj-cor.meio_ambiente_clima_staging.quantidade_agua_precipitavel_satelite_last_partition` AS (SELECT CURRENT_DATETIME("America/Sao_Paulo") AS data_particao)'
    )
}}

SELECT
    CONCAT(data_particao, '_', CONCAT(SAFE_CAST(hora AS STRING), ":00:00"), '_', latitude, '_', longitude) AS primary_key,
    SAFE_CAST(longitude AS FLOAT64) longitude,
    SAFE_CAST(latitude AS FLOAT64) latitude,
    SAFE_CAST(tpw AS FLOAT64) tpw,
    SAFE_CAST(CONCAT(SAFE_CAST(hora AS STRING), ":00:00") AS TIME) horario,
    SAFE_CAST(DATE_TRUNC(DATE(data_particao), day) AS DATE) data_particao,
FROM `rj-cor.meio_ambiente_clima_staging.quantidade_agua_precipitavel_satelite`

{% if is_incremental() %}

{% set max_partition = run_query(
    "
    SELECT IF(
        max(data_particao) > CURRENT_DATETIME('America/Sao_Paulo'), CURRENT_DATETIME('America/Sao_Paulo'), max(data_particao)
        ) as gr 
    FROM `rj-cor.meio_ambiente_clima_staging.quantidade_agua_precipitavel_satelite_last_partition`
    ").columns[0].values()[0] %}

WHERE 
    ano_particao >= EXTRACT(YEAR FROM SAFE_CAST("{{ max_partition }}" AS DATETIME)) AND
    mes_particao >= EXTRACT(MONTH FROM SAFE_CAST("{{ max_partition }}" AS DATETIME)) AND
    data_particao >= EXTRACT(DATE FROM SAFE_CAST("{{ max_partition }}" AS DATETIME)) AND
    hora >= EXTRACT(HOUR FROM SAFE_CAST("{{ max_partition }}" AS DATETIME))
{% endif %}

--rj-cor