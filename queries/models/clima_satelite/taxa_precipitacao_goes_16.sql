{{
    config(
        materialized='incremental',
        unique_key="primary_key",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        post_hook='CREATE OR REPLACE TABLE `rj-cor.clima_satelite_staging.taxa_precipitacao_goes_16_last_partition` AS (SELECT CURRENT_DATETIME("America/Sao_Paulo") AS data_particao)'
    )
}}

SELECT
    CONCAT(data_particao, '_', horario, '_', latitude, '_', longitude) AS primary_key,
    SAFE_CAST(longitude AS FLOAT64) longitude,
    SAFE_CAST(latitude AS FLOAT64) latitude,
    SAFE_CAST(rrqpe AS FLOAT64) rrqpe,
    SAFE_CAST(horario AS TIME) horario,
    SAFE_CAST(DATE_TRUNC(DATE(data_particao), day) AS DATE) data_particao,
FROM `rj-cor.clima_satelite_staging.taxa_precipitacao_goes_16`


{% if is_incremental() %}

{% set max_partition = run_query(
    "SELECT IF(
        max(data_particao) > CURRENT_DATETIME('America/Sao_Paulo'), CURRENT_DATETIME('America/Sao_Paulo'), max(data_particao)
        ) as gr 
    FROM `rj-cor.clima_satelite_staging.taxa_precipitacao_goes_16_last_partition`
    ").columns[0].values()[0] %}

WHERE 
    ano_particao >= EXTRACT(YEAR FROM SAFE_CAST("{{ max_partition }}" AS DATETIME)) AND
    mes_particao >= EXTRACT(MONTH FROM SAFE_CAST("{{ max_partition }}" AS DATETIME)) AND
    data_particao >= EXTRACT(DATE FROM SAFE_CAST("{{ max_partition }}" AS DATETIME)) AND
    hora >= EXTRACT(HOUR FROM SAFE_CAST("{{ max_partition }}" AS DATETIME))
{% endif %}
