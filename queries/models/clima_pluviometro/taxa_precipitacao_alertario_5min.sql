{{
    config(
        materialized='incremental',
        unique_key="primary_key",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month", 
        },
        post_hook='CREATE OR REPLACE TABLE `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min_last_partition` AS (SELECT CURRENT_DATETIME("America/Sao_Paulo") AS data_particao)'
    )
}}

WITH remove_extreme_values as (
    SELECT 
        SAFE_CAST(
            REGEXP_REPLACE(id_estacao, r'\.0$', '') AS STRING
        ) id_estacao,
        data_medicao,
        CASE WHEN SAFE_CAST(acumulado_chuva_5min AS FLOAT64) < 0 THEN NULL ELSE acumulado_chuva_5min END acumulado_chuva_5min,
        CASE WHEN SAFE_CAST(acumulado_chuva_10min AS FLOAT64) < 0 THEN NULL ELSE acumulado_chuva_10min END acumulado_chuva_10min,
        CASE WHEN SAFE_CAST(acumulado_chuva_15min AS FLOAT64) < 0 THEN NULL ELSE acumulado_chuva_15min END acumulado_chuva_15min,
        CASE WHEN SAFE_CAST(acumulado_chuva_30min AS FLOAT64) < 0 THEN NULL ELSE acumulado_chuva_30min END acumulado_chuva_30min,
        CASE WHEN SAFE_CAST(acumulado_chuva_1h AS FLOAT64)    < 0 THEN NULL ELSE acumulado_chuva_1h END acumulado_chuva_1h,
        CASE WHEN SAFE_CAST(acumulado_chuva_2h AS FLOAT64)    < 0 THEN NULL ELSE acumulado_chuva_2h END acumulado_chuva_2h,
        CASE WHEN SAFE_CAST(acumulado_chuva_3h AS FLOAT64)    < 0 THEN NULL ELSE acumulado_chuva_3h END acumulado_chuva_3h,
        CASE WHEN SAFE_CAST(acumulado_chuva_4h AS FLOAT64)    < 0 THEN NULL ELSE acumulado_chuva_4h END acumulado_chuva_4h,
        CASE WHEN SAFE_CAST(acumulado_chuva_6h AS FLOAT64)    < 0 THEN NULL ELSE acumulado_chuva_6h END acumulado_chuva_6h,
        CASE WHEN SAFE_CAST(acumulado_chuva_12h AS FLOAT64)    < 0 THEN NULL ELSE acumulado_chuva_12h END acumulado_chuva_12h,
        CASE WHEN SAFE_CAST(acumulado_chuva_24h AS FLOAT64)   < 0 THEN NULL ELSE acumulado_chuva_24h END acumulado_chuva_24h,
        CASE WHEN SAFE_CAST(acumulado_chuva_96h AS FLOAT64)   < 0 THEN NULL ELSE acumulado_chuva_96h END acumulado_chuva_96h,
        CASE WHEN SAFE_CAST(acumulado_chuva_mes AS FLOAT64)    < 0 THEN NULL ELSE acumulado_chuva_mes END acumulado_chuva_mes,
    FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
    
    {% if is_incremental() %}

    {% set max_partition = run_query(
        "SELECT DATE(gr) FROM (
            SELECT IF(
                max(data_particao) > CURRENT_DATE('America/Sao_Paulo'), 
                CURRENT_DATE('America/Sao_Paulo'), 
                max(data_particao)
            ) as gr 
            FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min_last_partition`
        )").columns[0].values()[0] %}
    WHERE
        ano_particao >= SAFE_CAST(EXTRACT(YEAR FROM DATE(("{{ max_partition }}"))) AS STRING) AND
        mes_particao >= SAFE_CAST(EXTRACT(MONTH FROM DATE(("{{ max_partition }}"))) AS STRING) AND
        data_particao >= SAFE_CAST(DATE_TRUNC(DATE(("{{ max_partition }}")), day) AS STRING)
    
    {% endif %} 
    ),

    remove_duplicated as (
    SELECT 
        id_estacao,
        data_medicao,
        MIN(SAFE_CAST(acumulado_chuva_5min AS FLOAT64)) acumulado_chuva_5min,
        MIN(SAFE_CAST(acumulado_chuva_10min AS FLOAT64)) acumulado_chuva_10min,
        MIN(SAFE_CAST(acumulado_chuva_15min AS FLOAT64)) acumulado_chuva_15min,
        MIN(SAFE_CAST(acumulado_chuva_30min AS FLOAT64)) acumulado_chuva_30min,
        MIN(SAFE_CAST(acumulado_chuva_1h AS FLOAT64)) acumulado_chuva_1h,
        MIN(SAFE_CAST(acumulado_chuva_2h AS FLOAT64)) acumulado_chuva_2h,
        MIN(SAFE_CAST(acumulado_chuva_3h AS FLOAT64)) acumulado_chuva_3h,
        MIN(SAFE_CAST(acumulado_chuva_4h AS FLOAT64)) acumulado_chuva_4h,
        MIN(SAFE_CAST(acumulado_chuva_6h AS FLOAT64)) acumulado_chuva_6h,
        MIN(SAFE_CAST(acumulado_chuva_12h AS FLOAT64)) acumulado_chuva_12h,
        MIN(SAFE_CAST(acumulado_chuva_24h AS FLOAT64)) acumulado_chuva_24h,
        MIN(SAFE_CAST(acumulado_chuva_96h AS FLOAT64)) acumulado_chuva_96h,
        MIN(SAFE_CAST(acumulado_chuva_mes AS FLOAT64)) acumulado_chuva_mes
    FROM remove_extreme_values
    GROUP BY
        id_estacao,
        data_medicao
)

SELECT 
    DISTINCT
    id_estacao,
    SAFE_CAST(
        SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', data_medicao) AS DATETIME
    ) AS data_medicao,
    acumulado_chuva_5min,
    acumulado_chuva_10min,
    acumulado_chuva_15min,
    acumulado_chuva_30min,
    acumulado_chuva_1h,
    acumulado_chuva_2h,
    acumulado_chuva_3h,
    acumulado_chuva_4h,
    acumulado_chuva_6h,
    acumulado_chuva_12h,
    acumulado_chuva_24h,
    acumulado_chuva_96h,
    acumulado_chuva_mes,
    SAFE_CAST(DATE_TRUNC(DATE(data_medicao), day) AS DATE) data_particao,
    CONCAT(id_estacao, '_', data_medicao) AS primary_key,
FROM 
    remove_duplicated