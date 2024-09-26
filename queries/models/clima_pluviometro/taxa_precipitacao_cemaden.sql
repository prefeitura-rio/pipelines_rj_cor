{{
    config(
        materialized='incremental',
        unique_key="primary_key",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month", 
        },
        post_hook='CREATE OR REPLACE TABLE `rj-cor.clima_pluviometro_staging.taxa_precipitacao_cemaden_last_partition` AS (SELECT CURRENT_DATETIME("America/Sao_Paulo") AS data_particao)'
    )
}}

WITH remove_extreme_values as (
    SELECT 
        SAFE_CAST(id_estacao AS STRING) id_estacao,
        data_medicao,
        CASE WHEN SAFE_CAST(acumulado_chuva_10_min AS FLOAT64) < 0 THEN NULL ELSE acumulado_chuva_10_min END acumulado_chuva_10_min,
        CASE WHEN SAFE_CAST(acumulado_chuva_1_h AS FLOAT64)  < 0 THEN NULL ELSE acumulado_chuva_1_h END acumulado_chuva_1_h,
        CASE WHEN SAFE_CAST(acumulado_chuva_3_h AS FLOAT64)  < 0 THEN NULL ELSE acumulado_chuva_3_h END acumulado_chuva_3_h,
        CASE WHEN SAFE_CAST(acumulado_chuva_6_h AS FLOAT64)  < 0 THEN NULL ELSE acumulado_chuva_6_h END acumulado_chuva_6_h,
        CASE WHEN SAFE_CAST(acumulado_chuva_12_h AS FLOAT64) < 0 THEN NULL ELSE acumulado_chuva_12_h END acumulado_chuva_12_h,
        CASE WHEN SAFE_CAST(acumulado_chuva_24_h AS FLOAT64) < 0 THEN NULL ELSE acumulado_chuva_24_h END acumulado_chuva_24_h,
        CASE WHEN SAFE_CAST(acumulado_chuva_48_h AS FLOAT64) < 0 THEN NULL ELSE acumulado_chuva_48_h END acumulado_chuva_48_h,
        CASE WHEN SAFE_CAST(acumulado_chuva_72_h AS FLOAT64) < 0 THEN NULL ELSE acumulado_chuva_72_h END acumulado_chuva_72_h,
        CASE WHEN SAFE_CAST(acumulado_chuva_96_h AS FLOAT64) < 0 THEN NULL ELSE acumulado_chuva_96_h END acumulado_chuva_96_h
    FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_cemaden`
    
    {% if is_incremental() %}

    {% set max_partition = run_query(
        "SELECT DATE(gr) FROM (
            SELECT IF(
                max(data_particao) > CURRENT_DATE('America/Sao_Paulo'), 
                CURRENT_DATE('America/Sao_Paulo'), 
                max(data_particao)
            ) as gr 
            FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_cemaden_last_partition`
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
        MIN(SAFE_CAST(acumulado_chuva_10_min AS FLOAT64)) acumulado_chuva_10_min,
        MIN(SAFE_CAST(acumulado_chuva_1_h AS FLOAT64)) acumulado_chuva_1_h,
        MIN(SAFE_CAST(acumulado_chuva_3_h AS FLOAT64)) acumulado_chuva_3_h,
        MIN(SAFE_CAST(acumulado_chuva_6_h AS FLOAT64)) acumulado_chuva_6_h,
        MIN(SAFE_CAST(acumulado_chuva_12_h AS FLOAT64)) acumulado_chuva_12_h,
        MIN(SAFE_CAST(acumulado_chuva_24_h AS FLOAT64)) acumulado_chuva_24_h,
        MIN(SAFE_CAST(acumulado_chuva_48_h AS FLOAT64)) acumulado_chuva_48_h,
        MIN(SAFE_CAST(acumulado_chuva_72_h AS FLOAT64)) acumulado_chuva_72_h,
        MIN(SAFE_CAST(acumulado_chuva_96_h AS FLOAT64)) acumulado_chuva_96_h
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
    acumulado_chuva_10_min,
    acumulado_chuva_1_h,
    acumulado_chuva_3_h,
    acumulado_chuva_6_h,
    acumulado_chuva_12_h,
    acumulado_chuva_24_h,
    acumulado_chuva_48_h,
    acumulado_chuva_72_h,
    acumulado_chuva_96_h,
    SAFE_CAST(DATE_TRUNC(DATE(data_medicao), day) AS DATE) data_particao,
    CONCAT(id_estacao, '_', data_medicao) AS primary_key
FROM 
    remove_duplicated