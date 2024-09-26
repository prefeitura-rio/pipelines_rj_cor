{{
    config(
        materialized='incremental',
        unique_key="primary_key",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        post_hook='CREATE OR REPLACE TABLE `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_last_partition` AS (SELECT CURRENT_DATETIME("America/Sao_Paulo") AS data_particao)'
    )
}}

WITH remove_extreme_values as (
    SELECT
        SAFE_CAST(
            REGEXP_REPLACE(id_estacao, r'\.0$', '') AS STRING
        ) id_estacao,
        data_medicao,
        CASE WHEN SAFE_CAST(acumulado_chuva_15_min AS FLOAT64) < 0 THEN NULL ELSE acumulado_chuva_15_min END acumulado_chuva_15_min,
        CASE WHEN SAFE_CAST(acumulado_chuva_1_h AS FLOAT64)    < 0 THEN NULL ELSE acumulado_chuva_1_h END acumulado_chuva_1_h,
        CASE WHEN SAFE_CAST(acumulado_chuva_4_h AS FLOAT64)    < 0 THEN NULL ELSE acumulado_chuva_4_h END acumulado_chuva_4_h,
        CASE WHEN SAFE_CAST(acumulado_chuva_24_h AS FLOAT64)   < 0 THEN NULL ELSE acumulado_chuva_24_h END acumulado_chuva_24_h,
        CASE WHEN SAFE_CAST(acumulado_chuva_96_h AS FLOAT64)   < 0 THEN NULL ELSE acumulado_chuva_96_h END acumulado_chuva_96_h
    FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario`

    {% if is_incremental() %}

    {% set max_partition = run_query(
        "SELECT DATE(gr) FROM (
            SELECT IF(
                max(data_particao) > CURRENT_DATE('America/Sao_Paulo'),
                CURRENT_DATE('America/Sao_Paulo'),
                max(data_particao)
            ) as gr
            FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_last_partition`
        )").columns[0].values()[0] %}
    WHERE
        ano >= EXTRACT(YEAR FROM DATE(("{{ max_partition }}"))) AND
        mes >= EXTRACT(MONTH FROM DATE(("{{ max_partition }}"))) AND
        dia >= EXTRACT(DAY FROM DATE(("{{ max_partition }}")))

    {% endif %}
    ),

    remove_duplicated as (
    SELECT
        id_estacao,
        data_medicao,
        MIN(SAFE_CAST(acumulado_chuva_15_min AS FLOAT64)) acumulado_chuva_15_min,
        MIN(SAFE_CAST(acumulado_chuva_1_h AS FLOAT64)) acumulado_chuva_1_h,
        MIN(SAFE_CAST(acumulado_chuva_4_h AS FLOAT64)) acumulado_chuva_4_h,
        MIN(SAFE_CAST(acumulado_chuva_24_h AS FLOAT64)) acumulado_chuva_24_h,
        MIN(SAFE_CAST(acumulado_chuva_96_h AS FLOAT64)) acumulado_chuva_96_h
    FROM remove_extreme_values
    GROUP BY
        id_estacao,
        data_medicao
)

SELECT
    DISTINCT
    CONCAT(id_estacao, '_', data_medicao) AS primary_key,
    id_estacao,
    acumulado_chuva_15_min,
    acumulado_chuva_1_h,
    acumulado_chuva_4_h,
    acumulado_chuva_24_h,
    acumulado_chuva_96_h,
    SAFE_CAST(
            SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_medicao) AS TIME
        ) AS horario,
    SAFE_CAST(DATE_TRUNC(DATE(data_medicao), day) AS DATE) data_particao
FROM
    remove_duplicated