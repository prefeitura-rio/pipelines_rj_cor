{{
    config(
        materialized='incremental',
        unique_key="primary_key",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month", 
        },
        post_hook='CREATE OR REPLACE TABLE `rj-cor.clima_fluviometro_staging.lamina_agua_inea_last_partition` AS (SELECT CURRENT_DATETIME("America/Sao_Paulo") AS data_particao)'
    )
}}

SELECT 
    DISTINCT
    CONCAT(id_estacao, '_', data_medicao) AS primary_key,
    SAFE_CAST(id_estacao AS STRING) AS id_estacao,
    SAFE_CAST(
            SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', data_medicao) AS DATETIME
        ) AS data_medicao,
    SAFE_CAST(altura_agua AS FLOAT64) altura_agua,
    SAFE_CAST(DATE_TRUNC(DATE(data_medicao), day) AS DATE) data_particao
FROM `rj-cor.clima_fluviometro_staging.lamina_agua_inea`

{% if is_incremental() %}

    {% set max_partition = run_query(
        "SELECT DATE(gr) FROM (
            SELECT IF(
                max(data_particao) > CURRENT_DATE('America/Sao_Paulo'), 
                CURRENT_DATE('America/Sao_Paulo'), 
                max(data_particao)
            ) as gr 
            FROM `rj-cor.clima_fluviometro_staging.lamina_agua_inea_last_partition`
        )").columns[0].values()[0] %}
    WHERE
        ano_particao >= SAFE_CAST(EXTRACT(YEAR FROM DATE(("{{ max_partition }}"))) AS STRING) AND
        mes_particao >= SAFE_CAST(EXTRACT(MONTH FROM DATE(("{{ max_partition }}"))) AS STRING) AND
        data_particao >= SAFE_CAST(DATE_TRUNC(DATE(("{{ max_partition }}")), day) AS STRING)

    {% endif %} 