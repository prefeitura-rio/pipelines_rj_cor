{{
    config(
        materialized='incremental',
        unique_key="primary_key",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        post_hook='CREATE OR REPLACE TABLE `rj-cor.clima_estacao_meteorologica_staging.meteorologia_alertario_last_partition` AS (SELECT CURRENT_DATETIME("America/Sao_Paulo") AS data_particao)'
    )
}}

SELECT
    DISTINCT
    SAFE_CAST(
        REGEXP_REPLACE(id_estacao, r'\.0$', '') AS STRING
        ) id_estacao,
    SAFE_CAST(
        SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', data_medicao) AS DATETIME
    ) AS data_medicao,
    SAFE_CAST(temperatura AS FLOAT64) temperatura,
    SAFE_CAST(pressao_atmosferica AS FLOAT64) pressao,
    SAFE_CAST(temperatura_orvalho AS FLOAT64) temperatura_orvalho,
    SAFE_CAST(SAFE_CAST(umidade_ar AS FLOAT64) AS INT64) umidade,
    SAFE_CAST(SAFE_CAST(direcao_vento AS FLOAT64) AS INT64) direcao_vento,
    SAFE_CAST(velocidade_vento AS FLOAT64) velocidade_vento,
    SAFE_CAST(sensacao_termica AS FLOAT64) sensacao_termica,
    SAFE_CAST(DATE_TRUNC(DATE(data_medicao), day) AS DATE) data_particao,
    CONCAT(id_estacao, '_', data_medicao) AS primary_key,
FROM `rj-cor.clima_estacao_meteorologica_staging.meteorologia_alertario`


{% if is_incremental() %}

{% set max_partition = run_query(
    "SELECT DATE(gr) FROM (
        SELECT IF(
            max(data_particao) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(data_particao)
            ) as gr
        FROM `rj-cor.clima_estacao_meteorologica_staging.meteorologia_alertario_last_partition`
        )
    ").columns[0].values()[0] %}

WHERE
    ano_particao >= SAFE_CAST(EXTRACT(YEAR FROM DATE(("{{ max_partition }}"))) AS STRING) AND
    mes_particao >= SAFE_CAST(EXTRACT(MONTH FROM DATE(("{{ max_partition }}"))) AS STRING) AND
    data_particao >= SAFE_CAST(DATE_TRUNC(DATE(("{{ max_partition }}")), day) AS STRING)

AND
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_medicao) AS DATETIME
    ) > ("{{ max_partition }}")

{% endif %}
