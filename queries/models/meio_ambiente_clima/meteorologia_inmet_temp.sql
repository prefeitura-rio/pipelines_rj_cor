{{
    config(
        materialized='incremental',
        unique_key="primary_key",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
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
FROM `rj-cor.meio_ambiente_clima_staging.meteorologia_inmet_temp`


{% if is_incremental() %}


WHERE
    ano >= EXTRACT(YEAR FROM SAFE_CAST('{{ var("actual_run") }}' AS DATE)) AND
    mes >= EXTRACT(MONTH FROM SAFE_CAST('{{ var("actual_run") }}' AS DATE)) AND
    dia >= EXTRACT(DAY FROM SAFE_CAST('{{ var("actual_run") }}' AS DATE))

{% endif %}
