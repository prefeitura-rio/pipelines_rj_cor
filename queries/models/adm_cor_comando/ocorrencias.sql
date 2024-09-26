{{
    config(
        materialized='incremental',
        unique_key="id_evento",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }    
    )
}}

WITH remove_duplicated AS (
    SELECT
        DISTINCT
        id_evento,
        pop,
        bairro,
        SAFE_CAST(data_inicio AS TIMESTAMP) data_inicio,
        SAFE_CAST(data_fim AS TIMESTAMP) data_fim,
        prazo,
        descricao,
        gravidade,
        SAFE_CAST(latitude AS FLOAT64) latitude,
        SAFE_CAST(longitude AS FLOAT64) longitude,
        status,
        tipo,
        SAFE_CAST(data_particao  AS DATE) as data_particao,
        row_number() OVER (PARTITION BY id_evento ORDER BY last_updated_at DESC) AS last_update
    FROM `rj-cor.adm_cor_comando_staging.ocorrencias`

    {% if is_incremental() %}

        WHERE CAST(data_particao AS DATE) <= CURRENT_DATE('America/Sao_Paulo')
            AND CAST(data_particao AS DATE) >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 30 day)

    {% endif %}

)

SELECT * EXCEPT(last_update) FROM remove_duplicated
WHERE last_update = 1