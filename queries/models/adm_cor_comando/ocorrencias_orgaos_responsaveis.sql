{{
    config(
        materialized='incremental',
        unique_key='primary_key',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}

WITH remove_duplicated AS (
    SELECT
        id_evento,
        sigla,
        SAFE_CAST(data_chegada AS TIMESTAMP) data_chegada,
        SAFE_CAST(data_inicio AS TIMESTAMP) data_inicio,
        SAFE_CAST(data_fim AS TIMESTAMP) data_fim,
        SAFE_CAST(data_particao AS DATE) data_particao,
        CONCAT(id_evento, '_', sigla, '_', descricao) AS primary_key,
        row_number() OVER (PARTITION BY id_evento ORDER BY last_updated_at DESC) AS last_update
    FROM `rj-cor.adm_cor_comando_staging.ocorrencias_orgaos_responsaveis_nova_api`

    {% if is_incremental() %}

        WHERE data_particao <= CAST(CURRENT_DATE('America/Sao_Paulo') AS STRING)
        AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 30 day) AS STRING)

    {% endif %}
)

SELECT * EXCEPT(last_update) FROM remove_duplicated
WHERE last_update = 1
