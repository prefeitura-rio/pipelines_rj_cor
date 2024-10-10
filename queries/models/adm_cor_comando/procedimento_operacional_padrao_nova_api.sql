SELECT id_pop, REGEXP_REPLACE(NORMALIZE(pop_titulo, NFD), r'\pM', '') AS pop_titulo
FROM `rj-cor.adm_cor_comando_staging.procedimento_operacional_padrao_nova_api`
ORDER BY CAST(id_pop AS INT)