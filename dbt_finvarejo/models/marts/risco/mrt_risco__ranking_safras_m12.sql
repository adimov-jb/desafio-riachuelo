{#
  Tarefa 2: ranking das safras dentro do ano pelo % de inadimplência no M12
  (1 = pior safra) e desvio em p.p. em relação à média simples das safras do ano.
#}

with marcos as (

    select * from {{ ref('mrt_risco__safras_inadimplencia_marcos') }}
    where pct_inad_m12 is not null

)

select
    ano_safra,
    safra,
    trimestre_safra,
    qtd_contratos,
    qtd_inadimplentes_m12,
    pct_inad_m12,
    rank() over (
        partition by ano_safra
        order by pct_inad_m12 desc
    )                                                                       as rank_m12_no_ano,
    round(avg(pct_inad_m12) over (partition by ano_safra), 2)               as media_pct_m12_ano,
    round(pct_inad_m12 - avg(pct_inad_m12) over (partition by ano_safra), 2) as desvio_pp_media_ano,
    -- referência ponderada pelo volume (taxa da carteira do ano), útil para PDD
    round(
        100.0 * sum(qtd_inadimplentes_m12) over (partition by ano_safra)
              / sum(qtd_contratos)         over (partition by ano_safra), 2
    )                                                                       as pct_m12_ano_ponderado
from marcos
