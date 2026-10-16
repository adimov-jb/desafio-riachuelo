{# 1 linha por cliente que recebeu ao menos um disparo de campanha. #}

with leads as (

    select * from {{ ref('stg_finvarejo__lead_campanha') }}

),

referencia as (

    select {{ data_referencia() }} as data_referencia

)

select
    l.id_cliente,
    count(*) as qtd_disparos,
    sum(case
            when l.status_resposta in ('clicou', 'contratou')
             and l.data_disparo >  date(r.data_referencia, '-{{ var("janela_engajamento_meses") }} months')
             and l.data_disparo <= r.data_referencia
            then 1 else 0
        end) as qtd_cliques_conversoes_janela,
    max(case
            when l.status_resposta in ('clicou', 'contratou') then l.data_disparo
        end) as data_ultimo_clique_conversao,
    max(l.data_disparo) as data_ultimo_disparo
from leads as l
cross join referencia as r
group by l.id_cliente
