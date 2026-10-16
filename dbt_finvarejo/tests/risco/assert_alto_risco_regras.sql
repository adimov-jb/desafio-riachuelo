-- Revalida os critérios de alto risco direto na fonte e checa a sanidade do saldo.
-- Qualquer linha retornada é um cliente que não deveria estar na lista.
{% set cfg = var('alto_risco') %}

with lista as (

    select * from {{ ref('mrt_risco__clientes_alto_risco') }}

),

mediana as (

    select avg(renda_declarada) as mediana
    from (
        select renda_declarada,
               row_number() over (order by renda_declarada) as pos,
               count(*) over () as n
        from {{ source('finvarejo', 'cliente') }}
    )
    where pos in ((n + 1) / 2, (n + 2) / 2)

)

select l.id_cliente, 'menos_de_n_contratos_ativos' as violacao
from lista l
where (select count(*) from {{ source('finvarejo', 'contrato') }} c
       where c.id_cliente = l.id_cliente and c.status = 'ativo') < {{ cfg.min_contratos_ativos }}

union all

select l.id_cliente, 'sem_atraso_recente'
from lista l
where not exists (
    select 1
    from {{ source('finvarejo', 'parcela') }} p
    join {{ source('finvarejo', 'contrato') }} c on c.id_contrato = p.id_contrato
    where c.id_cliente = l.id_cliente
      and p.dias_atraso > {{ cfg.dias_atraso_minimo }}
      and date(p.data_vencimento) >  date(l.data_referencia, '-{{ var("janela_atraso_recente_dias") }} days')
      and date(p.data_vencimento) <= l.data_referencia
)

union all

select l.id_cliente, 'renda_nao_abaixo_da_mediana'
from lista l
join {{ source('finvarejo', 'cliente') }} cl on cl.id_cliente = l.id_cliente
cross join mediana m
where cl.renda_declarada >= m.mediana

union all

select l.id_cliente, 'total_em_aberto_negativo'
from lista l
where l.total_em_aberto < 0
