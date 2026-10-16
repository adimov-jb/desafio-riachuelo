{#
  1 linha por cliente com os indicadores de crédito reutilizados por Marketing
  (exclusão de inadimplentes) e Risco (alto risco / features de ML).
  Sem dados pessoais de identificação: o join com nome/e-mail é feito nos marts.
#}

{% set regra = var('regra_inadimplencia_cliente') %}

with clientes as (

    select id_cliente, renda_declarada from {{ ref('stg_finvarejo__cliente') }}

),

contratos as (

    select * from {{ ref('stg_finvarejo__contrato') }}

),

parcelas as (

    select * from {{ ref('stg_finvarejo__parcela') }}

),

referencia as (

    select {{ data_referencia() }} as data_referencia

),

-- SQLite não tem MEDIAN: média dos valores centrais via ROW_NUMBER/COUNT em window
mediana_renda as (

    select avg(renda_declarada) as mediana_renda_base
    from (
        select
            renda_declarada,
            row_number() over (order by renda_declarada) as posicao,
            count(*)     over ()                         as total
        from clientes
        where renda_declarada is not null
    )
    where posicao in ((total + 1) / 2, (total + 2) / 2)

),

indicadores_contrato as (

    select
        c.id_cliente,
        count(distinct case when c.status_contrato = 'ativo' then c.id_contrato end)
            as qtd_contratos_ativos,
        max(case
                when c.status_contrato = 'inadimplente'
                {%- if regra == 'over90' %}
                  or p.status_parcela = 'baixada'
                  or p.dias_atraso > 90
                {%- elif regra == 'qualquer_atraso' %}
                  or p.status_parcela in ('atrasada', 'baixada')
                {%- elif regra != 'status' %}
                  {{ exceptions.raise_compiler_error("regra_inadimplencia_cliente inválida: " ~ regra) }}
                {%- endif %}
                then 1 else 0
            end) as fl_inadimplente,
        max(case
                when p.data_vencimento >  date(r.data_referencia, '-{{ var("janela_atraso_recente_dias") }} days')
                 and p.data_vencimento <= r.data_referencia
                then p.dias_atraso
            end) as max_dias_atraso_recente,
        -- saldo a receber: parcelas atrasadas ou em aberto ('baixada' já é perda reconhecida)
        coalesce(sum(case
                when p.status_parcela in ('atrasada', 'em_aberto')
                then p.valor_parcela - coalesce(p.valor_pago, 0)
            end), 0) as total_em_aberto
    from contratos as c
    left join parcelas as p
        on p.id_contrato = c.id_contrato
    cross join referencia as r
    group by c.id_cliente

)

select
    cl.id_cliente,
    coalesce(ic.qtd_contratos_ativos, 0)                as qtd_contratos_ativos,
    coalesce(ic.fl_inadimplente, 0)                     as fl_inadimplente,
    coalesce(ic.max_dias_atraso_recente, 0)             as max_dias_atraso_recente,
    coalesce(ic.total_em_aberto, 0)                     as total_em_aberto,
    m.mediana_renda_base,
    cl.renda_declarada < m.mediana_renda_base           as fl_renda_abaixo_mediana,
    r.data_referencia
from clientes as cl
cross join mediana_renda as m
cross join referencia as r
left join indicadores_contrato as ic
    on ic.id_cliente = cl.id_cliente
