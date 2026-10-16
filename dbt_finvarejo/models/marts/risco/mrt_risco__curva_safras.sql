{#
  Curva de inadimplência acumulada por safra (formato longo): safra x mês de vida 1..N.
  Base para gráficos de vintage no BI e para o pivot dos marcos.
  Marcos ainda não maturados (último contrato da safra + N*30 dias > data de referência)
  ficam nulos para não subestimar safras recentes.
#}

with recursive meses(mes_vida) as (

    select 1
    union all
    select mes_vida + 1 from meses where mes_vida < {{ var('horizonte_meses') }}

),

contratos as (

    select * from {{ ref('int_contrato__primeira_inadimplencia') }}

),

data_referencia as (

    select {{ data_referencia() }} as data_referencia

),

safras as (

    select
        safra,
        ano_safra,
        trimestre_safra,
        count(*)            as qtd_contratos,
        max(data_contrato)  as data_ultimo_contrato
    from contratos
    group by safra, ano_safra, trimestre_safra

),

novos_inadimplentes as (

    select
        safra,
        mes_vida_inadimplencia  as mes_vida,
        count(*)                as qtd_novos_inadimplentes
    from contratos
    where mes_vida_inadimplencia <= {{ var('horizonte_meses') }}
    group by safra, mes_vida_inadimplencia

),

curva as (

    select
        s.safra,
        s.ano_safra,
        s.trimestre_safra,
        m.mes_vida,
        s.qtd_contratos,
        coalesce(n.qtd_novos_inadimplentes, 0) as qtd_novos_inadimplentes,
        sum(coalesce(n.qtd_novos_inadimplentes, 0)) over (
            partition by s.safra
            order by m.mes_vida
            rows between unbounded preceding and current row
        ) as qtd_inadimplentes_acumulado,
        date(s.data_ultimo_contrato, '+' || (m.mes_vida * 30) || ' days') <= r.data_referencia
            as fl_maturado
    from safras as s
    cross join meses as m
    cross join data_referencia as r
    left join novos_inadimplentes as n
        on  n.safra = s.safra
        and n.mes_vida = m.mes_vida

)

select
    safra,
    ano_safra,
    trimestre_safra,
    mes_vida,
    'M' || printf('%02d', mes_vida) as marco,
    qtd_contratos,
    qtd_novos_inadimplentes,
    qtd_inadimplentes_acumulado,
    case
        when fl_maturado then round(100.0 * qtd_inadimplentes_acumulado / qtd_contratos, 2)
    end as pct_inadimplencia_acumulada,
    fl_maturado
from curva
