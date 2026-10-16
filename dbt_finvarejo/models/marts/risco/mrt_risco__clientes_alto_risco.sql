{#
  Item 2: clientes de alto risco (insumo do modelo de ML de Risco).
  (a) 2+ contratos ativos, (b) parcela com atraso > 30 dias vencida nos últimos
  90 dias e (c) renda abaixo da mediana da base.
#}

{% set cfg = var('alto_risco') %}

with clientes as (

    select * from {{ ref('stg_finvarejo__cliente') }}

),

indicadores as (

    select * from {{ ref('int_cliente__indicadores_credito') }}

)

select
    cl.id_cliente,
    cl.nome,
    cl.score_interno,
    i.qtd_contratos_ativos,
    round(i.total_em_aberto, 2) as total_em_aberto,
    i.data_referencia
from clientes as cl
join indicadores as i
    on i.id_cliente = cl.id_cliente
where i.qtd_contratos_ativos >= {{ cfg.min_contratos_ativos }}
  and i.max_dias_atraso_recente > {{ cfg.dias_atraso_minimo }}
  and i.fl_renda_abaixo_mediana
