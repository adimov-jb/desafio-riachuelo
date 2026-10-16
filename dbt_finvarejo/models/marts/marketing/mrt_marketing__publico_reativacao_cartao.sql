{#
  Item 1: público da campanha de reativação de cartão.
  Contém dados pessoais (nome, e-mail): finalidade, base legal e retenção em
  _mrt_marketing.yml. Só pode ser entregue após o filtro de opt-out/consentimento.
#}

{% set cfg = var('reativacao_cartao') %}

with clientes as (

    select * from {{ ref('stg_finvarejo__cliente') }}

),

indicadores as (

    select * from {{ ref('int_cliente__indicadores_credito') }}

),

engajamento as (

    select * from {{ ref('int_cliente__engajamento_campanha') }}

),

contratos_cartao as (

    select
        c.id_cliente,
        count(*)                as qtd_contratos_cartao,
        avg(c.valor_contratado) as valor_medio_contratado
    from {{ ref('stg_finvarejo__contrato') }} as c
    join {{ ref('stg_finvarejo__produto_financeiro') }} as p
        on p.id_produto = c.id_produto
    where p.tipo_produto = 'cartao'
      and c.status_contrato <> 'cancelado'
      and c.data_contrato between '{{ cfg.data_inicio_originacao }}' and '{{ cfg.data_fim_originacao }}'
    group by c.id_cliente

)

select
    cl.id_cliente,
    cl.nome,
    cl.email,
    cl.estado,
    cl.canal_aquisicao,
    cc.qtd_contratos_cartao,
    round(cc.valor_medio_contratado, 2) as valor_medio_contratado,
    i.data_referencia
from clientes as cl
join contratos_cartao as cc
    on cc.id_cliente = cl.id_cliente
join indicadores as i
    on i.id_cliente = cl.id_cliente
left join engajamento as e
    on e.id_cliente = cl.id_cliente
where cl.dominio_email in ('{{ cfg.dominios_email | join("', '") }}')
  and cc.qtd_contratos_cartao between {{ cfg.min_contratos_cartao }} and {{ cfg.max_contratos_cartao }}
  and i.fl_inadimplente = 0
  and coalesce(e.qtd_cliques_conversoes_janela, 0) = 0
