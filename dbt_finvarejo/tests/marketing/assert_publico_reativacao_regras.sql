-- Revalida as regras do público direto na fonte (independente dos intermediates).
-- Qualquer linha retornada é um cliente que não deveria estar na lista.
{% set cfg = var('reativacao_cartao') %}

select m.id_cliente, 'dominio_invalido' as violacao
from {{ ref('mrt_marketing__publico_reativacao_cartao') }} m
where substr(m.email, instr(m.email, '@') + 1) not in ('{{ cfg.dominios_email | join("', '") }}')

union all

select m.id_cliente, 'qtd_cartao_fora_da_faixa'
from {{ ref('mrt_marketing__publico_reativacao_cartao') }} m
where m.qtd_contratos_cartao not between {{ cfg.min_contratos_cartao }} and {{ cfg.max_contratos_cartao }}

union all

select distinct m.id_cliente, 'contrato_inadimplente'
from {{ ref('mrt_marketing__publico_reativacao_cartao') }} m
join {{ source('finvarejo', 'contrato') }} c on c.id_cliente = m.id_cliente
where c.status = 'inadimplente'

union all

select distinct m.id_cliente, 'clique_ou_conversao_recente'
from {{ ref('mrt_marketing__publico_reativacao_cartao') }} m
join {{ source('finvarejo', 'lead_campanha') }} l on l.id_cliente = m.id_cliente
where l.status_resposta in ('clicou', 'contratou')
  and date(l.data_disparo) > date(m.data_referencia, '-{{ var("janela_engajamento_meses") }} months')
