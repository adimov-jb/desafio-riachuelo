-- =============================================================================
-- Item 1 - Público da campanha de reativação de cartão (Marketing)
-- SQLite (executável direto no DBeaver sobre FinVarejo.db)
--
-- Regras e decisões:
--   * Data de referência  = última data de pagamento observada na base (corte do
--                           snapshot). Em produção: current_date.
--   * E-mail              = domínio exatamente gmail.com ou yahoo.com.br
--                           (comparação do domínio inteiro, não LIKE solto:
--                           evita "gmail.com.br", "notgmail.com" etc.)
--   * Contratos de cartão = produto tipo 'cartao', data_contrato em 2021-2023,
--                           excluindo cancelados (não houve originação efetiva)
--   * Inadimplente        = contrato com status 'inadimplente' OU parcela baixada
--                           OU parcela com mais de 90 dias de atraso (default
--                           regulatório). O status do contrato sozinho não basta:
--                           há contratos 'ativo' com parcelas baixadas.
--   * Engajamento         = resposta 'clicou' ou 'contratou' em disparo nos 6 meses
--                           anteriores à data de referência (qualquer campanha)
-- =============================================================================
with data_referencia as (
    select max(date(data_pagamento)) as data_referencia from parcela
),

contratos_cartao as (
    select
        c.id_cliente,
        count(*)                as qtd_contratos_cartao,
        avg(c.valor_contratado) as valor_medio_contratado
    from contrato c
    join produto_financeiro p on p.id_produto = c.id_produto
    where p.tipo = 'cartao'
      and c.status <> 'cancelado'
      and date(c.data_contrato) between '2021-01-01' and '2023-12-31'
    group by c.id_cliente
),

clientes_inadimplentes as (
    select distinct c.id_cliente
    from contrato c
    where c.status = 'inadimplente'
       or exists (
            select 1
            from parcela p
            where p.id_contrato = c.id_contrato
              and (p.status = 'baixada' or p.dias_atraso > 90)
       )
),

clientes_engajados as (
    select distinct l.id_cliente
    from lead_campanha l
    cross join data_referencia r
    where l.status_resposta in ('clicou', 'contratou')
      and date(l.data_disparo) >  date(r.data_referencia, '-6 months')
      and date(l.data_disparo) <= r.data_referencia
)

select
    cl.nome,
    cl.email,
    cl.estado,
    cl.canal_aquisicao,
    cc.qtd_contratos_cartao,
    round(cc.valor_medio_contratado, 2) as valor_medio_contratado
from cliente cl
join contratos_cartao cc on cc.id_cliente = cl.id_cliente
where substr(lower(trim(cl.email)), instr(lower(trim(cl.email)), '@') + 1)
          in ('gmail.com', 'yahoo.com.br')
  and cc.qtd_contratos_cartao between 2 and 3
  and not exists (select 1 from clientes_inadimplentes i where i.id_cliente = cl.id_cliente)
  and not exists (select 1 from clientes_engajados e where e.id_cliente = cl.id_cliente)
order by cc.qtd_contratos_cartao desc, cc.valor_medio_contratado desc;
