-- =============================================================================
-- Item 2 - Clientes de alto risco (insumo para modelo de ML de Risco)
-- SQLite (executável direto no DBeaver sobre FinVarejo.db)
--
-- Critérios (simultâneos):
--   (a) 2+ contratos com status 'ativo'
--   (b) 1+ parcela com dias_atraso > 30 e vencimento nos 90 dias anteriores à
--       data de referência (qualquer contrato do cliente)
--   (c) renda_declarada estritamente abaixo da mediana da base de clientes
--       (SQLite não tem MEDIAN: calculada com ROW_NUMBER/COUNT em window)
-- Total em aberto = soma de (valor_parcela - valor_pago) nas parcelas 'atrasada'
--   ou 'em_aberto' de todos os contratos do cliente. 'baixada' fica fora: já é
--   perda reconhecida, não saldo a receber.
-- =============================================================================
with data_referencia as (
    select max(date(data_pagamento)) as data_referencia from parcela
),

mediana_renda as (
    select avg(renda_declarada) as mediana_renda
    from (
        select
            renda_declarada,
            row_number() over (order by renda_declarada) as posicao,
            count(*)     over ()                         as total
        from cliente
        where renda_declarada is not null
    )
    where posicao in ((total + 1) / 2, (total + 2) / 2)
),

contratos_ativos as (
    select id_cliente, count(*) as qtd_contratos_ativos
    from contrato
    where status = 'ativo'
    group by id_cliente
),

atraso_recente as (
    select distinct c.id_cliente
    from parcela p
    join contrato c on c.id_contrato = p.id_contrato
    cross join data_referencia r
    where p.dias_atraso > 30
      and date(p.data_vencimento) >  date(r.data_referencia, '-90 days')
      and date(p.data_vencimento) <= r.data_referencia
),

saldo_em_aberto as (
    select
        c.id_cliente,
        sum(p.valor_parcela - coalesce(p.valor_pago, 0)) as total_em_aberto
    from parcela p
    join contrato c on c.id_contrato = p.id_contrato
    where p.status in ('atrasada', 'em_aberto')
    group by c.id_cliente
)

select
    cl.nome,
    cl.score_interno,
    ca.qtd_contratos_ativos,
    round(coalesce(s.total_em_aberto, 0), 2) as total_em_aberto
from cliente cl
join contratos_ativos ca on ca.id_cliente = cl.id_cliente
join atraso_recente   ar on ar.id_cliente = cl.id_cliente
cross join mediana_renda m
left join saldo_em_aberto s on s.id_cliente = cl.id_cliente
where ca.qtd_contratos_ativos >= 2
  and cl.renda_declarada < m.mediana_renda
order by total_em_aberto desc;
