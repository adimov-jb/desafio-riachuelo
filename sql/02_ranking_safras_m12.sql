-- =============================================================================
-- Tarefa 2 - Ranking das safras dentro do ano pelo % de inadimplência no M12
-- e desvio (p.p.) em relação à média do ano. SQLite / DBeaver.
-- Mesmas regras da Tarefa 1 (ver 01_safras_inadimplencia_marcos.sql).
-- Rank 1 = safra com MAIOR inadimplência no M12 (pior safra do ano).
-- =============================================================================
with data_referencia as (
    select max(data_pagamento) as data_referencia from parcela
),

contratos as (
    select
        id_contrato,
        date(data_contrato) as data_contrato,
        cast(strftime('%Y', data_contrato) as integer) as ano_safra,
        strftime('%Y', data_contrato) || '-T'
            || ((cast(strftime('%m', data_contrato) as integer) + 2) / 3) as safra
    from contrato
    where status <> 'cancelado'
),

primeira_inadimplencia as (
    select id_contrato, data_vencimento
    from (
        select
            id_contrato,
            date(data_vencimento) as data_vencimento,
            row_number() over (
                partition by id_contrato
                order by data_vencimento, num_parcela
            ) as ordem
        from parcela
        where status in ('atrasada', 'baixada')
    )
    where ordem = 1
),

safras_m12 as (
    select
        c.ano_safra,
        c.safra,
        count(*) as qtd_contratos,
        sum(
            cast((julianday(p.data_vencimento) - julianday(c.data_contrato) + 29) / 30 as integer) <= 12
        ) as qtd_inadimplentes_m12,
        max(c.data_contrato) as data_ultimo_contrato
    from contratos c
    left join primeira_inadimplencia p on p.id_contrato = c.id_contrato
    group by c.ano_safra, c.safra
),

pct_m12 as (
    select
        s.ano_safra,
        s.safra,
        s.qtd_contratos,
        s.qtd_inadimplentes_m12,
        round(100.0 * s.qtd_inadimplentes_m12 / s.qtd_contratos, 2) as pct_inad_m12
    from safras_m12 s
    cross join data_referencia r
    -- só safras com M12 maturado
    where date(s.data_ultimo_contrato, '+360 days') <= r.data_referencia
)

select
    ano_safra,
    safra,
    qtd_contratos,
    qtd_inadimplentes_m12,
    pct_inad_m12,
    rank() over w_ano_ordenado                                           as rank_m12_no_ano,
    round(avg(pct_inad_m12) over w_ano, 2)                               as media_pct_m12_ano,
    round(pct_inad_m12 - avg(pct_inad_m12) over w_ano, 2)                as desvio_pp_media_ano,
    round(100.0 * sum(qtd_inadimplentes_m12) over w_ano
                / sum(qtd_contratos) over w_ano, 2)                      as pct_m12_ano_ponderado
from pct_m12
window
    w_ano          as (partition by ano_safra),
    w_ano_ordenado as (partition by ano_safra order by pct_inad_m12 desc)
order by ano_safra, rank_m12_no_ano;
