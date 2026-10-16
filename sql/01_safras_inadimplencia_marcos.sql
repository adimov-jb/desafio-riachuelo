-- =============================================================================
-- Tarefa 1 - Curva de inadimplência por safra trimestral (marcos M03/M06/M09/M12)
-- SQLite (executável direto no DBeaver sobre FinVarejo.db)
--
-- Regras:
--   * Safra            = trimestre de data_contrato (AAAA-Tn)
--   * Denominador      = contratos não cancelados da safra (cancelados não têm exposição)
--   * Evento           = 1ª parcela (por vencimento) com status 'atrasada' ou 'baixada'
--   * Mês de vida      = meses comerciais de 30 dias entre assinatura e vencimento
--                        da 1ª parcela inadimplente (parcela N vence em 30*N dias -> MN)
--   * % acumulado Mx   = contratos com evento em mês de vida <= x / contratos da safra
--   * Marcos não maturados (último contrato + x*30 dias > data de referência) = NULL
-- =============================================================================
with recursive meses(mes_vida) as (
    select 1
    union all
    select mes_vida + 1 from meses where mes_vida < 12
),

data_referencia as (
    select max(data_pagamento) as data_referencia from parcela
),

contratos as (
    select
        id_contrato,
        date(data_contrato) as data_contrato,
        strftime('%Y', data_contrato) || '-T'
            || ((cast(strftime('%m', data_contrato) as integer) + 2) / 3) as safra
    from contrato
    where status <> 'cancelado'
),

-- Window function 1: identifica a primeira parcela inadimplente de cada contrato
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

contrato_evento as (
    select
        c.safra,
        c.data_contrato,
        cast((julianday(p.data_vencimento) - julianday(c.data_contrato) + 29) / 30 as integer)
            as mes_vida_inadimplencia
    from contratos c
    left join primeira_inadimplencia p on p.id_contrato = c.id_contrato
),

safras as (
    select safra, count(*) as qtd_contratos, max(data_contrato) as data_ultimo_contrato
    from contrato_evento
    group by safra
),

novos_inadimplentes as (
    select safra, mes_vida_inadimplencia as mes_vida, count(*) as qtd_novos
    from contrato_evento
    where mes_vida_inadimplencia <= 12
    group by safra, mes_vida_inadimplencia
),

-- Window function 2: soma acumulada dos novos inadimplentes ao longo do mês de vida
curva as (
    select
        s.safra,
        s.qtd_contratos,
        m.mes_vida,
        sum(coalesce(n.qtd_novos, 0)) over (
            partition by s.safra
            order by m.mes_vida
            rows between unbounded preceding and current row
        ) as qtd_acumulado,
        date(s.data_ultimo_contrato, '+' || (m.mes_vida * 30) || ' days') <= r.data_referencia
            as fl_maturado
    from safras s
    cross join meses m
    cross join data_referencia r
    left join novos_inadimplentes n
        on n.safra = s.safra and n.mes_vida = m.mes_vida
)

-- Pivot: uma linha por safra, marcos como colunas
select
    safra,
    qtd_contratos,
    max(case when mes_vida = 3  and fl_maturado then round(100.0 * qtd_acumulado / qtd_contratos, 2) end) as pct_inad_m03,
    max(case when mes_vida = 6  and fl_maturado then round(100.0 * qtd_acumulado / qtd_contratos, 2) end) as pct_inad_m06,
    max(case when mes_vida = 9  and fl_maturado then round(100.0 * qtd_acumulado / qtd_contratos, 2) end) as pct_inad_m09,
    max(case when mes_vida = 12 and fl_maturado then round(100.0 * qtd_acumulado / qtd_contratos, 2) end) as pct_inad_m12
from curva
group by safra, qtd_contratos
order by safra;
