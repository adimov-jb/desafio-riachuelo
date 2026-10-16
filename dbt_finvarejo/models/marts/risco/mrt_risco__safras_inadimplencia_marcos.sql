{# Tarefa 1: uma linha por safra, marcos M03/M06/M09/M12 como colunas (% acumulado). #}

with curva as (

    select * from {{ ref('mrt_risco__curva_safras') }}

)

select
    safra,
    ano_safra,
    trimestre_safra,
    qtd_contratos,
    max(case when mes_vida = 3  then pct_inadimplencia_acumulada end) as pct_inad_m03,
    max(case when mes_vida = 6  then pct_inadimplencia_acumulada end) as pct_inad_m06,
    max(case when mes_vida = 9  then pct_inadimplencia_acumulada end) as pct_inad_m09,
    max(case when mes_vida = 12 then pct_inadimplencia_acumulada end) as pct_inad_m12,
    max(case when mes_vida = 12 then qtd_inadimplentes_acumulado end) as qtd_inadimplentes_m12
from curva
group by safra, ano_safra, trimestre_safra
