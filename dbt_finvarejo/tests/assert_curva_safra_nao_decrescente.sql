-- O % acumulado precisa ser não decrescente ao longo dos marcos e estar entre 0 e 100.
select *
from {{ ref('mrt_risco__safras_inadimplencia_marcos') }}
where pct_inad_m03 > pct_inad_m06
   or pct_inad_m06 > pct_inad_m09
   or pct_inad_m09 > pct_inad_m12
   or pct_inad_m03 < 0
   or pct_inad_m12 > 100
