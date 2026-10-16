{{ config(severity='warn') }}

-- Atualidade das fontes frente a hoje (substitui o freshness nativo, que o dbt-sqlite
-- não suporta). Em produção esse teste vira severity error e bloqueia a publicação.
-- Limites: parcela até 2 dias sem movimento; lead_campanha até 7 dias.
select 'parcela' as fonte, max(data_pagamento) as ultimo_registro,
       cast(julianday('now') - julianday(max(data_pagamento)) as integer) as dias_sem_carga
from {{ source('finvarejo', 'parcela') }}
having julianday('now') - julianday(max(data_pagamento)) > 2

union all

select 'lead_campanha', max(data_disparo),
       cast(julianday('now') - julianday(max(data_disparo)) as integer)
from {{ source('finvarejo', 'lead_campanha') }}
having julianday('now') - julianday(max(data_disparo)) > 7
