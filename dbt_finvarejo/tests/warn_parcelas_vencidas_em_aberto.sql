{{ config(severity='warn') }}

-- Qualidade de dados: parcelas vencidas há mais de 30 dias, sem pagamento, ainda
-- marcadas como "em_aberto". Pela regra atual elas não contam como inadimplência,
-- o que pode subestimar a curva. Alerta para alinhamento com o sistema de origem.
select p.*
from {{ ref('stg_finvarejo__parcela') }} as p
cross join (
    select max(data_pagamento) as data_referencia
    from {{ ref('stg_finvarejo__parcela') }}
) as r
where p.status_parcela = 'em_aberto'
  and p.data_pagamento is null
  and julianday(r.data_referencia) - julianday(p.data_vencimento) > 30
