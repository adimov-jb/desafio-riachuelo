{{ config(severity='warn') }}

-- Atualidade: se não há nenhum disparo de campanha na janela de engajamento, o filtro
-- "sem clique/conversão nos últimos N meses" não exclui ninguém. Pode ser ausência
-- real de campanhas ou carga de lead_campanha parada: confirmar com Marketing.
select max(data_disparo) as data_ultimo_disparo, {{ data_referencia() }} as data_referencia
from {{ ref('stg_finvarejo__lead_campanha') }}
having max(data_disparo) <= date({{ data_referencia() }}, '-{{ var("janela_engajamento_meses") }} months')
