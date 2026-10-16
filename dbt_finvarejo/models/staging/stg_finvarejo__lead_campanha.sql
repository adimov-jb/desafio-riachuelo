with source as (

    select * from {{ source('finvarejo', 'lead_campanha') }}

)

select
    id_lead,
    id_campanha,
    id_cliente,
    date(data_disparo)              as data_disparo,
    lower(trim(status_resposta))    as status_resposta
from source
