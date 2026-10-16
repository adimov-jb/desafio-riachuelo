with source as (

    select * from {{ source('finvarejo', 'parcela') }}

)

select
    id_parcela,
    id_contrato,
    num_parcela,
    date(data_vencimento)          as data_vencimento,
    date(data_pagamento)           as data_pagamento,
    cast(valor_parcela as real)    as valor_parcela,
    cast(valor_pago as real)       as valor_pago,
    dias_atraso,
    lower(trim(status))            as status_parcela
from source
