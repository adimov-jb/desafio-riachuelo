with source as (

    select * from {{ source('finvarejo', 'contrato') }}

)

select
    id_contrato,
    id_cliente,
    id_produto,
    id_vendedor,
    id_loja,
    date(data_contrato)              as data_contrato,
    cast(valor_contratado as real)   as valor_contratado,
    num_parcelas,
    lower(trim(status))              as status_contrato
from source
