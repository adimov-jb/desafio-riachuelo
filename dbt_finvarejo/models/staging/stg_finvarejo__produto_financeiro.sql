with source as (

    select * from {{ source('finvarejo', 'produto_financeiro') }}

)

select
    id_produto,
    nome_produto,
    lower(trim(tipo))                   as tipo_produto,
    cast(taxa_juros_mensal as real)     as taxa_juros_mensal,
    cast(limite_maximo as real)         as limite_maximo
from source
