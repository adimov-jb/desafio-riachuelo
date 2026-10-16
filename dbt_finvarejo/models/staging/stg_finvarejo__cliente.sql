with source as (

    select * from {{ source('finvarejo', 'cliente') }}

)

select
    id_cliente,
    nome,
    lower(trim(email))                                                  as email,
    substr(lower(trim(email)), instr(lower(trim(email)), '@') + 1)      as dominio_email,
    cpf,
    date(data_nascimento)                                               as data_nascimento,
    cidade,
    upper(trim(estado))                                                 as estado,
    cast(renda_declarada as real)                                       as renda_declarada,
    score_interno,
    lower(trim(canal_aquisicao))                                        as canal_aquisicao,
    date(data_cadastro)                                                 as data_cadastro
from source
