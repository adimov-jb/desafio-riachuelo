{#
  1 linha por contrato elegível (exclui cancelados: sem exposição de crédito).
  Evento de inadimplência = primeira parcela (por vencimento) com status atrasada/baixada.
  O status do contrato NÃO é usado: há contratos "ativo" com parcelas atrasadas.
#}

with contratos as (

    select *
    from {{ ref('stg_finvarejo__contrato') }}
    where status_contrato <> 'cancelado'

),

parcelas_inadimplentes as (

    select
        id_contrato,
        num_parcela,
        data_vencimento,
        row_number() over (
            partition by id_contrato
            order by data_vencimento, num_parcela
        ) as ordem_inadimplencia
    from {{ ref('stg_finvarejo__parcela') }}
    where status_parcela in ('{{ var("status_parcela_inadimplente") | join("', '") }}')
      and dias_atraso >= {{ var('dias_atraso_minimo') }}

)

select
    c.id_contrato,
    c.data_contrato,
    {{ safra_trimestral('c.data_contrato') }}                           as safra,
    cast(strftime('%Y', c.data_contrato) as integer)                    as ano_safra,
    (cast(strftime('%m', c.data_contrato) as integer) + 2) / 3          as trimestre_safra,
    p.data_vencimento                                                   as data_primeira_inadimplencia,
    p.num_parcela                                                       as parcela_primeira_inadimplencia,
    case
        when p.id_contrato is not null
            then {{ meses_de_vida('c.data_contrato', 'p.data_vencimento') }}
    end                                                                 as mes_vida_inadimplencia
from contratos as c
left join parcelas_inadimplentes as p
    on  p.id_contrato = c.id_contrato
    and p.ordem_inadimplencia = 1
