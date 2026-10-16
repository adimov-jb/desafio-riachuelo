{#
  Data de referência (corte) única para todos os modelos: a var data_referencia
  ou, se nula, a última data de pagamento observada na base.
#}
{% macro data_referencia() -%}
    {%- if var('data_referencia') -%}
        date('{{ var("data_referencia") }}')
    {%- else -%}
        (select max(data_pagamento) from {{ ref('stg_finvarejo__parcela') }})
    {%- endif -%}
{%- endmacro %}
