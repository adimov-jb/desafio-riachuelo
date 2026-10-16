{#
  Mês de vida do contrato em meses comerciais de 30 dias (mesmo ciclo das parcelas).
  Mês parcial conta como mês iniciado: dias 1-30 -> M01, 31-60 -> M02, ...
  Assim, a parcela N (vencimento = contrato + 30*N dias) cai exatamente no mês N.
#}
{% macro meses_de_vida(data_inicio, data_evento) -%}
    cast((julianday({{ data_evento }}) - julianday({{ data_inicio }}) + 29) / 30 as integer)
{%- endmacro %}


{# Safra trimestral no formato AAAA-Tn #}
{% macro safra_trimestral(data) -%}
    strftime('%Y', {{ data }}) || '-T' || ((cast(strftime('%m', {{ data }}) as integer) + 2) / 3)
{%- endmacro %}
