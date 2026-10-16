{% test unique_combination_safra_mes(model, columns) %}

select {{ columns | join(', ') }}, count(*) as qtd
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}
