-- A reusable test: fails for every row where the column is outside [min_value, max_value].
-- Leave either bound out to only check the other side.
{% test between(model, column_name, min_value=none, max_value=none, inclusive=true) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and (
    {% if min_value is not none %}
      {{ column_name }} {{ '<' if inclusive else '<=' }} {{ min_value }}
    {% else %} false {% endif %}
    or
    {% if max_value is not none %}
      {{ column_name }} {{ '>' if inclusive else '>=' }} {{ max_value }}
    {% else %} false {% endif %}
  )
{% endtest %}
