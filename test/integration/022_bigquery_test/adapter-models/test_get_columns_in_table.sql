{% set source = ref('source') %}
{% set cols = get_columns_in_relation(source) %}

select
    {% for col in cols %}
        {{ col.name }}
        {% if not loop.last %}, {% endif %}
    {% endfor %}

from {{ source }}
