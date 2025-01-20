{% macro generate_hash_key_column(columns, separator='||') %}
    {% if target.type == 'bigquery' %}
        TO_HEX(SHA256(CONCAT({% for column in columns %}{{ column }}{% if not loop.last %}, '{{ separator }}', {% endif %}{% endfor %})))
    {% elif target.type == 'duckdb' %}
        md5({% for column in columns %}{{ column }}{% if not loop.last %} || '{{ separator }}' || {% endif %}{% endfor %})
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported target database: " ~ target.type) }}
    {% endif %}
{% endmacro %}

{% macro generate_surrogate_key_column(columns, separator='||') %}
    {% if target.type == 'bigquery' %}
        CONCAT({% for column in columns %}{{ column }}{% if not loop.last %}, '{{ separator }}', {% endif %}{% endfor %})
    {% elif target.type == 'duckdb' %}
        {% for column in columns %}{{ column }}{% if not loop.last %} || '{{ separator }}' || {% endif %}{% endfor %}
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported target database: " ~ target.type) }}
    {% endif %}
{% endmacro %}