{#
    Test combines not null and unique into one

    It fails if a column is null or occurs more than once. 
#}

{% test primary_key(model, column_name) %}

with validation AS (
    select
        {{ column_name }} as primary_key,
        count(1) as occurrences

    from {{ model }}
    group by 1
)

select *

from validation
where primary_key is null
    or occurrences > 1

{% endtest %}