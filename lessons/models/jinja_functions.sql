{% set columns = adapter.get_columns_in_relation(ref('dim_orders')) %} -- retrieves all the columns in a table

-- we could then loop through them
select {% for column in columns -%}
	{%- if column.name.startswith('total') %} -- looks for columns if they start with total
	{{ column.name.lower() }},
	{%- endif -%}
{%- endfor %}