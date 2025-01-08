WITH

-- aggregate measures
order_item_measures AS (
    SELECT
        order_id,
        SUM(item_sale_price) as total_sale_price,
        SUM(product_cost) as total_product_cost,
        SUM(item_profit) as total_profit,
        SUM(item_discount) as total_discount,
{#
        {%- set departments = ['Men', 'Women'] -%}
        {%- set department_name in departments %}
        SUM(IF(product_department = '{{department_name}}', item_sale_price))
        {% end for %} #}

    FROM {{ ref('int_ecommerce__order_items_products') }}
    group by 1
)

SELECT
    -- data from orders table
	od.order_id,
	od.created_at AS order_created_at,
	od.shipped_at AS order_shipped_at,
	od.delivered_at AS order_delivered_at,
	od.returned_at AS order_returned_at,
	od.status AS order_status,
	od.num_items_ordered,
    -- dates on order level
	om.total_sale_price,
	om.total_product_cost,
	om.total_profit,
	om.total_discount,

from {{ ref('stg_ecommerce__orders') }} as od
left join order_item_measures AS om
    on od.order_id = om.order_id