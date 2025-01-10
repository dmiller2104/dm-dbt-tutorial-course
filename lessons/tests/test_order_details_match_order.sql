/*
 A test to ensure that all the order details numbers
 match the order details numbers.

 Returns all the row where we dont get a match
*/

with order_details AS (
    select
        order_id,
        COUNT(*) as num_of_items_in_order

    from {{ ref('stg_ecommerce__order_items') }}
    group by 1
)

Select
    o.*,
    od.*

from {{ ref('stg_ecommerce__orders') }}as o
full outer join order_details as od USING(order_id)
where
    -- All orders should have at least 1 item, and every item should tie to an order
    o.order_id IS NULL
    OR od.order_id IS NULL
    -- Number of items doesn't match
    OR o.num_items_ordered != od.num_of_items_in_order
