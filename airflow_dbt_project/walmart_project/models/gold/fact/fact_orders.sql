SELECT
    order_item_id,
    order_id,
    product_id,
    store_id,
    customer_id,
    quantity,
    unit_price,
    line_amount
FROM {{ ref('obt_b') }}
