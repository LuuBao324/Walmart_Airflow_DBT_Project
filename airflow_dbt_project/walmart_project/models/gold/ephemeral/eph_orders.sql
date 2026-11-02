SELECT
    order_id,
    payment_method,
    order_status,
    total_amount,
    order_timestamp,
    created_timestamp AS order_created_timestamp,
    updated_timestamp AS order_updated_timestamp,
    is_active AS order_is_active,
    processed_at AS order_processed_at,
    current_timestamp() AS order_gold_processed_at
FROM {{ ref('orders_t') }}
