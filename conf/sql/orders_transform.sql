-- Transformation SQL for Orders (SCD Type 1)
-- The source DataFrame is registered as temp view: source_incremental
-- This SQL transforms Bronze data to Silver schema

SELECT
    order_id,
    customer_id,
    product_id,
    CAST(order_quantity AS INT) AS order_quantity,
    CAST(unit_price AS DECIMAL(18, 2)) AS unit_price,
    CAST(order_quantity * unit_price AS DECIMAL(18, 2)) AS total_amount,
    UPPER(TRIM(order_status)) AS order_status,
    TO_TIMESTAMP(order_date, 'yyyy-MM-dd HH:mm:ss') AS order_date,
    TRIM(shipping_address) AS shipping_address,
    TRIM(billing_address) AS billing_address,
    ingestion_ts AS source_timestamp,
    current_timestamp() AS processing_timestamp
FROM source_incremental
WHERE order_id IS NOT NULL
  AND customer_id IS NOT NULL
