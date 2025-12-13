-- Transformation SQL for Products (SCD Type 2 with Composite Key)
-- The source DataFrame is registered as temp view: source_incremental
-- This SQL transforms Bronze data to Silver schema
-- SCD2 columns (effective_start_date, effective_end_date, is_current) are added by the framework

SELECT
    product_id,
    region_code,
    TRIM(product_name) AS product_name,
    TRIM(INITCAP(category)) AS category,
    TRIM(subcategory) AS subcategory,
    CAST(price AS DECIMAL(18, 2)) AS price,
    CAST(cost AS DECIMAL(18, 2)) AS cost,
    CAST(price - cost AS DECIMAL(18, 2)) AS margin,
    supplier_id,
    TRIM(supplier_name) AS supplier_name,
    CAST(stock_quantity AS INT) AS stock_quantity,
    CASE 
        WHEN stock_quantity > 100 THEN 'HIGH'
        WHEN stock_quantity > 20 THEN 'MEDIUM'
        WHEN stock_quantity > 0 THEN 'LOW'
        ELSE 'OUT_OF_STOCK'
    END AS stock_status,
    CAST(is_active AS BOOLEAN) AS is_active,
    ingestion_ts AS source_timestamp,
    current_timestamp() AS processing_timestamp
FROM source_incremental
WHERE product_id IS NOT NULL
  AND region_code IS NOT NULL
