-- Transformation SQL for Customers (SCD Type 2)
-- The source DataFrame is registered as temp view: source_incremental
-- This SQL transforms Bronze data to Silver schema
-- SCD2 columns (effective_start_date, effective_end_date, is_current) are added by the framework

SELECT
    customer_id,
    TRIM(INITCAP(first_name)) AS first_name,
    TRIM(INITCAP(last_name)) AS last_name,
    CONCAT(TRIM(INITCAP(first_name)), ' ', TRIM(INITCAP(last_name))) AS customer_name,
    LOWER(TRIM(email)) AS email,
    REGEXP_REPLACE(phone, '[^0-9+]', '') AS phone,
    TRIM(address_line1) AS address,
    TRIM(INITCAP(city)) AS city,
    UPPER(TRIM(state)) AS state,
    UPPER(TRIM(country)) AS country,
    TRIM(postal_code) AS postal_code,
    TO_DATE(registration_date, 'yyyy-MM-dd') AS registration_date,
    UPPER(TRIM(customer_tier)) AS customer_tier,
    ingestion_ts AS source_timestamp,
    current_timestamp() AS processing_timestamp
FROM source_incremental
WHERE customer_id IS NOT NULL
  AND email IS NOT NULL
