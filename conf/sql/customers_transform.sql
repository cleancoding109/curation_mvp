-- Transformation SQL for Claimants (SCD Type 2)
-- The source DataFrame is registered as temp view: source_incremental
-- This SQL transforms Bronze data to Silver schema
-- SCD2 columns (effective_start_date, effective_end_date, is_current) are added by the framework

SELECT
    claimant_id,
    TRIM(INITCAP(first_name)) AS first_name,
    TRIM(INITCAP(last_name)) AS last_name,
    CONCAT(TRIM(INITCAP(first_name)), ' ', TRIM(INITCAP(last_name))) AS full_name,
    TO_DATE(date_of_birth, 'yyyy-MM-dd') AS date_of_birth,
    FLOOR(DATEDIFF(CURRENT_DATE(), TO_DATE(date_of_birth, 'yyyy-MM-dd')) / 365.25) AS age,
    CASE 
        WHEN FLOOR(DATEDIFF(CURRENT_DATE(), TO_DATE(date_of_birth, 'yyyy-MM-dd')) / 365.25) >= 65 THEN 'SENIOR'
        ELSE 'ADULT'
    END AS age_group,
    UPPER(TRIM(gender)) AS gender,
    TRIM(address_line1) AS address,
    TRIM(INITCAP(city)) AS city,
    UPPER(TRIM(state)) AS state,
    TRIM(zip_code) AS zip_code,
    REGEXP_REPLACE(phone, '[^0-9]', '') AS phone,
    LOWER(TRIM(email)) AS email,
    UPPER(TRIM(care_level)) AS care_level,
    UPPER(TRIM(marital_status)) AS marital_status,
    TRIM(emergency_contact_name) AS emergency_contact_name,
    REGEXP_REPLACE(emergency_contact_phone, '[^0-9]', '') AS emergency_contact_phone,
    ingestion_ts AS source_timestamp,
    current_timestamp() AS processing_timestamp
FROM source_incremental
WHERE claimant_id IS NOT NULL
