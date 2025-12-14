-- Transformation SQL for Claims (SCD Type 1)
-- The source DataFrame is registered as temp view: source_incremental
-- This SQL transforms Bronze data to Silver schema

SELECT
    claim_id,
    claimant_id,
    policy_id,
    provider_id,
    TO_DATE(service_start_date, 'yyyy-MM-dd') AS service_start_date,
    TO_DATE(service_end_date, 'yyyy-MM-dd') AS service_end_date,
    DATEDIFF(TO_DATE(service_end_date, 'yyyy-MM-dd'), TO_DATE(service_start_date, 'yyyy-MM-dd')) + 1 AS service_days,
    UPPER(TRIM(claim_type)) AS claim_type,
    UPPER(TRIM(care_setting)) AS care_setting,
    CAST(billed_amount AS DECIMAL(18, 2)) AS billed_amount,
    CAST(approved_amount AS DECIMAL(18, 2)) AS approved_amount,
    CAST(paid_amount AS DECIMAL(18, 2)) AS paid_amount,
    CAST(billed_amount - COALESCE(approved_amount, 0) AS DECIMAL(18, 2)) AS denied_amount,
    UPPER(TRIM(claim_status)) AS claim_status,
    CASE 
        WHEN UPPER(TRIM(claim_status)) IN ('SUBMITTED', 'PENDING', 'IN_REVIEW') THEN 'OPEN'
        WHEN UPPER(TRIM(claim_status)) IN ('APPROVED', 'PAID', 'PARTIALLY_PAID') THEN 'CLOSED'
        WHEN UPPER(TRIM(claim_status)) IN ('DENIED', 'REJECTED') THEN 'DENIED'
        ELSE 'OTHER'
    END AS claim_status_category,
    TO_TIMESTAMP(submission_date, 'yyyy-MM-dd HH:mm:ss') AS submission_date,
    TO_TIMESTAMP(decision_date, 'yyyy-MM-dd HH:mm:ss') AS decision_date,
    CASE 
        WHEN decision_date IS NOT NULL AND submission_date IS NOT NULL 
        THEN DATEDIFF(TO_DATE(decision_date), TO_DATE(submission_date))
        ELSE NULL
    END AS days_to_decision,
    TRIM(denial_reason_code) AS denial_reason_code,
    TRIM(denial_reason_description) AS denial_reason_description,
    ingestion_ts AS source_timestamp,
    current_timestamp() AS processing_timestamp
FROM source_incremental
WHERE claim_id IS NOT NULL
  AND claimant_id IS NOT NULL
