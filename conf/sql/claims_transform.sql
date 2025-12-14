SELECT
  sha2(cast(claim_id as string), 256) as claim_hash,
  claim_id,
  claimant_id as customer_id,
  policy_number,
  claim_type,
  step_name as workflow_step,
  step_status as workflow_status,
  adjuster_id,
  claim_decision as decision,
  denial_reason,
  claim_amount as requested_amount,
  approved_amount,
  from_utc_timestamp(event_timestamp, 'America/New_York') as event_at,
  source_system,
  source_record_start_dt as effective_start_date,
  source_record_end_dt as effective_end_date,
  ingestion_timestamp as ingestion_ts
FROM source_incremental
