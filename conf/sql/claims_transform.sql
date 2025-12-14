-- Claims transformation: SCD Type 1
SELECT
  s.claim_id,
  s.source_system,
  s.claimant_id,
  s.policy_id,
  s.claim_status,
  s.claim_amount,
  s.service_date,
  s.submitted_at,
  s.ingestion_ts,
  CURRENT_TIMESTAMP() AS processing_timestamp,
  SHA2(CONCAT_WS('|',
    COALESCE(CAST(s.claim_id AS STRING), '__NULL__'),
    COALESCE(s.source_system, '__NULL__')
  ), 256) AS _pk_hash
FROM source_deduped s;
