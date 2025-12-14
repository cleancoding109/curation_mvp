-- Policies transformation: reads from source_deduped
SELECT
  s.policy_id,
  s.source_system,
  s.policy_status,
  s.coverage_type,
  s.daily_benefit_amount,
  s.benefit_period,
  s.elimination_period,
  s.premium_amount,
  s.ingestion_ts,
  CURRENT_TIMESTAMP() AS processing_timestamp,
  SHA2(CONCAT_WS('|',
    COALESCE(CAST(s.policy_id AS STRING), '__NULL__'),
    COALESCE(s.source_system, '__NULL__')
  ), 256) AS _pk_hash,
  SHA2(CONCAT_WS('|',
    COALESCE(s.policy_status, '__NULL__'),
    COALESCE(s.coverage_type, '__NULL__'),
    COALESCE(CAST(s.daily_benefit_amount AS STRING), '__NULL__'),
    COALESCE(CAST(s.premium_amount AS STRING), '__NULL__')
  ), 256) AS _diff_hash
FROM source_deduped s;
