-- Claimants transformation: reads from framework-provided source_deduped
SELECT
  s.claimant_id,
  s.source_system,
  s.first_name,
  s.last_name,
  s.date_of_birth,
  s.address,
  s.city,
  s.state,
  s.zip_code,
  s.phone,
  s.email,
  s.care_level,
  s.ingestion_ts,
  CURRENT_TIMESTAMP() AS processing_timestamp,
  -- Required hashes
  SHA2(CONCAT_WS('|',
    COALESCE(CAST(s.claimant_id AS STRING), '__NULL__'),
    COALESCE(s.source_system, '__NULL__')
  ), 256) AS _pk_hash,
  SHA2(CONCAT_WS('|',
    COALESCE(s.first_name, '__NULL__'),
    COALESCE(s.last_name, '__NULL__'),
    COALESCE(CAST(s.date_of_birth AS STRING), '__NULL__'),
    COALESCE(s.care_level, '__NULL__')
  ), 256) AS _diff_hash
FROM source_deduped s;
