SELECT
  sha2(cast(customer_id as string), 256) as customer_hash,
  customer_id,
  customer_name as full_name,
  date_of_birth,
  email,
  phone as phone_number,
  state as state_code,
  zip_code as postal_code,
  status as customer_status,
  from_utc_timestamp(last_login, 'America/New_York') as last_login_at,
  session_count,
  page_views,
  source_system,
  source_record_start_dt as effective_start_date,
  source_record_end_dt as effective_end_date,
  ingestion_timestamp as ingestion_ts
FROM source_incremental