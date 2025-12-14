SELECT 
  {{_pk_hash}},
  {{_diff_hash}},
  src.customer_id,
  src.customer_name AS full_name,
  src.date_of_birth,
  src.email,
  src.phone AS phone_number,
  src.state AS state_code,
  {{st:state_name}} AS state_name,
  {{st:region}} AS region,
  src.zip_code AS postal_code,
  src.status AS customer_status,
  to_utc_timestamp(src.last_login, 'America/New_York') AS last_login_at_utc,
  src.session_count,
  src.page_views,
  src.source_system,
  coalesce(src.deleted_ind, false) AS deleted_ind,
  src.effective_start_date,
  src.effective_end_date,
  src.is_current
FROM {{source}} src
LEFT JOIN {{ref:state_lookup}} st ON src.state = st.state_code