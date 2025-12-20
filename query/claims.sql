SELECT
    {{_pk_hash}} AS _pk_hash,
    {{_diff_hash}} AS _diff_hash,
    
    src.claim_id,
    src.claimant_id AS customer_id,
    src.policy_number,
    src.claim_type,
    src.step_name AS workflow_step,
    src.step_status AS workflow_status,
    src.adjuster_id,
    
    {{adj:adjuster_name}} AS adjuster_name,
    {{adj:adjuster_region}} AS adjuster_region,
    
    src.claim_decision AS decision,
    src.denial_reason,
    src.claim_amount AS requested_amount,
    src.approved_amount,
    
    to_utc_timestamp(src.event_timestamp, '{{source_timezone}}') AS event_timestamp_utc,
    
    src.source_system,
    coalesce(src.is_deleted, false) AS deleted_ind

FROM {{source}} src
LEFT JOIN {{ref:adjuster_lookup}} adj ON src.adjuster_id = adj.adjuster_id
