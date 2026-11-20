-- AgentCloud research contract v1.1: proposed, unimplemented analytical model.
-- DuckDB DDL, not a PostgreSQL production migration. Raw events remain immutable.
-- All ns values arrive as decimal JSON strings and are cast to DECIMAL(38,0).
-- UTC is not inferred from a monotonic counter. No FK rejects late-arriving events;
-- cross-table integrity is checked after a closed ingest watermark.
CREATE SCHEMA IF NOT EXISTS agentcloud_research;

CREATE TABLE agentcloud_research.dataset_versions (
  dataset_id VARCHAR NOT NULL, dataset_version VARCHAR NOT NULL,
  schema_version VARCHAR NOT NULL, created_at_ns DECIMAL(38,0) NOT NULL,
  source_manifest_sha256 VARCHAR NOT NULL, transform_commit VARCHAR NOT NULL,
  split_manifest_sha256 VARCHAR, license_id VARCHAR, source_urls JSON,
  selection_policy JSON, privacy_policy JSON, raw_object_manifest JSON,
  PRIMARY KEY(dataset_id, dataset_version)
);
CREATE TABLE agentcloud_research.runs (
  run_id VARCHAR PRIMARY KEY, dataset_id VARCHAR NOT NULL, dataset_version VARCHAR NOT NULL,
  execution_kind VARCHAR NOT NULL, source_commit VARCHAR, workflow_commit VARCHAR,
  policy_commit VARCHAR, config_sha256 VARCHAR NOT NULL, hardware_manifest_sha256 VARCHAR,
  paired_runner_id VARCHAR, seed BIGINT, planned_trial_count BIGINT,
  planned_request_count BIGINT, actual_start_ns DECIMAL(38,0), actual_end_ns DECIMAL(38,0),
  status VARCHAR, environment JSON, frozen_plan JSON, artifact_manifest JSON
);
CREATE TABLE agentcloud_research.raw_events (
  event_id VARCHAR NOT NULL, ingest_batch_id VARCHAR NOT NULL,
  dataset_id VARCHAR NOT NULL, dataset_version VARCHAR NOT NULL,
  source_object_uri VARCHAR, source_row_number BIGINT,
  payload_sha256 VARCHAR NOT NULL, ingest_time_ns DECIMAL(38,0) NOT NULL,
  payload JSON NOT NULL, parse_status VARCHAR NOT NULL,
  PRIMARY KEY(ingest_batch_id, event_id, payload_sha256)
);
CREATE TABLE agentcloud_research.events (
  event_id VARCHAR PRIMARY KEY, event_type VARCHAR NOT NULL,
  schema_version VARCHAR NOT NULL, dataset_id VARCHAR NOT NULL, dataset_version VARCHAR NOT NULL,
  run_id VARCHAR, source_component VARCHAR NOT NULL,
  source_instance_id VARCHAR NOT NULL, source_seq DECIMAL(38,0) NOT NULL,
  source_revision VARCHAR, source_record_id VARCHAR, source_commit VARCHAR,
  tenant_id_hmac VARCHAR, workspace_id VARCHAR, session_id VARCHAR, parent_session_id VARCHAR,
  root_session_id VARCHAR, turn_id VARCHAR, loop_id VARCHAR,
  trace_id VARCHAR, span_id VARCHAR, parent_span_id VARCHAR,
  actor_id VARCHAR, actor_epoch DECIMAL(38,0), worker_id VARCHAR, node_id VARCHAR,
  assignment_id VARCHAR, queue_episode_id VARCHAR,
  operation_id VARCHAR, operation_attempt_id VARCHAR, model_request_id VARCHAR,
  model_attempt_id VARCHAR, tool_call_id VARCHAR, snapshot_id VARCHAR,
  related_decision_event_id VARCHAR, occurred_at_ns DECIMAL(38,0),
  observed_at_ns DECIMAL(38,0) NOT NULL, ingested_at_ns DECIMAL(38,0),
  available_at_ns DECIMAL(38,0), available_to VARCHAR NOT NULL,
  monotonic_ns DECIMAL(38,0), clock_domain_id VARCHAR,
  host_boot_id VARCHAR, process_incarnation VARCHAR, runtime_incarnation VARCHAR,
  wall_offset_ns DECIMAL(38,0), wall_uncertainty_ns DECIMAL(38,0),
  corrected_utc_ns DECIMAL(38,0), duration_ns DECIMAL(38,0),
  duration_clock_domain_id VARCHAR, duration_basis VARCHAR,
  execution_kind VARCHAR NOT NULL, outcome_status VARCHAR NOT NULL, outcome_scope VARCHAR NOT NULL,
  error_code VARCHAR, error_class VARCHAR, is_terminal BOOLEAN NOT NULL,
  censor_kind VARCHAR NOT NULL, censor_reason VARCHAR, censor_at_ns DECIMAL(38,0),
  exposure_ns DECIMAL(38,0), completion_observed BOOLEAN NOT NULL,
  informative_censoring_possible BOOLEAN, normalization_version VARCHAR NOT NULL,
  CHECK(source_seq >= 0), CHECK(duration_ns IS NULL OR duration_ns >= 0)
);
CREATE TABLE agentcloud_research.event_states (
  event_id VARCHAR NOT NULL, boundary VARCHAR NOT NULL CHECK(boundary IN ('before','after')),
  agent_phase VARCHAR, runtime_state VARCHAR, admission_state VARCHAR,
  inference_state VARCHAR, safety_gate VARCHAR, active_model_requests BIGINT,
  active_tools BIGINT, active_subagents BIGINT, PRIMARY KEY(event_id, boundary)
);
CREATE TABLE agentcloud_research.causal_links (
  child_event_id VARCHAR NOT NULL, parent_event_id VARCHAR NOT NULL,
  relation VARCHAR NOT NULL, dependency_kind VARCHAR NOT NULL,
  dependency_group_id VARCHAR NOT NULL, join_semantics VARCHAR NOT NULL,
  required BOOLEAN NOT NULL, minimum_lag_ns DECIMAL(38,0),
  evidence_class VARCHAR NOT NULL CHECK(evidence_class IN ('A','B','C')),
  transform_version VARCHAR, PRIMARY KEY(child_event_id, parent_event_id, relation)
);
CREATE TABLE agentcloud_research.source_watermarks (
  decision_event_id VARCHAR NOT NULL, source_instance_id VARCHAR NOT NULL,
  through_source_seq DECIMAL(38,0) NOT NULL, source_revision VARCHAR,
  available_at_ns DECIMAL(38,0) NOT NULL, age_ns DECIMAL(38,0),
  PRIMARY KEY(decision_event_id, source_instance_id)
);
CREATE TABLE agentcloud_research.sessions (
  dataset_id VARCHAR NOT NULL, dataset_version VARCHAR NOT NULL,
  tenant_id_hmac VARCHAR NOT NULL, session_id VARCHAR NOT NULL,
  parent_session_id VARCHAR, root_session_id VARCHAR, family_id_hmac VARCHAR, harness_id VARCHAR,
  first_event_id VARCHAR, terminal_event_id VARCHAR,
  source_start_ns DECIMAL(38,0), source_end_ns DECIMAL(38,0),
  left_censored BOOLEAN, right_censored BOOLEAN, terminal_status VARCHAR,
  PRIMARY KEY(dataset_id, dataset_version, tenant_id_hmac, session_id)
);
CREATE TABLE agentcloud_research.runtime_executions (
  runtime_incarnation VARCHAR PRIMARY KEY, actor_id VARCHAR NOT NULL,
  actor_epoch DECIMAL(38,0) NOT NULL, worker_id VARCHAR, assignment_id VARCHAR, host_boot_id VARCHAR,
  parent_runtime_incarnation VARCHAR, restored_from_snapshot_id VARCHAR,
  start_event_id VARCHAR NOT NULL, end_event_id VARCHAR,
  coordinator_state_revision VARCHAR, identity_authority VARCHAR NOT NULL
);
CREATE TABLE agentcloud_research.snapshots (
  dataset_id VARCHAR NOT NULL, dataset_version VARCHAR NOT NULL, snapshot_id VARCHAR NOT NULL,
  actor_id VARCHAR NOT NULL, actor_epoch DECIMAL(38,0), generation DECIMAL(38,0),
  captured_runtime_incarnation VARCHAR, captured_source_seq DECIMAL(38,0),
  parent_snapshot_id VARCHAR, fidelity VARCHAR, durability_mode VARCHAR,
  local_complete_event_id VARCHAR, remote_durable_event_id VARCHAR,
  resource_release_event_id VARCHAR, manifest_commit_event_id VARCHAR,
  local_complete_at_ns DECIMAL(38,0), remote_durable_at_ns DECIMAL(38,0),
  resource_released_at_ns DECIMAL(38,0), manifest_committed_at_ns DECIMAL(38,0),
  logical_bytes DECIMAL(38,0), allocated_bytes DECIMAL(38,0),
  continuity_proof_sha256 VARCHAR, PRIMARY KEY(dataset_id, dataset_version, snapshot_id)
);
CREATE TABLE agentcloud_research.admissions (
  observation_event_id VARCHAR PRIMARY KEY, queue_id VARCHAR NOT NULL, queue_episode_id VARCHAR,
  queue_kind VARCHAR NOT NULL, run_id VARCHAR, session_id VARCHAR, operation_id VARCHAR,
  enqueued_at_ns DECIMAL(38,0), admitted_at_ns DECIMAL(38,0), queue_age_ns DECIMAL(38,0),
  queue_rank BIGINT, waiting_first_count BIGINT, waiting_resume_count BIGINT,
  capacity_slots BIGINT, allocated_slots BIGINT, free_slots BIGINT,
  capacity_memory_bytes DECIMAL(38,0), allocated_memory_bytes DECIMAL(38,0),
  tenant_weight DOUBLE, queue_policy_name VARCHAR
);
CREATE TABLE agentcloud_research.decisions (
  event_id VARCHAR PRIMARY KEY, run_id VARCHAR, session_id VARCHAR, actor_epoch DECIMAL(38,0),
  policy_name VARCHAR NOT NULL, policy_version VARCHAR NOT NULL, policy_commit VARCHAR,
  policy_config_sha256 VARCHAR, decision_time_ns DECIMAL(38,0) NOT NULL,
  features_asof_ns DECIMAL(38,0) NOT NULL, state_revision VARCHAR,
  feature_snapshot_sha256 VARCHAR, computed_action VARCHAR NOT NULL, final_action VARCHAR NOT NULL,
  computed_reason VARCHAR NOT NULL, final_reason VARCHAR NOT NULL, safety_override_reason VARCHAR,
  safe_point_observed BOOLEAN, episode_id VARCHAR, wait_age_ns DECIMAL(38,0),
  holding_rate DOUBLE, shadow BOOLEAN NOT NULL, chosen_candidate_id VARCHAR,
  observation_duration_ns DECIMAL(38,0), decision_cpu_ns DECIMAL(38,0), decision_wall_ns DECIMAL(38,0)
);
CREATE TABLE agentcloud_research.decision_candidates (
  decision_event_id VARCHAR NOT NULL, candidate_id VARCHAR NOT NULL,
  action VARCHAR NOT NULL, feasible BOOLEAN NOT NULL, release_deadline_ns DECIMAL(38,0),
  estimated_cost_value DOUBLE, cost_unit VARCHAR, reason VARCHAR NOT NULL,
  PRIMARY KEY(decision_event_id, candidate_id)
);
CREATE TABLE agentcloud_research.predictions (
  event_id VARCHAR PRIMARY KEY, predictor_name VARCHAR NOT NULL, predictor_version VARCHAR NOT NULL,
  training_manifest_sha256 VARCHAR, trained_until_ns DECIMAL(38,0), features_asof_ns DECIMAL(38,0) NOT NULL,
  target VARCHAR NOT NULL, known BOOLEAN NOT NULL, unknown_reason VARCHAR,
  sample_count BIGINT, completed_count BIGINT, right_censored_count BIGINT,
  at_risk_count BIGINT, minimum_support BIGINT, current_age_ns DECIMAL(38,0),
  support_horizon_ns DECIMAL(38,0), quantile_level DOUBLE, remaining_wait_ns DECIMAL(38,0),
  prediction_lower_ns DECIMAL(38,0), prediction_upper_ns DECIMAL(38,0), interval_level DOUBLE,
  distribution_representation VARCHAR
);
CREATE TABLE agentcloud_research.prediction_survival_steps (
  event_id VARCHAR NOT NULL, ordinal BIGINT NOT NULL, remaining_ns DECIMAL(38,0) NOT NULL,
  survival_probability DOUBLE NOT NULL CHECK(survival_probability BETWEEN 0 AND 1),
  PRIMARY KEY(event_id, ordinal)
);
CREATE TABLE agentcloud_research.cost_measurements (
  event_id VARCHAR NOT NULL, phase_ordinal BIGINT NOT NULL, phase VARCHAR NOT NULL,
  related_decision_event_id VARCHAR, operation_id VARCHAR NOT NULL, operation_attempt_id VARCHAR NOT NULL,
  start_event_id VARCHAR, end_event_id VARCHAR, clock_domain_id VARCHAR,
  duration_ns DECIMAL(38,0), cpu_ns DECIMAL(38,0), logical_bytes DECIMAL(38,0),
  allocated_bytes DECIMAL(38,0), physical_write_bytes DECIMAL(38,0), network_bytes DECIMAL(38,0),
  memory_gb_s DOUBLE, retry_count BIGINT, outcome VARCHAR NOT NULL,
  measurement_method VARCHAR NOT NULL, sample_coverage DOUBLE,
  PRIMARY KEY(event_id, phase_ordinal)
);
CREATE TABLE agentcloud_research.cost_estimates (
  event_id VARCHAR NOT NULL, phase_ordinal BIGINT NOT NULL, phase VARCHAR NOT NULL,
  cost_model_id VARCHAR NOT NULL, cost_model_version VARCHAR NOT NULL,
  features_asof_ns DECIMAL(38,0) NOT NULL, workload_class VARCHAR, hardware_config_sha256 VARCHAR,
  duration_ns DECIMAL(38,0), p95_duration_ns DECIMAL(38,0), mean_ci_lower_ns DECIMAL(38,0),
  mean_ci_upper_ns DECIMAL(38,0), ci_level DOUBLE, logical_bytes DECIMAL(38,0),
  physical_write_bytes DECIMAL(38,0), memory_gb_s DOUBLE, sample_count BIGINT,
  measurement_basis_event_ids JSON NOT NULL, PRIMARY KEY(event_id, phase_ordinal)
);
CREATE TABLE agentcloud_research.resource_samples (
  event_id VARCHAR PRIMARY KEY, resource_scope VARCHAR NOT NULL, scope_id VARCHAR NOT NULL,
  attribution_method VARCHAR NOT NULL, sample_start_ns DECIMAL(38,0), sample_end_ns DECIMAL(38,0),
  sample_clock_domain_id VARCHAR, source_update_at_ns DECIMAL(38,0) NOT NULL,
  actual_interval_ns DECIMAL(38,0), is_cached_read BOOLEAN NOT NULL,
  substrate_stats_epoch_raw VARCHAR, counter_reset BOOLEAN, coverage_fraction DOUBLE
);
CREATE TABLE agentcloud_research.resource_metrics (
  event_id VARCHAR NOT NULL, metric_name VARCHAR NOT NULL,
  integer_value DECIMAL(38,0), numeric_value DOUBLE, unit VARCHAR NOT NULL,
  evidence_class VARCHAR NOT NULL CHECK(evidence_class IN ('A','B','C')),
  PRIMARY KEY(event_id, metric_name),
  CHECK((integer_value IS NULL) <> (numeric_value IS NULL)),
  CHECK(metric_name NOT IN ('engine_prefill_tokens_executed_total','engine_prefill_tokens_recomputed_total')
        OR (integer_value IS NOT NULL AND integer_value >= 0 AND unit = 'token_execution'))
);
CREATE TABLE agentcloud_research.model_request_observations (
  event_id VARCHAR PRIMARY KEY, model_request_id VARCHAR NOT NULL, model_attempt_id VARCHAR,
  model_id VARCHAR, backend_kind VARCHAR NOT NULL, selected_inference_worker_id VARCHAR,
  submitted_at_ns DECIMAL(38,0), engine_queue_enter_ns DECIMAL(38,0), engine_service_start_ns DECIMAL(38,0),
  first_token_at_ns DECIMAL(38,0), first_token_boundary VARCHAR, completed_at_ns DECIMAL(38,0),
  input_tokens_known DECIMAL(38,0), output_tokens_so_far DECIMAL(38,0), final_output_tokens DECIMAL(38,0),
  engine_cached_input_tokens DECIMAL(38,0), indexed_prefix_tokens_before_dispatch DECIMAL(38,0),
  kv_blocks_stored DECIMAL(38,0), kv_blocks_removed DECIMAL(38,0), kv_index_sequence DECIMAL(38,0),
  service_time_semantics VARCHAR NOT NULL
);
CREATE TABLE agentcloud_research.tool_observations (
  event_id VARCHAR PRIMARY KEY, tool_call_id VARCHAR NOT NULL, parent_tool_call_id VARCHAR,
  tool_name VARCHAR, tool_class VARCHAR, process_handle_hash VARCHAR,
  result_available BOOLEAN, process_terminal_observed BOOLEAN, exit_code BIGINT,
  child_session_id VARCHAR, join_policy VARCHAR
);
CREATE TABLE agentcloud_research.sampling_membership (
  event_id VARCHAR PRIMARY KEY, sampling_design VARCHAR NOT NULL, sampling_unit VARCHAR NOT NULL,
  inclusion_probability DOUBLE, inclusion_probability_known BOOLEAN NOT NULL,
  probability_source VARCHAR, stratum_id VARCHAR, family_id_hmac VARCHAR,
  dataset_split VARCHAR NOT NULL, selection_rule_version VARCHAR, sample_weight DOUBLE,
  production_arrival_recoverable VARCHAR NOT NULL, quality_admitted BOOLEAN,
  CHECK(inclusion_probability IS NULL OR inclusion_probability > 0 AND inclusion_probability <= 1)
);
CREATE TABLE agentcloud_research.field_provenance (
  event_id VARCHAR NOT NULL, field_path VARCHAR NOT NULL,
  evidence_class VARCHAR NOT NULL CHECK(evidence_class IN ('A','B','C')),
  value_kind VARCHAR NOT NULL, availability_class VARCHAR NOT NULL,
  source_event_ids JSON NOT NULL, source_field_paths JSON NOT NULL, source_artifact_sha256 VARCHAR,
  transform_version VARCHAR, assumption VARCHAR, available_at_ns DECIMAL(38,0),
  PRIMARY KEY(event_id, field_path)
);
CREATE TABLE agentcloud_research.missing_fields (
  event_id VARCHAR NOT NULL, field_path VARCHAR NOT NULL,
  reason VARCHAR NOT NULL, handling VARCHAR NOT NULL, PRIMARY KEY(event_id, field_path)
);
CREATE TABLE agentcloud_research.quality_results (
  validation_run_id VARCHAR NOT NULL, event_id VARCHAR NOT NULL, rule_id VARCHAR NOT NULL,
  rule_version VARCHAR NOT NULL, severity VARCHAR NOT NULL, passed BOOLEAN NOT NULL,
  detail JSON, checked_at_ns DECIMAL(38,0) NOT NULL,
  PRIMARY KEY(validation_run_id, event_id, rule_id)
);
-- Recommended Parquet layout: dataset_id / dataset_version / run_id / event_family.
-- Avoid one tiny file per session. Partition by day only when the dataset has valid wall dates.
-- Each export receives a new immutable dataset_version + SHA256 manifest.
-- v1.1 native prefill counters map without adding separate metric tables:
-- JSON /resource_usage/engine_prefill_tokens_executed_total or
-- /resource_usage/engine_prefill_tokens_recomputed_total -> resource_metrics
-- (metric_name = exact JSON leaf name, integer_value = DECIMAL(38,0), unit='token_execution').
-- Missing native counters remain null in raw JSON + missing_fields; omit the metric fact row.
-- Join resource_samples by event_id: resource_scope must be inference_worker;
-- scope_id includes the native engine instance and complete counter label set.
-- counter_reset must be known for a populated native counter. Source names/labels,
-- implementation commit and observation availability remain in field_provenance/events.
-- Split deltas at engine-instance/reset boundaries. Never derive these actual counters
-- from input-cached tokens; never duplicate a worker-total delta across requests or TP shards.
-- Example (execute only after materialization with an explicit intended destination):
-- COPY agentcloud_research.events TO '<destination>/events.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
