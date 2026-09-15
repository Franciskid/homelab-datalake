-- GENERATED FILE. Do not hand-edit.
--
-- Rendered from homelab_datalake.parsing.ai_platform_registry.render_grant_sql().
-- test_ai_platform_registry.py asserts this file is exactly what that function
-- renders today, so a registry change that is not accompanied by regenerating this
-- file fails the test suite rather than silently drifting.
--
-- Run on the ai_platform Postgres as a superuser (or a role with GRANT privilege).
-- CONNECTION LIMIT 3: the pipeline opens one connection per run and closes it
-- immediately (see ai_platform.py), and the server this runs on is shared --
-- max_connections is 100, with keycloak, banking-tracker, litellm and dagster all
-- drawing on it too.

CREATE ROLE datalake_ro LOGIN CONNECTION LIMIT 3 PASSWORD '<set at provisioning time>';
GRANT CONNECT ON DATABASE ai_platform TO datalake_ro;
GRANT USAGE ON SCHEMA public TO datalake_ro;

GRANT SELECT (id, agent_id, version, created_by_user_id, definition, created_at) ON "agent_versions" TO datalake_ro;
GRANT SELECT (id, owner_user_id, team_id, name, description, status, current_version, created_at, updated_at, template_id) ON "agents" TO datalake_ro;
GRANT SELECT (id, application_id, created_by_user_id, label, prefix, suffix, status, last_used_at, revoked_at, created_at, updated_at) ON "api_keys" TO datalake_ro;
GRANT SELECT (application_id, user_id, created_at) ON "application_owners" TO datalake_ro;
GRANT SELECT (id, created_by_user_id, team_id, name, environment, status, metadata, created_at, updated_at, model_aliases, tool_profiles, monthly_budget_eur, last_used_at, parent_application_id) ON "applications" TO datalake_ro;
GRANT SELECT (id, run_id, requested_by_user_id, decided_by_user_id, tool_name, risk, status, purpose, target, arguments, expires_at, decided_at, created_at, updated_at) ON "approvals" TO datalake_ro;
GRANT SELECT (id, actor_user_id, actor_subject, action, resource_type, resource_id, result, request_id, ip_address, user_agent, metadata, occurred_at) ON "audit_events" TO datalake_ro;
GRANT SELECT (id, automation_id, dagster_run_id, status, started_at, finished_at, created_at) ON "automation_runs" TO datalake_ro;
GRANT SELECT (id, automation_run_id, step_id, dagster_run_id, status, outputs, error, started_at, finished_at, step_position, step_type, step_label, step_config) ON "automation_step_runs" TO datalake_ro;
GRANT SELECT (id, automation_id, position, type, label, config, created_at, updated_at) ON "automation_steps" TO datalake_ro;
GRANT SELECT (id, owner_user_id, name, description, enabled, cadence_cron, timezone, created_by_approval_id, created_at, updated_at, consecutive_failures, paused_reason, paused_at, last_error) ON "automations" TO datalake_ro;
GRANT SELECT (id, created_by_user_id, name, scope_type, scope_id, period, amount_eur, alert_threshold_percent, hard_limit, status, metadata, created_at, updated_at) ON "budgets" TO datalake_ro;
GRANT SELECT (id, owner_user_id, team_id, title, agent_id, created_at, updated_at, deleted_at, reasoning_level, origin) ON "conversations" TO datalake_ro;
GRANT SELECT (id, source, chunk_key, section, content, content_hash, indexed_at) ON "document_chunks" TO datalake_ro;
GRANT SELECT (id, owner_user_id, name, slug, curated, created_by_run_id, created_at, updated_at) ON "document_tags" TO datalake_ro;
GRANT SELECT (id, owner_user_id, label, address, notes, archived_at, created_at, updated_at) ON "email_recipients" TO datalake_ro;
GRANT SELECT (id, owner_user_id, run_id, approval_id, template_script_id, template_version, subject, recipients, attachments, status, failure_reason, created_at) ON "email_sends" TO datalake_ro;
GRANT SELECT (id, generated_file_id, document_tag_id, assigned_by, run_id, created_at) ON "generated_file_tags" TO datalake_ro;
GRANT SELECT (id, conversation_id, run_id, filename, mime_type, byte_size, data, created_at, tool_name, server_alias) ON "generated_files" TO datalake_ro;
GRANT SELECT (id, fingerprint, status, name, summary, severity, occurrences, opened_at, last_seen_at, resolved_at) ON "incidents" TO datalake_ro;
GRANT SELECT (index_key, status, started_at, finished_at, last_successful_at, indexed, skipped, failed, error, cost_eur, embed_tokens, updated_at) ON "index_build_states" TO datalake_ro;
GRANT SELECT (id, created_by_user_id, name, transport, url, status, tool_count, allowed_tools, auth_fingerprint, auth_updated_at, created_at, updated_at) ON "mcp_servers" TO datalake_ro;
GRANT SELECT (id, jellyfin_item_id, title, year, genres, overview, community_rating, content_hash, indexed_at, cast_line, media_type) ON "media_items" TO datalake_ro;
GRANT SELECT (id, conversation_id, message_id, uploaded_by_user_id, mime_type, byte_size, data, created_at) ON "message_attachments" TO datalake_ro;
GRANT SELECT (id, conversation_id, author_user_id, run_id, role, content, metadata, created_at) ON "messages" TO datalake_ro;
GRANT SELECT (id, provider_id, created_by_user_id, alias, display_name, provider_model_id, mode, privacy, status, context_window_tokens, input_cost_eur_per_1m, output_cost_eur_per_1m, allowed_roles, fallback_policy, metadata, created_at, updated_at) ON "model_routes" TO datalake_ro;
GRANT SELECT (id, user_id, kind, severity, subject, body, channel_results, read_at, run_id, schedule_id, incident_id, created_at, report_id) ON "notifications" TO datalake_ro;
GRANT SELECT (id, default_chat_model, default_approval_policy, notification_email, data_retention_days, feature_flags, created_at, updated_at, notification_events, media_index_enabled, media_embedding_model, media_index_cadence_hours, media_default_max_retrieve, media_include_watched_default, media_query_prefix, media_document_prefix, documents_index_enabled, documents_embedding_model, documents_chunk_size, documents_chunk_overlap, documents_query_prefix, documents_document_prefix) ON "platform_settings" TO datalake_ro;
GRANT SELECT (id, created_by_user_id, name, kind, base_url, status, credential_fingerprint, credential_updated_at, capabilities, metadata, created_at, updated_at) ON "providers" TO datalake_ro;
GRANT SELECT (id, user_id, user_agent, created_at) ON "push_subscriptions" TO datalake_ro;
GRANT SELECT (id, owner_user_id, template_id, metric_key, comparator, threshold, severity, enabled, created_at, updated_at, report_definition_id, consecutive_runs, retired_at) ON "report_alert_rules" TO datalake_ro;
GRANT SELECT (id, rule_id, report_id, owner_user_id, metric_key, metric_label, unit, value, threshold, comparator, severity, created_at) ON "report_alerts" TO datalake_ro;
GRANT SELECT (id, owner_user_id, template_id, agent_id, name, period, timezone, comparison_enabled, enabled_sections, advanced_instruction, notify_on_ready, notify_on_failure, delivery_channels, created_at, updated_at, language) ON "report_definitions" TO datalake_ro;
GRANT SELECT (id, owner_user_id, run_id, agent_id, template_id, title, summary, period_start, period_end, payload_version, payload, created_at, metrics, report_definition_id, dataset_version, layout_version, normalized_data, insights, archived_at) ON "reports" TO datalake_ro;
GRANT SELECT (id, provider, resource_type, local_resource_type, local_resource_id, external_id, metadata, created_at, updated_at) ON "resource_mappings" TO datalake_ro;
GRANT SELECT (id, run_id, sequence, type, title, description, payload, occurred_at) ON "run_steps" TO datalake_ro;
GRANT SELECT (id, conversation_id, initiated_by_user_id, status, requested_model, actual_route, input_tokens, output_tokens, cost_eur, started_at, completed_at, failure_code, metadata, awaiting_input_deadline, report_definition_id, reasoning_tokens) ON "runs" TO datalake_ro;
GRANT SELECT (id, run_id, owner_user_id, status, dataset, input_rows, wall_time_ms, output_bytes, failure_code, created_at, completed_at) ON "sandbox_executions" TO datalake_ro;
GRANT SELECT (id, enabled, min_input_rows, max_execution_ms, max_cpu_seconds, max_memory_mb, max_output_bytes, max_code_bytes, max_concurrent_runs, max_runs_per_hour, allowed_modules, created_at, updated_at) ON "sandbox_policy" TO datalake_ro;
GRANT SELECT (id, schedule_id, run_id, status, ran_at, created_at) ON "schedule_runs" TO datalake_ro;
GRANT SELECT (id, owner_user_id, agent_id, prompt, cadence, hour, minute, weekday, enabled, next_run_at, last_run_at, last_run_id, last_status, created_at, updated_at, report_definition_id, timezone, consecutive_failures, paused_reason, paused_at, last_error) ON "schedules" TO datalake_ro;
GRANT SELECT (id, script_id, version, code, parameters, author_user_id, created_at, tags, domain, guidance, return_contract, dependencies, verified_at) ON "script_versions" TO datalake_ro;
GRANT SELECT (id, owner_user_id, name, description, current_version, archived_at, created_at, updated_at, kind) ON "scripts" TO datalake_ro;
GRANT SELECT (user_id, team_id, source, joined_at) ON "team_members" TO datalake_ro;
GRANT SELECT (id, keycloak_group_path, display_name, slug, litellm_team_alias, litellm_team_id, created_at, updated_at) ON "teams" TO datalake_ro;
GRANT SELECT (id, base_name, description, category, risk, risk_reason, user_config_fields, created_at, updated_at) ON "tool_definitions" TO datalake_ro;
GRANT SELECT (id, conversation_id, user_id, category, granted_at, expires_at, revoked_at, source_excerpt) ON "tool_grants" TO datalake_ro;
GRANT SELECT (id, application_id, api_key_id, source, client_name, client_version, actor_label, actor_external_id, requested_model, actual_model, prompt, response, results, input_tokens, output_tokens, cost_eur, latency_ms, status, failure_code, metadata, created_at, conversation_id) ON "usage_events" TO datalake_ro;
GRANT SELECT (user_id, theme, locale, timezone, preferences, updated_at) ON "user_preferences" TO datalake_ro;
GRANT SELECT (user_id, role, source, assigned_at) ON "user_roles" TO datalake_ro;
GRANT SELECT (id, user_id, tool_base_name, variant, created_at, updated_at, confirm_before_use) ON "user_tool_configs" TO datalake_ro;
GRANT SELECT (id, keycloak_subject, username, display_name, email, status, litellm_end_user_id, first_seen_at, last_seen_at, last_login_at, created_at, updated_at) ON "users" TO datalake_ro;
