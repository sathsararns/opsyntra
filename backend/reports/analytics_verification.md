# Phase 2: analytics storage verification

Status: **FAIL**. 21/22 tests passed; 0 skipped.

PostgreSQL: PostgreSQL 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0, 64-bit. Driver: psycopg 3.3.6.

Tests ran against the separately provisioned `opsyntra_analytics_test` database; the main analytics database receives migrations only.

Verified schema reruns/checksums, duplicate and conflicting logical IDs, source provenance conflicts, transaction rollback, concurrent inserts,
both arrival orders, original event profiles, model/engine-separated metrics, UTC timestamps and actual read/write database grants.

| Test | Result |
|---|---|
| test_configuration_requires_credentials_and_hides_them | passed |
| test_deadletter_corrupt_bytes_and_nonobject_errors_are_rejected | passed |
| test_deadletter_matches_existing_inference_contract | passed |
| test_logical_hash_excludes_retry_processing_time_but_not_probability | passed |
| test_missing_total_charges_preserved_and_source_validated | passed |
| test_wire_validation_rejects_bad_keys_versions_probabilities_and_times | passed |
| test_concurrent_duplicate_inserts_have_one_logical_winner | error |
| test_conflicting_prediction_and_model_alias_are_rejected | passed |
| test_coordinate_conflict_rolls_back_entire_new_logical_event | passed |
| test_database_enforces_probability_foreign_keys_and_labels | passed |
| test_deadletter_replay_and_conflicting_error_are_separate_from_predictions | passed |
| test_distinct_models_engines_and_event_counts_are_not_customer_counts | passed |
| test_failed_migration_rolls_back_ddl_and_version | passed |
| test_logical_duplicates_keep_first_processing_time_and_count_deliveries | passed |
| test_migrations_rerun_and_detect_checksum_changes | passed |
| test_prediction_first_then_raw_joins_by_original_event | passed |
| test_raw_and_prediction_customer_or_time_conflicts_in_either_order | passed |
| test_raw_duplicates_and_conflicting_features | passed |
| test_reader_cannot_write_and_writer_cannot_update_delete_or_migrate | passed |
| test_repeated_customer_uses_event_profile_not_latest_profile | passed |
| test_transaction_rollback_removes_fact_identity_and_provenance | passed |
| test_utc_storage_keeps_timezone_display_out_of_the_fact | passed |

See [machine-readable results](analytics_verification.json), [test transcript](analytics_verification.log),
and [schema/setup/metric definitions](../../docs/ANALYTICS_STORAGE.md).

Phase 3 will connect Kafka to this store; Phase 4 will expose analytics APIs and the dashboard. These are not implemented by this checkpoint.
