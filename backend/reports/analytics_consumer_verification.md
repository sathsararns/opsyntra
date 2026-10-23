# Phase 3: Kafka analytics verification

Status: **PASS**. Contract/PostgreSQL checks: 36/36; skipped: 0.

The live check uses real Kafka and a separate PostgreSQL test database. It never loads an ML model. Diagnostic topics/groups and model IDs are recorded in the JSON report.

Published inputs: **14**. Final logical predictions: **3**; physical accepted deliveries: **9**; durable rejected inputs: **5**.

The fixture contains three raw event identities, three prediction identities (two sklearn and one Spark), one incoming dead letter, duplicate deliveries, one conflicting event and four malformed inputs. Logical counts stay unchanged on replay.

| Real process scenario | Expected exit | Evidence |
|---|---|---|
| crash-after-database-commit | 86 | [transcript](../../docs/evidence/analytics/20261020T094643Z-9c668674/crash-after-database-commit.txt) |
| restart-after-commit | 0 | [transcript](../../docs/evidence/analytics/20261020T094643Z-9c668674/restart-after-commit.txt) |
| known-event-snapshot | 0 | [transcript](../../docs/evidence/analytics/20261020T094643Z-9c668674/known-event-snapshot.txt) |
| new-group-full-replay | 0 | [transcript](../../docs/evidence/analytics/20261020T094643Z-9c668674/new-group-full-replay.txt) |
| completed-group-restart | 0 | [transcript](../../docs/evidence/analytics/20261020T094643Z-9c668674/completed-group-restart.txt) |
| crash-before-database-commit | 86 | [transcript](../../docs/evidence/analytics/20261020T094643Z-9c668674/crash-before-database-commit.txt) |
| restart-before-commit | 0 | [transcript](../../docs/evidence/analytics/20261020T094643Z-9c668674/restart-before-commit.txt) |
| database-unavailable | 1 | [transcript](../../docs/evidence/analytics/20261020T094643Z-9c668674/database-unavailable.txt) |
| bounded-streaming | 0 | [transcript](../../docs/evidence/analytics/20261020T094643Z-9c668674/bounded-streaming.txt) |

Exit 86 is the deliberately injected hard crash. Exit 1 in the database-unavailable stage is the expected refusal with no input commit. Other stages must exit 0.

The after-database-commit crash stores one prediction before exiting; Kafka offsets remain unchanged. Restarting the same group replays it without another logical row. A new group replays all inputs without count inflation. The before-commit crash rolls back its new raw event; restart inserts it once. Late raw features and previous-day predictions repair the event-based joins/time buckets.

## Contract/database checks

| Test | Result |
|---|---|
| test_configuration_requires_credentials_and_hides_them | passed |
| test_deadletter_corrupt_bytes_and_nonobject_errors_are_rejected | passed |
| test_deadletter_matches_existing_inference_contract | passed |
| test_logical_hash_excludes_retry_processing_time_but_not_probability | passed |
| test_missing_total_charges_preserved_and_source_validated | passed |
| test_wire_validation_rejects_bad_keys_versions_probabilities_and_times | passed |
| test_commit_is_synchronous_and_checks_partition_errors | passed |
| test_options_reject_unbounded_batch_bad_limits_and_inference_group | passed |
| test_readiness_failure_does_not_create_kafka_client | passed |
| test_retention_loss_is_not_reset_to_earliest | passed |
| test_retries_are_bounded_and_stop_interrupts_backoff | passed |
| test_runtime_needs_only_write_credentials | passed |
| test_concurrent_duplicate_inserts_have_one_logical_winner | passed |
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
| test_ambiguous_database_commit_retries_idempotently_before_input_commit | passed |
| test_before_commit_failure_rolls_back_facts_and_quarantine | passed |
| test_changed_quarantined_or_accepted_coordinates_stop_without_new_facts | passed |
| test_database_failure_never_commits_inputs | passed |
| test_graceful_stop_finishes_current_database_and_input_commit | passed |
| test_input_commit_observes_durable_database_rows_and_auto_commit_disabled | passed |
| test_late_raw_and_prediction_recompute_old_time_buckets | passed |
| test_malformed_tombstones_oversized_and_conflicts_are_durable_rejections | passed |

See [machine-readable results](analytics_consumer_verification.json), [test log](analytics_consumer_verification.log),
[full regression log](analytics_consumer_regression.log) when requested, and [operating/recovery guide](../../docs/ANALYTICS_CONSUMER.md).

The live diagnostic tests do not reset the application/inference groups. Delivery is at-least-once with idempotent PostgreSQL facts, not an atomic exactly-once transaction across systems.
