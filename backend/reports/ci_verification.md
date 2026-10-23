# Phase 6 CI verification

Status: **LOCAL PASS; HOSTED RUNS PENDING**. Checked 2026-10-21.

This report demonstrates local execution of the new CI entrypoints in an isolated application checkout. It does not claim a successful GitHub Actions run. Workflow activation and specific hosted pass/fail links require committing and pushing the changes.

| Check | Result |
|---|---|
| core | 91/91 passed; 0 skipped |
| postgres | 30/30 passed; 0 skipped |
| spark | 4/4 passed; 0 skipped |
| Browser fixture suite | 9/9 passed, no skipped/flaky tests |
| Frontend | fresh npm ci; route type generation; typecheck; production build passed |
| Workflow syntax | official SHA-verified actionlint 1.7.12 passed |
| Broken-source rejection | real Churn feature-leakage change failed with exit 1; disposable source restored |
| Kafka | real native broker; database-before-offset crash/replay/malformed checks passed |
| Credential scan | passed |

The checkout omitted raw/processed data, every saved split CSV, production model binaries, credentials and service state. Python checks used a fresh Linux 3.12.3 environment installed from the hash-locked CPU requirements. Node dependencies were installed into the copy from the existing offline npm cache; browser checks used Windows Edge and a newly fitted synthetic 120-customer model.

The local integration probe provisioned a separate PostgreSQL server on port 55433 and a separate Kafka 4.3.1 broker on port 19092. It tested the actual CI database provisioning entrypoint with generated roles and distinct application/test databases. Both owned services stopped afterward; normal application services/groups were not reset. Docker service images and hosted Chromium still require their hosted runs.

Kafka final logical predictions: 3; physical accepted deliveries: 9; rejected inputs: 5. Hard exits (86) and refused-database exits (1) are expected only in their named fault-injection stages.

The full-data workflow is provided separately with a required HTTPS CSV URL and the original SHA-256 guard. It was not executed in this CI checkpoint; earlier full-data evidence is separately documented.

See [CI job scopes and reproduction](../../docs/CI.md), [JSON results and source/artifact hashes](ci_verification.json), [local transcripts](../../docs/evidence/ci/20261021-local/), and [actual repository Actions runs](https://github.com/sathsararns/opsyntra/actions). There are currently no hosted run URLs for these unpushed workflows.
