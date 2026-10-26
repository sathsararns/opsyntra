# Backend workflow/tools refactor

**PASS - 2026-10-26 (Asia/Colombo).** The backend now separates its core ML
workflow commands from supporting service, CI, verification and evidence tools.

`backend/pipelines/` changed from 40 Python files to **9**: eight workflow
entrypoints plus `__init__.py`. Supporting modules were relocated as follows:

| Package | Modules moved | Responsibility |
|---|---:|---|
| `tools.ci` | 5 | CI fixtures and strict test jobs |
| `tools.verification` | 9 | Runtime, recovery, notebook and clean-clone checks |
| `tools.evidence` | 7 | Tracking, evidence reports and model-bundle handoff |
| `tools.services` | 10 | Service operations, setup, inspection and UI snapshot |

The previous `pipelines.verification` runner is now
`tools.verification.runner`. See the [complete command migration table](../tools/README.md).
Main data/training/inference commands and Windows launcher paths are unchanged.

Root resolution, cross-tool imports, test mocks, browser helper paths, GitHub
workflow commands and documentation were updated. Docker includes `tools/`
through its build allowlist and backend image COPY instruction. The reusable
package remains `opsyntra`; saved model class paths and artifact/data/recovery
locations are preserved.

| Validation | Observed result |
|---|---|
| Strict native backend core profile | **95/95 passed**, zero skipped |
| Frontend production build and typecheck | Passed |
| CI browser profile with relocated synthetic API | **9/9 passed**, zero skipped; includes polling, outage and mobile scenarios |
| Native tool imports/root paths | 30 Windows-compatible modules passed; PostgreSQL service helper is Linux-only |
| Rebuilt Linux image tool imports/root paths | All **31** modules passed, including the Linux-only helper |
| Representative CLI help | CI, model bundle, clean-clone and Kafka inspection entrypoints passed |
| Docker core | Six services healthy; initialization jobs succeeded |
| Persisted model inference | Raw single/CSV predictions identical; blank TotalCharges supported |
| Real bounded Kafka flow | Three acknowledged raw events created exactly three new raw rows and three logical SQL predictions |
| Saved model integrity | Model checksum unchanged during the Kafka smoke check |
| Final cleanup | Validation containers stopped; named volumes retained; regenerated test fixtures/caches removed after archiving results |

The browser profile uses synthetic model fixtures and controlled analytics
responses. The Docker check separately uses the real saved model, Kafka and
PostgreSQL. Those scopes are not interchangeable.

Evidence: [machine report](structure_refactor_verification.json),
[core test JSON](../../docs/evidence/structure-refactor/20261026/core.json),
[browser JSON](../../docs/evidence/structure-refactor/20261026/ci-browser.json),
[browser log](../../docs/evidence/structure-refactor/20261026/browser.txt),
[real Docker/Kafka result](../../docs/evidence/structure-refactor/20261026/docker.json).

No GitHub-hosted CI run, commit or push was performed. Full Spark training and
the eight-task Airflow scheduler run were not repeated; core workflow/task
contracts passed. Fresh clean-clone acceptance requires a commit containing
the new tools layout. Historical manifests and hashes retain their original
tested paths; the submission index's source links follow the current layout.
