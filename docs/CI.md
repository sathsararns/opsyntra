# GitHub Actions CI

Phase 6 provides three workflows. Local verification is retained in
[the CI verification report](../backend/reports/ci_verification.md).
The first hosted GitHub run still requires committing and pushing these files;
local checks do not establish that a GitHub-hosted runner has passed.

[Actual repository Actions runs](https://github.com/sathsararns/opsyntra/actions)
currently have no run for these unpushed workflows. After pushing, select
**Opsyntra CI** to inspect each job and download its result artifacts. The
repository's default branch is `master`.

## What runs

| Workflow | Trigger | Scope |
|---|---|---|
| `.github/workflows/ci.yml` | Push, pull request, manual | Synthetic backend checks; frontend typecheck/production build and browser flows; PostgreSQL grants/API/views and real Kafka recovery |
| `.github/workflows/spark.yml` | Manual and Monday 02:17 UTC | All four native Spark tests using synthetic training fixtures, Java 21 and a separate locked dependency file |
| `.github/workflows/full-data.yml` | Manual, with required dataset URL | Original verified CSV, sklearn/native Spark rebuilding and comparison, real Kafka-to-browser visibility/SQL checks, executed notebooks |

The normal workflow needs no dataset download, production model, AWS credentials
or repository secrets. It creates a synthetic 120-customer browser fixture in
`backend/artifacts/ci/fixture/`. That test-only API serves the existing real HTTP
prediction routes with its tiny saved model; the production API configuration
and saved artifacts are unchanged. Analytics polling/outage tests use labelled
browser fixtures. The separate full-data workflow exercises an actual producer,
model prediction consumer, database sink and browser together.

Backend profiles explicitly select their scope and fail on **any skipped check**,
an empty suite, import/setup errors or test failures. The normal core profile
excludes database/JVM checks because those have explicit jobs; it does not hide
failed prerequisites with skips. The browser fixture suite likewise excludes
the one real-stream acceptance test assigned to the full-data job. Browser
retries are disabled so a flaky pass cannot conceal the first failure.

## Dependencies and job outputs

Linux jobs use CPython 3.12 on `ubuntu-24.04`, Node 22 for the frontend, and Java
21 for Spark. Python dependency files contain exact transitive versions and
SHA-256 hashes, installed with `pip --require-hashes`. The Linux CPU XGBoost
distribution supplies the same `xgboost` module without GPU runtime downloads.
The frontend installs with `npm ci` using its committed package lock. Next route
types are generated before TypeScript checks, so a clean checkout does not
depend on `.next/dev` files from a developer's machine.

Official actions are pinned to verified revision hashes. PostgreSQL 17.6 and
Apache Kafka 4.3.1 run as disposable GitHub service containers with readiness
checks. Provisioning creates two distinct databases and separate owner, writer
and reader logins, then applies the real migrations. It refuses to reset/reuse
existing databases. Generated credentials are masked and carried through
`GITHUB_ENV`, not written into tracked files or test artifacts.

Jobs upload JSON/JUnit/text results, browser reports and available traces or
failure screenshots with `if: always()` and 14-day retention. No AWS deployment
or container publishing occurs. Tokens have read-only repository permissions;
checkout credentials are not persisted. A future deployment job can use a
separate trusted trigger and OIDC role when the cloud phases are implemented.

## Run equivalent checks locally

Core checks use synthetic inputs and temporary trained models. From `backend/`
in an environment with the normal web, Kafka and analytics dependencies:

```powershell
python -m pipelines.ci_checks --profile core
```

Database checks need the separately provisioned test database from Phase 2.
Kafka checks use that test database and isolated diagnostic topics/groups:

```powershell
# From repository root, start existing services first.
.\start-analytics.ps1 -Task start
.\start-kafka.ps1 -Task start
cd backend
python -m pipelines.ci_checks --profile postgres
python -m pipelines.ci_kafka
```

Native Spark runs on Linux with the locked Spark dependencies and Java 21:

```bash
# Working directory: backend/
python -m pipelines.ci_checks --profile spark
```

The fresh-process Spark inference test now fits and saves its own small model;
it no longer skips when an ignored production pipeline is missing. The database
permission test discovers the configured owner login instead of requiring one
particular account name. The real recovery diagnostic retries transient cold
Kafka coordinator errors for at most 20 seconds; unexpected errors still fail.

For browser checks, start with a new fixture directory:

```powershell
# Working directory: backend/
python -m pipelines.ci_prepare --directory artifacts/ci/fixture
cd ../frontend
npm ci
npx next typegen
npm run typecheck
npm run build
npx playwright test --config playwright.ci.config.ts
```

The fixture command refuses a nonempty destination. Preserve an existing fixture
or choose a new isolated checkout for a rerun. The browser config expects its
fixture at `backend/artifacts/ci/fixture/` and starts fresh servers on ports
8011/3011; override `OPSYNTRA_TEST_API_PORT` and `OPSYNTRA_TEST_WEB_PORT` if those
are occupied. Windows uses installed Edge; GitHub installs Chromium with
`npx playwright install --with-deps chromium`. Results go to
`backend/artifacts/ci/reports/`, `frontend/test-results/` and
`frontend/playwright-report/`, all ignored by Git.

## Full-data workflow

After committing/pushing, choose **Opsyntra full-data verification → Run
workflow**, then enter a public HTTPS URL that returns the original CSV bytes.
The job requires SHA-256:

```text
88be4b93fbe0cc83421af1c503794c97c342eca914c1576db7c276e61d61358a
```

Use the [documented assignment dataset](VERIFICATION.md#clean-checkout). The URL
must return a CSV, not a ZIP, HTML login page or a reformatted CSV. A missing URL,
non-HTTPS redirect, oversized response or checksum mismatch fails the job; the
workflow does not turn that into a skip. URLs cannot contain login credentials.
It rebuilds models inside the disposable checkout rather than copying models
from a developer workstation. The normal PR workflow remains self-contained.

The full-data workflow is defined but has not been run on GitHub in this
checkpoint. Earlier Phase 1/4/5 reports remain separately labelled evidence for
their full-data local checks. Hosted full-data timing and Docker service behavior
must be evaluated from the resulting GitHub run, not inferred from native WSL.

## Updating locks and action pins

These lockfiles were resolved with uv 0.10.12 for Linux CPython 3.12. Rebuild
them together when deliberately updating package inputs, then rerun each profile:

```powershell
python -m uv pip compile backend/requirements/ci.in --python-version 3.12 --python-platform x86_64-unknown-linux-gnu --generate-hashes --no-header --output-file backend/requirements/ci-py312-linux.lock
python -m uv pip compile backend/requirements/ci-spark.in --python-version 3.12 --python-platform x86_64-unknown-linux-gnu --generate-hashes --no-header --output-file backend/requirements/ci-spark-py312-linux.lock
```

Verify new action revisions against their official repositories:
[checkout](https://github.com/actions/checkout),
[setup-python](https://github.com/actions/setup-python),
[setup-node](https://github.com/actions/setup-node),
[setup-java](https://github.com/actions/setup-java), and
[upload-artifact](https://github.com/actions/upload-artifact).
Service-container behavior follows [GitHub's PostgreSQL guidance](https://docs.github.com/en/actions/tutorials/use-containerized-services/create-postgresql-service-containers)
and [Apache's Kafka Docker documentation](https://kafka.apache.org/43/getting-started/docker/).

## Failure proof and hosted completion

The retained local proof adds `Churn` to predictors only in a disposable checkout.
The real data-contract test fails and the CI runner returns exit 1; original
source bytes are restored afterward. Valid code passes all selected checks.
The main working tree was not mutated for that experiment. The local proof uses
native isolated services, not Docker service containers or a GitHub runner.

For the full Phase 6 checkpoint, push a review branch, confirm the valid hosted
workflow passes, demonstrate the same rejected change in an isolated branch/run,
restore that branch, and retain both actual run URLs. Do not merge the deliberate
failure. Link those specific results from the README/report when available.
