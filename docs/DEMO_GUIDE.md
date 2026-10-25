# Opsyntra local Docker demo

Final presentation scope confirmed on 2026-10-25 (Asia/Colombo): run Opsyntra
locally with Docker Compose. AWS S3/RDS/ECS and QuickSight remain deferred future
work. No extra TensorFlow/Terraform integration is needed for this delivery.

## Before the presentation

Run commands from the repository root, `Opsyntra/`. From `backend/`, prefix
launcher paths with `..\scripts\windows\` instead of `.\scripts\windows\`.

1. Install Docker Desktop and provide the original CSV at
   `backend/data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv`. Python 3 is required
   for the launcher setup helper. Initial image downloads need internet access.
2. Start the core application:

   ```powershell
   .\scripts\windows\start-containers.ps1 -Task start
   .\scripts\windows\start-containers.ps1 -Task status
   ```

   The launcher starts the local Docker Desktop Linux engine when needed, waits
   for readiness, prepares or preserves root `.env`, and starts the core stack.
   Six running services should be healthy: frontend, API, Kafka, PostgreSQL,
   inference and analytics. The three setup jobs (`model-init`, `db-init`,
   `topics-init`) should finish with exit code 0. Their `Exited` state is expected.
3. Open <http://127.0.0.1:3000>. These are the default ports; use the configured
   frontend port if root `.env` overrides it. See [container setup](CONTAINERS.md).
4. Keep the [submission evidence matrix](../backend/reports/submission_evidence.md),
   [model comparison](../backend/reports/engine_comparison.md) and
   [container acceptance evidence](../backend/reports/container_verification.md)
   ready. Recorded evidence has its own date/model/run identity.

The core includes its own Kafka and PostgreSQL, Python API, both consumers and
Next.js frontend. Use the Docker commands for this session. The separate native
Windows/WSL launchers remain an alternative described in [verification](VERIFICATION.md).

## Presentation sequence

| Part | Show | Explain |
|---|---|---|
| Overview | Customer count, data quality and model status | 7,043 source customers, 11 blank TotalCharges values and fixed 60/20/20 memberships |
| Model selection | Model performance and recorded MLflow comparison | Validation AP selects the model; validation F1 selects its threshold; held-out test metrics are the assessment |
| Single prediction | Customer prediction -> Load example -> Predict churn risk | The saved complete Python pipeline handles raw features without Churn |
| CSV prediction | Batch predictions -> upload sample -> Run batch prediction -> Download results | The same pipeline handles the sample CSV and supports blank TotalCharges |
| Live monitoring | Publish 20 bounded events and watch recent stream predictions | Producer -> Kafka -> saved-model consumer -> prediction topic -> analytics consumer -> PostgreSQL -> API -> browser |
| Traceability | Expand a recent prediction | Customer ID, event ID, prediction ID, engine and model version identify its source |
| Reliability | Stop/start persistence and retained recovery evidence | Named volumes preserve data/models; stable logical IDs support deduplication under documented at-least-once replay |
| Assessed workflows | Spark comparison and successful Airflow Grid/Graph evidence | Both engines share held-out customers; eight-task ML DAGs and bonus Kafka DAGs have recorded successful runs |

Direct customer and CSV predictions call the Python API. **Live monitoring**
reads Kafka-derived predictions stored in PostgreSQL. The overview also shows
saved dataset/model reports; it is not the live event count.

## Publish the live demo

Keep the browser on **Live monitoring**, then run:

```powershell
.\scripts\windows\start-containers.ps1 -Task demo -DemoEvents 20
```

The bounded producer acknowledges 20 new events and exits. The inference and
analytics consumers keep running. A producer acknowledgement proves Kafka
received the input; verify that recent predictions then appear in the dashboard.
The browser polls every five seconds by default. Old runs, time filters, late
arrivals and other active producers affect the total; a reused database need not
show exactly 20 overall. Keep engine/model/time filters consistent when comparing.

The source CSV is replayed synthetically. Prediction volume, mean risk and
predicted churn share are model outputs, not measured company churn outcomes.
A churn prediction can be Yes below 50% because it uses the saved validation
threshold. Customer values such as `2931-VUVJN` are dataset IDs, not names.

For a longer feed, run this in a separate terminal and press Ctrl+C when done:

```powershell
docker compose run --rm --no-deps producer producer --mode streaming --events-per-sec 5 --exclude-churn
```

For a batch/checkpoint demonstration:

```powershell
docker compose run --rm --no-deps producer producer --mode batch --batch-size 500 --exclude-churn
# Repeat the same command to resume the saved checkpoint.
```

## Stop and resume

```powershell
.\scripts\windows\start-containers.ps1 -Task stop
# Later: recreate containers using the retained volumes.
.\scripts\windows\start-containers.ps1 -Task start
```

Normal stop removes containers/network while retaining the named volumes for
Kafka, PostgreSQL, models, reports, MLflow and the SQLite inference outbox. Do not
use the reset command or `down --volumes` for an ordinary demo shutdown.

On a later day the default last-24-hours dashboard can be empty even when older
rows remain stored. Publish a new bounded set or choose the appropriate time
window. After inspecting the resumed app, stop it again when finished.

## Evidence and assessed workflows

Capture overview/model, single prediction, CSV results and live monitoring
screenshots. Record the time, model version, filter window and producer run ID
with the live evidence. Keep the SQL/API agreement and prior crash/replay logs.
The [container acceptance report](../backend/reports/container_verification.md)
covers real browser/SQL checks, service restart, container recreation and replay.
To repeat the full isolated acceptance check:

```powershell
.\scripts\windows\start-containers.ps1 -Task test
```

That check needs frontend Playwright/Edge prerequisites and creates its own
Compose project; see [acceptance setup](CONTAINERS.md#reproduce-acceptance-checks).
It is a verification step, not a command needed before each presentation.

MLflow, Spark and Airflow are optional Docker profiles, with startup commands
in [the container guide](CONTAINERS.md#optional-heavy-services). Existing complete
Spark/Airflow scheduler evidence came from the documented WSL runtime. The
Docker Airflow report covers profile health/imports and two task probes; it does
not claim a new complete eight-task scheduler run inside Docker.

For an optional live WSL orchestration demonstration, stop the Docker core
first and follow [native verification](VERIFICATION.md) and
[Airflow operations](../backend/dags/README.md). Kafka brokers and model stores
in the two setups are separate. Do not mix their counts or model versions.

## Submission handoff

- [ ] Record the repository URL and the tested commit/version.
- [ ] Keep dataset acquisition/checksum and fresh setup instructions available.
- [ ] Review the dated clean-clone, frontend, MLflow, Airflow and recovery reports.
- [ ] Capture the final local UI screens with event/model traceability.
- [ ] Explain validation selection, Spark timing, synthetic replay and recovery limits.
- [ ] Commit reviewed source/docs; keep `.env`, raw data and generated models excluded.
- [ ] After a separately requested push, check actual GitHub Actions results.

Hosted CI is still pending in the recorded report; a local demo pass does not
claim a hosted CI pass. Cloud deployment is deferred, not a remaining local
presentation requirement. The [single roadmap](../OPSYNTRA_ROADMAP.md) records
these scopes separately.
