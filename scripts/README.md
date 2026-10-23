# Local launchers

Windows and Ubuntu WSL launchers live in `windows/`. Run these examples from
the repository root:

```powershell
# Choose Docker Compose or the native Windows/WSL setup for a session.
.\scripts\windows\start-containers.ps1 -Task start
.\scripts\windows\start-containers.ps1 -Task demo -DemoEvents 20
.\scripts\windows\start-containers.ps1 -Task stop
```

For the native setup, install the prerequisites described in the linked guides:

| Launcher | Purpose | Guide |
|---|---|---|
| `start-frontend.ps1` | Next.js frontend and Python API | [Application setup](../README.md#start-the-application) |
| `start-mlflow.ps1` | Experiment tracking UI | [Backend instructions](../backend/README.md) |
| `start-spark.ps1` | Spark setup, training, inference and tests | [Backend instructions](../backend/README.md) |
| `start-airflow.ps1` | Airflow service and ML/Kafka DAG runs | [Airflow workflow](../backend/dags/README.md) |
| `start-kafka.ps1` | Kafka service, topics and checks | [Kafka guide](../backend/KAFKA.md) |
| `start-consumer.ps1` | Kafka prediction consumer using sklearn or Spark | [Kafka guide](../backend/KAFKA.md) |
| `start-analytics.ps1` | PostgreSQL setup, migrations and analytics tests | [Analytics storage](../docs/ANALYTICS_STORAGE.md) |
| `start-containers.ps1` | Docker Compose setup, lifecycle and verification | [Container guide](../docs/CONTAINERS.md) |

For example, start the native frontend from root:

```powershell
.\scripts\windows\start-frontend.ps1
```

If your terminal is inside `backend/`, use:

```powershell
..\scripts\windows\start-frontend.ps1
```

The launchers resolve the repository from their own location. Customer file,
model and state arguments keep their existing backend-relative meaning.
`start-containers.ps1` runs Compose from the repository root.

These files previously lived at the repository root. Use the new paths in this
guide; older captured logs and evidence manifests retain their historical paths
and hashes. Moving the launchers does not rerun those historical checks.
