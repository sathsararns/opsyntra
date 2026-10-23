import json
import os
import subprocess
import urllib.request
from pathlib import Path

root = Path(__file__).resolve().parents[2]
project = "opsyntra-verify-20261021064526"
prefix = ["docker", "compose", "--env-file", str(root / ".verification/containers" / project / ".env"), "-p", project, "--profile", "airflow"]
directory = root / "docs/evidence/containers/runtime"
code = '''
import json, os, subprocess
from uuid import uuid4
from pathlib import Path
from airflow.dag_processing.dagbag import DagBag
bag=DagBag(dag_folder="/app/backend/dags")
assert not bag.import_errors, str(list(bag.import_errors))
expected={"opsyntra_ml_workflow","opsyntra_kafka_batch","opsyntra_kafka_streaming"}
assert expected <= set(bag.dag_ids), str(bag.dag_ids)
for dag in bag.dags.values():
 for task in dag.tasks:
  assert task.env["OPSYNTRA_ML_PYTHON"]=="/opt/opsyntra-ml/bin/python"
env={**os.environ,"OPSYNTRA_RUN_ID":"container-runtime-probe-"+uuid4().hex,"OPSYNTRA_ENGINE":"sklearn"}
for task in ("validate_data","prepare_splits"):
 result=subprocess.run([env["OPSYNTRA_ML_PYTHON"],"-m","pipelines.airflow_task","--task",task],env=env,capture_output=True,text=True,timeout=120)
 if result.returncode: raise RuntimeError(result.stdout+result.stderr)
probe={"status":"PASS","dag_ids":sorted(bag.dag_ids),"tasks":sum(len(d.tasks) for d in bag.dags.values()),"ml_python":env["OPSYNTRA_ML_PYTHON"],"task_probes":["validate_data","prepare_splits"],"run_id":env["OPSYNTRA_RUN_ID"],"shared_artifacts":str(Path("/app/backend/artifacts/airflow").exists())}
print("OPSYNTRA_PROBE_JSON="+json.dumps(probe))
'''
result = subprocess.run([*prefix,"exec","-T","airflow","python","-"],input=code,cwd=root,text=True,capture_output=True,timeout=300)
(root / ".verification/containers/airflow-probe-private.txt").write_text(result.stdout+result.stderr,encoding="utf-8")
if result.returncode:
    raise RuntimeError("Airflow runtime probe failed; inspect private log")
line=next(x for x in result.stdout.splitlines() if x.startswith("OPSYNTRA_PROBE_JSON="))
report=json.loads(line.split("=",1)[1])
with urllib.request.urlopen("http://127.0.0.1:1495/api/v2/monitor/health",timeout=15) as response:
    report["health"]=json.load(response)
(directory / "airflow.json").write_text(json.dumps(report,indent=2))
print(json.dumps(report,indent=2))
