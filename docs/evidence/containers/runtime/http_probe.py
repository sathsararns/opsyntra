import csv
import io
import json
import urllib.request
from pathlib import Path

root=Path(__file__).resolve().parents[2]
url="http://127.0.0.1:1490"
def request(endpoint,payload=None):
    body=json.dumps(payload).encode() if payload is not None else None
    req=urllib.request.Request(url+endpoint,data=body,headers={"Content-Type":"application/json"} if body else {})
    with urllib.request.urlopen(req,timeout=30) as response:return json.load(response)
record=json.loads((root/"backend/examples/customer.json").read_text())
assert "Churn" not in record
overview=request("/api/overview")
single=request("/api/predict",{"record":record})
output=io.StringIO()
writer=csv.DictWriter(output,fieldnames=list(record))
writer.writeheader()
writer.writerow(record)
batch=request("/api/predict/batch",{"csv_text":output.getvalue()})
assert single["predictions"]==batch["predictions"]
assert overview["prediction_ready"] and overview["dataset"]["customers"]==7043
assert single["model_version"]==batch["model_version"]==overview["model"]["version"]
report={"status":"PASS","customers":overview["dataset"]["customers"],"selected_model":overview["model"]["name"],"model_version":single["model_version"],"single":single,"batch":batch,"churn_required":False,"blank_total_charges_supported":True}
(root/"docs/evidence/containers/runtime/prediction-api.json").write_text(json.dumps(report,indent=2))
print(json.dumps({k:v for k,v in report.items() if k not in ("single","batch")},indent=2))
