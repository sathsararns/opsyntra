# Shared project documentation

- [Project roadmap](../OPSYNTRA_ROADMAP.md)
- [Frontend setup](../frontend/README.md)
- [Backend and ML workflows](../backend/README.md)
- [Desktop screenshot](screenshots/frontend-desktop.png)
- [Mobile screenshot](screenshots/frontend-mobile.png)
- [Customer prediction screenshot](screenshots/frontend-prediction.png)
- [MLflow completed runs](screenshots/mlflow-runs.png)
- [MLflow model comparison](screenshots/mlflow-comparison.png)
- [Selected model, threshold and test metrics](screenshots/mlflow-selected-model.png)
- [Model selection explanation](../backend/reports/model_selection.md)

`screenshots/` contains shared interface evidence. Machine-readable ML validation
and evaluation reports belong in `backend/reports/`.

The MLflow comparison shows four runs from the same training batch and saved
partitions. Random Forest has the highest validation average precision (0.6626).
Its validation-F1 threshold is 0.276342. The selected-run screenshot explains this
choice and shows the final test metrics. It catches more churners at the cost of
more false positives; the threshold is a starting F1 policy, not a cost model.
Missing test metrics for other candidates are intentional: test data did not
select the model or threshold.

`screenshots/mlflow-evidence.json` links the images to the exact batch, model
version, dataset/split hashes, run IDs, and test metrics. Images show the actual
local MLflow UI. Its run Duration covers artifact logging; `training_seconds`
records model training and CV time.

To refresh evidence after training, start `.\start-mlflow.ps1` from the repository
root, then run in another terminal:

```powershell
cd frontend
npm run evidence:mlflow
```

The capture script uses installed Microsoft Edge on Windows and the current
`backend/reports/model_training_report.json`. MLflow models, Joblib pipelines,
confusion matrices, ROC/PR curves, threshold searches and configuration are
available under each run's Models/Artifacts views. See the current selection
report for the batch-specific plot directory.
