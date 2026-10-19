# Opsyntra model selection

Selected **random_forest**, version `d9a13b4cf6839ed4`.

Hyperparameters were tuned with stratified training-only cross-validation. Models were ranked by validation average precision (AP). Each non-dummy model's threshold maximizes validation F1; ties prefer proximity to 0.5, then a higher threshold. The dummy keeps its most-frequent baseline rule. The test set was used only after selection.

| Model | Validation AP | Threshold | Precision | Recall | F1 |
|---|---:|---:|---:|---:|---:|
| dummy | 0.2654 | 0.500000 | 0.0000 | 0.0000 | 0.0000 |
| logistic_regression | 0.6505 | 0.330058 | 0.5425 | 0.7513 | 0.6300 |
| random_forest | 0.6626 | 0.276342 | 0.5140 | 0.8316 | 0.6353 |
| xgboost | 0.6522 | 0.375186 | 0.5746 | 0.7005 | 0.6313 |

The selected threshold is **0.276342**. Validation F1 changes from 0.5582 at the native decision rule to 0.6353. This prioritizes balancing precision and recall; it is not a business-cost optimization. Revisit the objective when retention costs are known.

Validation scores are selection evidence and can be optimistic. Final held-out test metrics:

- accuracy: 0.741661
- precision: 0.508532
- recall: 0.796791
- f1: 0.620833
- roc_auc: 0.843857
- average_precision: 0.658023
- confusion_matrix: [[747, 288], [76, 298]]

Average precision summarizes the precision–recall curve; it is not trapezoidal PR-AUC.

Dataset SHA256: `88be4b93fbe0cc83421af1c503794c97c342eca914c1576db7c276e61d61358a`.
Split SHA256: `b51f7b090312e8c727b11753dd740acdadad3e47d01c2261fe76fb5421b4e82c`.
MLflow experiment: `Opsyntra-Telco-sklearn`. Batch: `9f64a429df7f42b3a72bedc1b8b29b20`.
Selected run: `b877c5c27d884d2fb81dab882ee74592`.

Plots: `plots/9f64a429df7f42b3a72bedc1b8b29b20/random_forest/`. UI screenshots: `../../docs/screenshots/mlflow-*.png`.
