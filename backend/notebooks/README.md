# Notebooks

Three executed notebooks are available:

1. [Training-only Telco EDA](01_telco_eda.ipynb): class balance, numeric/missing
   values and contract/tenure associations using persisted training memberships.
2. [Saved model comparison](02_model_comparison.ipynb): validation selection and
   thresholds, verified ROC/PR curves, recorded final test assessment and honest
   sklearn/Spark timing. No retraining or test-set tuning.
3. [Streaming analytics](03_streaming_analytics.ipynb): read-only PostgreSQL
   snapshot, UTC hourly trends, original-event segments, risk and late/duplicate
   delivery analysis. Synthetic replay is a simulation of stream processing.

From `backend/`, install `requirements/notebooks.txt`, then run
`python -m pipelines.notebook_check`. This executes all cells in fresh kernels,
verifies inputs/exports, checks SQL against dashboard metrics and saves outputs.

Figures/tables/manifests go to `reports/notebooks/<notebook-name>/`.
See [setup, required artifacts and snapshot scope](../../docs/NOTEBOOKS.md)
and [actual verification](../reports/notebook_verification.md).
