# Step 1: Telco dataset overview

The source CSV was imported unchanged from the supplied archive.zip. Its exact
SHA-256 checksum is recorded in data_validation_report.json and the split manifest.

| Property | Observed value |
|---|---:|
| Customers | 7,043 |
| Columns including identifier and target | 21 |
| Model input columns | 19 |
| Churn = Yes | 1,869 (26.5370%) |
| Churn = No | 5,174 (73.4630%) |
| Duplicate customer IDs | 0 |
| Blank TotalCharges | 11 |
| Other missing values | 0 |

All 11 customers with blank TotalCharges have tenure = 0. The normalized data
stores these values as numeric missing values and retains the customers. No
imputation has been fitted; the next phase will learn a median inside the
training/CV pipeline. This keeps validation and test information out of training.

| Partition | Customers | Churn = Yes | Churn = No | Churn % | Missing TotalCharges |
|---|---:|---:|---:|---:|---:|
| Train | 4,225 | 1,121 | 3,104 | 26.5325 | 8 |
| Validation | 1,409 | 374 | 1,035 | 26.5436 | 1 |
| Test | 1,409 | 374 | 1,035 | 26.5436 | 2 |

The configured 60/20/20 split uses seed 42. Each customer occurs in exactly one
partition. Rounding accounts for the fractional row counts. The authoritative
customerID-to-split map is data/splits/memberships.csv: sklearn and Spark must join
this map rather than generating their own independent random splits.

customerID identifies predictions and Churn is the target; neither is a predictor.
The remaining 19 fields contain four numeric features and 15 categorical features.
Event metadata will also be excluded from model inputs when streaming is added.

The majority-class baseline would predict No for everybody. Its full-dataset
accuracy is 73.4630%, illustrating why accuracy alone will be insufficient for
model comparison. Baseline model evaluation will use the actual held-out split in
the next phase.

This overview describes the supplied dataset with the default configuration. The
JSON report and manifest are the generated evidence for each pipeline run.
