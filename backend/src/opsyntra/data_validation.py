"""Validate the Telco contract and apply deterministic type conversion only."""

from pathlib import Path

import numpy as np
import pandas as pd

IDENTIFIER = "customerID"
TARGET = "Churn"
NUMERIC_COLUMNS = ("SeniorCitizen", "tenure", "MonthlyCharges", "TotalCharges")
YES_NO_COLUMNS = (
    "Partner", "Dependents", "PhoneService", "PaperlessBilling",
)
INTERNET_OPTIONS = ("No", "Yes", "No internet service")
CATEGORICAL_VALUES = {
    "gender": ("Female", "Male"),
    **{column: ("No", "Yes") for column in YES_NO_COLUMNS},
    "MultipleLines": ("No", "Yes", "No phone service"),
    "InternetService": ("DSL", "Fiber optic", "No"),
    **{column: INTERNET_OPTIONS for column in (
        "OnlineSecurity", "OnlineBackup", "DeviceProtection", "TechSupport",
        "StreamingTV", "StreamingMovies",
    )},
    "Contract": ("Month-to-month", "One year", "Two year"),
    "PaymentMethod": (
        "Electronic check", "Mailed check", "Bank transfer (automatic)",
        "Credit card (automatic)",
    ),
    TARGET: ("No", "Yes"),
}
EXPECTED_COLUMNS = (
    IDENTIFIER, "gender", "SeniorCitizen", "Partner", "Dependents", "tenure",
    "PhoneService", "MultipleLines", "InternetService", "OnlineSecurity",
    "OnlineBackup", "DeviceProtection", "TechSupport", "StreamingTV",
    "StreamingMovies", "Contract", "PaperlessBilling", "PaymentMethod",
    "MonthlyCharges", "TotalCharges", TARGET,
)
FEATURE_COLUMNS = tuple(column for column in EXPECTED_COLUMNS if column not in (IDENTIFIER, TARGET))


class DataValidationError(ValueError):
    def __init__(self, report: dict):
        self.report = report
        super().__init__("Data validation failed: " + "; ".join(report["errors"]))


def read_raw_data(path: Path) -> pd.DataFrame:
    # Preserve literal values, including whitespace-only TotalCharges entries.
    return pd.read_csv(path, dtype="string", keep_default_na=False)


def validate_and_normalize(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    report = {
        "status": "PASS", "rows": len(raw), "columns": len(raw.columns),
        "errors": [], "warnings": [], "checks": {},
    }
    errors = report["errors"]
    missing_columns = sorted(set(EXPECTED_COLUMNS) - set(raw.columns))
    extra_columns = sorted(set(raw.columns) - set(EXPECTED_COLUMNS))
    if missing_columns or extra_columns or raw.columns.duplicated().any():
        errors.append(f"Schema mismatch: missing={missing_columns}, extra={extra_columns}, duplicate_headers={bool(raw.columns.duplicated().any())}")
    if raw.empty:
        errors.append("Dataset is empty.")
    if errors:
        report["status"] = "FAIL"
        raise DataValidationError(report)

    clean = raw.loc[:, list(EXPECTED_COLUMNS)].copy()
    for column in clean:
        clean[column] = clean[column].astype("string").str.strip().replace("", pd.NA)
    missing = {column: int(clean[column].isna().sum()) for column in clean}
    report["missing_values"] = missing
    for column, count in missing.items():
        if count and column != "TotalCharges":
            errors.append(f"{column}: {count} missing values are not permitted.")

    duplicates = int(clean[IDENTIFIER].duplicated().sum())
    report["duplicate_customer_ids"] = duplicates
    report["checks"]["unique_customer_ids"] = duplicates == 0
    if duplicates:
        errors.append(f"Found {duplicates} duplicate customer IDs.")

    for column in NUMERIC_COLUMNS:
        converted = pd.to_numeric(clean[column], errors="coerce")
        invalid = int((clean[column].notna() & converted.isna()).sum())
        numeric = converted.to_numpy(dtype=float, na_value=np.nan)
        if invalid:
            errors.append(f"{column}: {invalid} invalid numeric values.")
        if np.isinf(numeric).any():
            errors.append(f"{column}: infinite values are not permitted.")
        if (converted < 0).any():
            errors.append(f"{column}: negative values are not permitted.")
        if column in ("SeniorCitizen", "tenure"):
            if ((converted.dropna() % 1) != 0).any():
                errors.append(f"{column}: values must be integers.")
            else:
                try:
                    converted = converted.astype("Int64")
                except (TypeError, ValueError, OverflowError):
                    errors.append(f"{column}: values exceed the supported integer range.")
        if column == "SeniorCitizen" and not converted.dropna().isin([0, 1]).all():
            errors.append("SeniorCitizen: only 0 and 1 are permitted.")
        clean[column] = converted

    for column, allowed in CATEGORICAL_VALUES.items():
        invalid = sorted(set(clean[column].dropna()) - set(allowed))
        if invalid:
            errors.append(f"{column}: unexpected categories {invalid}.")

    counts = clean[TARGET].value_counts()
    if set(counts.index) != {"No", "Yes"} or counts.min() < 5:
        errors.append("Both Churn classes require at least five customers for splitting.")

    report["target_distribution"] = {
        label: {"count": int(counts.get(label, 0)), "percentage": round(float(counts.get(label, 0)) / len(clean) * 100, 4)}
        for label in ("No", "Yes")
    }
    report["numeric_summary"] = {}
    for column in NUMERIC_COLUMNS:
        finite_values = clean[column].dropna()
        finite_values = finite_values[np.isfinite(finite_values.astype(float))]
        # Failure reports must also be valid JSON when input contains infinity.
        report["numeric_summary"][column] = {
            "dtype": str(clean[column].dtype),
            "missing": int(clean[column].isna().sum()),
            "min": float(finite_values.min()) if len(finite_values) else None,
            "max": float(finite_values.max()) if len(finite_values) else None,
        }
    report["feature_contract"] = {
        "identifier": IDENTIFIER, "target": TARGET,
        "target_mapping_for_training": {"No": 0, "Yes": 1},
        "features": list(FEATURE_COLUMNS),
        "numeric_features": list(NUMERIC_COLUMNS),
        "categorical_features": [column for column in FEATURE_COLUMNS if column not in NUMERIC_COLUMNS],
        "excluded_from_features": [IDENTIFIER, TARGET, "event_ts", "processed_ts", "event_id"],
    }
    report["missing_value_policy"] = {
        "TotalCharges": "Convert blanks to numeric missing values; retain rows. Fit median imputation inside the later training/CV pipeline, using training data only.",
        "all_other_columns": "Reject missing values.",
        "imputation_performed": False,
        "blank_total_charges_count": missing["TotalCharges"],
        "blank_total_charges_with_zero_tenure": int((clean["TotalCharges"].isna() & clean["tenure"].eq(0)).sum()),
    }
    if missing["TotalCharges"]:
        report["warnings"].append(f"Retained {missing['TotalCharges']} missing TotalCharges values for training-only imputation.")
    report["checks"].update({
        "schema": True, "required_values": not any(missing[c] for c in missing if c != "TotalCharges"),
        "identifier_and_target_excluded_from_features": not {IDENTIFIER, TARGET}.intersection(FEATURE_COLUMNS),
        "no_rows_dropped": len(clean) == len(raw),
    })
    if errors:
        report["status"] = "FAIL"
        raise DataValidationError(report)
    report["status"] = "PASS_WITH_WARNINGS" if report["warnings"] else "PASS"
    # Input row order cannot change the split assignment.
    return clean.sort_values(IDENTIFIER).reset_index(drop=True), report
