import pandas as pd

from opsyntra.data.validation import EXPECTED_COLUMNS


def sample_data(size: int = 100) -> pd.DataFrame:
    rows = []
    for index in range(size):
        row = dict.fromkeys(EXPECTED_COLUMNS, "No")
        row.update({
            "customerID": f"TEST-{index:05d}", "gender": "Female" if index % 2 else "Male",
            "SeniorCitizen": "0", "tenure": str(index % 73),
            "InternetService": "DSL", "Contract": "Month-to-month",
            "PaymentMethod": "Electronic check", "MonthlyCharges": "29.85",
            "TotalCharges": " " if index % 20 == 0 else str(29.85 * (index + 1)),
            "Churn": "Yes" if index % 10 < 3 else "No",
        })
        rows.append(row)
    return pd.DataFrame(rows, columns=EXPECTED_COLUMNS).astype("string")
