"""Persist customer membership so sklearn and Spark share identical partitions."""

from math import isclose, isfinite

import pandas as pd
from sklearn.model_selection import train_test_split

from .data_validation import IDENTIFIER, TARGET

SPLITS = ("train", "validation", "test")


def create_memberships(data: pd.DataFrame, *, seed: int, train: float, validation: float, test: float) -> pd.DataFrame:
    if not all(isfinite(value) and 0 < value < 1 for value in (train, validation, test)) or not isclose(train + validation + test, 1.0, abs_tol=1e-9):
        raise ValueError("Positive train/validation/test fractions must sum to one.")
    ordered = data.sort_values(IDENTIFIER).reset_index(drop=True)
    train_data, held_out = train_test_split(
        ordered, test_size=validation + test, random_state=seed, stratify=ordered[TARGET],
    )
    validation_data, test_data = train_test_split(
        held_out, test_size=test / (validation + test), random_state=seed, stratify=held_out[TARGET],
    )
    memberships = pd.concat([
        part[[IDENTIFIER]].assign(split=name)
        for name, part in zip(SPLITS, (train_data, validation_data, test_data))
    ], ignore_index=True).sort_values(IDENTIFIER).reset_index(drop=True)
    apply_memberships(data, memberships)
    return memberships


def apply_memberships(data: pd.DataFrame, memberships: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Join by ID, never by row position; Spark can perform the same join."""
    if set(memberships.columns) != {IDENTIFIER, "split"}:
        raise ValueError("Memberships must contain only customerID and split.")
    if data[IDENTIFIER].isna().any() or data[IDENTIFIER].duplicated().any():
        raise ValueError("Input customer IDs must be unique and nonmissing.")
    if memberships.isna().any().any() or memberships[IDENTIFIER].duplicated().any():
        raise ValueError("Memberships must be nonmissing and unique per customer.")
    if set(memberships["split"]) != set(SPLITS):
        raise ValueError("Memberships must include train, validation and test only.")
    if set(data[IDENTIFIER]) != set(memberships[IDENTIFIER]):
        raise ValueError("Memberships must cover exactly the input customer IDs.")
    joined = data.merge(memberships, on=IDENTIFIER, validate="one_to_one")
    return {
        name: joined.loc[joined["split"].eq(name), data.columns].sort_values(IDENTIFIER).reset_index(drop=True)
        for name in SPLITS
    }
