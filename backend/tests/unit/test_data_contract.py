import unittest

import pandas as pd

from opsyntra.data.splitting import apply_memberships, create_memberships
from opsyntra.data.validation import DataValidationError, FEATURE_COLUMNS, validate_and_normalize
from tests.helpers import sample_data


class DataContractTests(unittest.TestCase):
    def test_blank_charges_become_numeric_missing_without_dropping_rows(self):
        data, report = validate_and_normalize(sample_data())
        self.assertEqual(len(data), 100)
        self.assertTrue(pd.api.types.is_numeric_dtype(data["TotalCharges"]))
        self.assertEqual(data["TotalCharges"].isna().sum(), 5)
        self.assertFalse(report["missing_value_policy"]["imputation_performed"])

    def test_identifiers_and_target_are_not_features(self):
        self.assertEqual(len(FEATURE_COLUMNS), 19)
        self.assertNotIn("Churn", FEATURE_COLUMNS)
        self.assertNotIn("customerID", FEATURE_COLUMNS)

    def test_corrupt_values_fail_instead_of_becoming_missing(self):
        for column, value in [("TotalCharges", "broken"), ("MonthlyCharges", "-1"), ("Churn", "maybe"), ("tenure", "1.5"), ("MonthlyCharges", "inf")]:
            with self.subTest(column=column, value=value):
                data = sample_data()
                data.loc[0, column] = value
                with self.assertRaises(DataValidationError):
                    validate_and_normalize(data)

    def test_duplicate_ids_missing_required_values_and_schema_errors_fail(self):
        duplicate = sample_data()
        duplicate.loc[1, "customerID"] = duplicate.loc[0, "customerID"]
        missing = sample_data()
        missing.loc[0, "Contract"] = " "
        for data in [duplicate, missing, sample_data().drop(columns="Churn"), sample_data().assign(extra="value")]:
            with self.assertRaises(DataValidationError):
                validate_and_normalize(data)

    def test_memberships_are_complete_disjoint_and_stratified(self):
        data, _ = validate_and_normalize(sample_data())
        memberships = create_memberships(data, seed=42, train=.6, validation=.2, test=.2)
        splits = apply_memberships(data, memberships)
        self.assertEqual([len(splits[name]) for name in ("train", "validation", "test")], [60, 20, 20])
        self.assertTrue(memberships["customerID"].is_unique)
        self.assertEqual(len(memberships), len(data))
        for part in splits.values():
            self.assertAlmostEqual(part["Churn"].eq("Yes").mean(), .3)

    def test_memberships_are_reproducible_even_after_reordering(self):
        data, _ = validate_and_normalize(sample_data())
        original = create_memberships(data, seed=42, train=.6, validation=.2, test=.2)
        reordered = create_memberships(data.sample(frac=1, random_state=3), seed=42, train=.6, validation=.2, test=.2)
        pd.testing.assert_frame_equal(original, reordered)
        changed_seed = create_memberships(data, seed=43, train=.6, validation=.2, test=.2)
        self.assertFalse(original.equals(changed_seed))

    def test_saved_memberships_rejoin_by_id_and_reject_partial_coverage(self):
        data, _ = validate_and_normalize(sample_data())
        memberships = create_memberships(data, seed=42, train=.6, validation=.2, test=.2)
        original = apply_memberships(data, memberships)
        reordered = apply_memberships(data.sample(frac=1, random_state=7), memberships)
        for name in original:
            pd.testing.assert_frame_equal(original[name], reordered[name])
        with self.assertRaises(ValueError):
            apply_memberships(data, memberships.iloc[:-1])
