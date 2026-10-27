import unittest

import numpy as np
from sklearn.model_selection import StratifiedKFold, cross_validate
from sklearn.preprocessing import StandardScaler

from opsyntra.data.validation import FEATURE_COLUMNS, NUMERIC_COLUMNS
from opsyntra.ml.building import build_model_pipeline
from opsyntra.ml.preprocessing import NormalizeTelcoInputs
from tests.helpers import sample_data


class ModelPipelineTests(unittest.TestCase):
    def setUp(self):
        self.data = sample_data()
        self.X = self.data.drop(columns="Churn")
        self.y = self.data["Churn"].map({"No": 0, "Yes": 1}).astype(int)

    def test_imputer_scaler_and_encoder_learn_only_from_fitted_rows(self):
        train = self.X.iloc[:60]
        pipeline = build_model_pipeline("logistic_regression", jobs=1).fit(train, self.y.iloc[:60])
        numeric = pipeline.named_steps["preprocessing"].named_transformers_["numeric"]
        normal = NormalizeTelcoInputs().fit_transform(train)
        expected = np.nanmedian(normal[list(NUMERIC_COLUMNS)].to_numpy(), axis=0)
        np.testing.assert_allclose(numeric.named_steps["imputer"].statistics_, expected)
        self.assertIsInstance(numeric.named_steps["scaler"], StandardScaler)
        unseen = self.X.iloc[60:].copy()
        unseen["Contract"] = "VALIDATION-ONLY-CATEGORY"
        unseen["TotalCharges"] = "9999999"
        pipeline.predict_proba(unseen)
        np.testing.assert_allclose(numeric.named_steps["imputer"].statistics_, expected)
        encoder = pipeline.named_steps["preprocessing"].named_transformers_["categorical"].named_steps["encoder"]
        self.assertFalse(any("VALIDATION-ONLY-CATEGORY" in values for values in encoder.categories_))

    def test_cv_relearns_medians_inside_each_training_fold(self):
        folds = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)
        result = cross_validate(
            build_model_pipeline("logistic_regression", jobs=1), self.X, self.y,
            cv=folds, return_estimator=True, error_score="raise",
        )
        for estimator, (train_indices, _) in zip(result["estimator"], folds.split(self.X, self.y)):
            fold = NormalizeTelcoInputs().fit_transform(self.X.iloc[train_indices])
            expected = np.nanmedian(fold[list(NUMERIC_COLUMNS)].to_numpy(), axis=0)
            actual = estimator.named_steps["preprocessing"].named_transformers_["numeric"].named_steps["imputer"].statistics_
            np.testing.assert_allclose(actual, expected)

    def test_label_identifier_and_column_order_cannot_change_predictions(self):
        pipeline = build_model_pipeline("logistic_regression", jobs=1).fit(self.X, self.y)
        original = pipeline.predict_proba(self.X)
        modified = self.X.copy()
        modified["customerID"] = "different identifier"
        modified["Churn"] = "not a feature"
        modified["event_ts"] = "2099-01-01"
        np.testing.assert_allclose(original, pipeline.predict_proba(modified[modified.columns[::-1]]))
        self.assertEqual(list(pipeline.named_steps["normalize"].get_feature_names_out()), list(FEATURE_COLUMNS))

    def test_missing_values_and_unknown_categories_work_without_label_or_identifier(self):
        pipeline = build_model_pipeline("logistic_regression", jobs=1).fit(self.X, self.y)
        record = self.X.iloc[[0]].drop(columns="customerID").copy()
        record["TotalCharges"] = " "
        record["gender"] = None
        record["PaymentMethod"] = "New payment method"
        probabilities = pipeline.predict_proba(record)
        self.assertTrue(np.isfinite(probabilities).all())
        self.assertAlmostEqual(probabilities.sum(), 1.0)

    def test_bad_numeric_values_and_missing_feature_columns_fail(self):
        pipeline = build_model_pipeline("logistic_regression", jobs=1).fit(self.X, self.y)
        for value in ("broken", "-1", "inf"):
            record = self.X.iloc[[0]].copy()
            record["MonthlyCharges"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                pipeline.predict(record)
        with self.assertRaises(ValueError):
            pipeline.predict(self.X.drop(columns="tenure"))

    def test_all_four_estimators_fit_and_predict_raw_records(self):
        for name in ("dummy", "logistic_regression", "random_forest", "xgboost"):
            with self.subTest(name=name):
                pipeline = build_model_pipeline(name, jobs=1)
                if name in ("random_forest", "xgboost"):
                    pipeline.set_params(classifier__n_estimators=5)
                pipeline.fit(self.X, self.y)
                probabilities = pipeline.predict_proba(self.X.iloc[:3])
                self.assertEqual(probabilities.shape, (3, 2))
                self.assertTrue(np.isfinite(probabilities).all())
