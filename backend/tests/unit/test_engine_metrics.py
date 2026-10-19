import unittest
import numpy as np
from sklearn.metrics import average_precision_score, f1_score

from opsyntra.model_evaluation import evaluate_probabilities


class EngineMetricsTests(unittest.TestCase):
    def test_comparison_uses_churn_f1_and_non_interpolated_average_precision(self):
        y = [0, 0, 0, 0, 0, 1, 1]
        probabilities = np.array([.1, .2, .3, .4, .8, .7, .9])
        metrics = evaluate_probabilities(y, probabilities, .6)
        self.assertAlmostEqual(metrics["f1"], f1_score(y, probabilities >= .6, pos_label=1))
        self.assertNotAlmostEqual(metrics["f1"], f1_score(y, probabilities >= .6, average="weighted"))
        self.assertAlmostEqual(metrics["average_precision"], average_precision_score(y, probabilities))
        self.assertEqual(metrics["confusion_matrix"], [[4, 1], [0, 2]])
