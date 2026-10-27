import unittest

import numpy as np
from sklearn.metrics import f1_score

from opsyntra.ml.evaluation import select_threshold


class ThresholdSelectionTests(unittest.TestCase):
    def test_threshold_finds_best_f1_with_duplicate_probabilities(self):
        labels = np.array([0, 1, 1, 0, 1, 0])
        probabilities = np.array([.1, .3, .3, .6, .8, .8])
        result = select_threshold(labels, probabilities)
        best = max(f1_score(labels, probabilities >= t) for t in np.unique(probabilities))
        self.assertAlmostEqual(result["f1"], best)
        self.assertAlmostEqual(f1_score(labels, probabilities >= result["threshold"]), best)

    def test_f1_ties_prefer_threshold_closest_to_half(self):
        result = select_threshold([0, 1, 0, 1], [.2, .4, .6, .8])
        self.assertAlmostEqual(result["threshold"], .4)
