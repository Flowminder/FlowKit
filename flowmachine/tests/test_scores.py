# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests classes in the `features/subscriber/scores` modules such as EventScore
and LabelEventScore.
"""

import pandas as pd

from unittest import TestCase

import pytest

from flowmachine.core import JoinToLocation
from flowmachine.features import EventScore, LabelEventScore


@pytest.mark.usefixtures("skip_datecheck")
def test_event_score_column_names(exemplar_level_param):
    if exemplar_level_param["level"] not in JoinToLocation.allowed_levels:
        pytest.skip(f'{exemplar_level_param["level"]} not valid for this test')
    es = EventScore(start="2016-01-01", stop="2016-01-05", **exemplar_level_param)
    assert es.head(0).columns.tolist() == es.column_names


@pytest.mark.usefixtures("skip_datecheck")
def test_labelled_event_score_column_names(exemplar_level_param):
    if exemplar_level_param["level"] not in JoinToLocation.allowed_levels:
        pytest.skip(f'{exemplar_level_param["level"]} not valid for this test')
    es = EventScore(start="2016-01-01", stop="2016-01-05", **exemplar_level_param)
    labelled = LabelEventScore(
        es,
        {
            "evening": "(score_hour > 0) AND (score_dow > 0.5 OR score_dow < -0.5)",
            "daytime": "(score_hour < 0) AND (score_dow < 0.5 AND score_dow > -0.5)",
        },
        "location_type",
        "evening",
    )
    assert labelled.head(0).columns.tolist() == labelled.column_names


class TestEventScore(TestCase):
    def setUp(self):
        self.es = EventScore(
            start="2016-01-01", stop="2016-01-05", level="versioned-site"
        )
        self.df = self.es.get_dataframe()

    def test_returns_dataframe(self):
        """
        Checks that EventScore returns a dataframe type.
        """
        self.assertIs(type(self.df), pd.DataFrame)

    def test_whether_scores_are_within_score_bounds(self):
        """
        Test whether the scores are within the bounds of maximum and minimum scores.
        """
        max_score = self.df[["score_hour", "score_dow"]].max()
        min_score = self.df[["score_hour", "score_dow"]].min()
        self.assertTrue(all(max_score <= [1, 1]))
        self.assertTrue(all(min_score >= [-1, -1]))

    def test_whether_zero_score_returns_only_zero(self):
        """
        Test whether passing a scoring rule where all events are scored with 0 returns only 0 scores.
        """
        es = EventScore(
            start="2016-01-01",
            stop="2016-01-05",
            score_hour={(0, 24): 0},
            score_dow={(0, 7): 0},
            level="versioned-site",
        )
        df = es.get_dataframe()
        valid = df[["score_hour", "score_dow"]] == 0

        self.assertTrue(all(valid.all()))

    def test_whether_score_that_do_not_cover_domain_return_null(self):
        """
        Test whether scoring rules that do not cover the whole domain return null values.
        """
        es = EventScore(
            start="2016-01-01",
            stop="2016-01-05",
            score_hour={(7, 9): 0},
            score_dow={(1, 2): 0},
        )
        df = es.get_dataframe()
        valid = df[["score_hour", "score_dow"]].apply(lambda x: (x.isnull()) | (x == 0))
        self.assertTrue(all(valid.all()))

    def test_whether_different_levels_do_not_raise_error(self):
        """
        Test whether passing different levels do not raise error.
        """
        es = EventScore(start="2016-01-01", stop="2016-01-05", level="admin3")
        df = es.get_dataframe()
        self.assertIs(type(df), pd.DataFrame)

        es = EventScore(
            start="2016-01-01",
            stop="2016-01-05",
            level="polygon",
            geom_col="geom_point",
            polygon_table="infrastructure.sites",
            column_name="geom_point",
        )
        df = es.get_dataframe()
        self.assertIs(type(df), pd.DataFrame)


class TestLabelEventScore(TestCase):
    def setUp(self):
        self.es = EventScore(
            start="2016-01-01", stop="2016-01-05", level="versioned-site"
        )

        self.ls = LabelEventScore(
            self.es,
            {
                "evening": "(score_hour > 0) AND (score_dow > 0.5 OR score_dow < -0.5)",
                "daytime": "(score_hour < 0) AND (score_dow < 0.5 AND score_dow > -0.5)",
            },
            "location_type",
            "evening",
        )
        self.df = self.ls.get_dataframe()

    def test_returns_dataframe(self):
        """
        Checks that LabelEventScore returns a dataframe type.
        """
        self.assertIs(type(self.df), pd.DataFrame)

    def test_existing_enumerated_type_initialization_fails(self):
        """
        Tests whether initializing an existing enumerated type in the database with extra arguments fail.
        """
        with self.assertRaises(ValueError):
            ls = LabelEventScore(
                self.es,
                {
                    "evening": "(score_hour > 0) AND (score_dow > 0.5 OR score_dow < -0.5)",
                    "daytime": "(score_hour < 0) AND (score_dow < 0.5 AND score_dow > -0.5)",
                    "new_label": "(score_hour > 1",
                },
                "location_type",
                "evening",
            )
            ls.head()

    def test_locations_are_labelled_correctly(self):
        """
        Test whether locations are labelled corrected.
        """
        ls = LabelEventScore(
            self.es, {"daytime": "(score_hour >= -1)"}, "location_type"
        )
        df = ls.get_dataframe()
        self.assertEquals(list(df["label"].unique()), ["daytime"])

    def test_whether_passing_reserved_label_fails(self):
        """
        Test whether passing the reserved label 'unknown' fails.
        """
        with self.assertRaises(ValueError):
            ls = LabelEventScore(
                self.es, {"unknown": "(score_hour >= -1)"}, "location_type"
            )

    def test_whether_required_label_relabels(self):
        """
        Test whether required label relabel the location of subscribers who did not originally have the required label.
        """
        ls = LabelEventScore(
            self.es, {"daytime": "(score_hour >= -1)"}, "location_type", "evening"
        )
        df = ls.get_dataframe()
        self.assertEquals(list(df["label"].unique()), ["evening"])

    def test_whether_injection_attempts_are_blocked(self):
        """
        Tests whether injection attempts are blocked by flowmachine.
        """
        with self.assertRaises(ValueError):
            ls = LabelEventScore(
                self.es,
                {
                    "daytime": "(score_hour >= -1) THEN 'evening' END AS foo CASE WHEN (score_hour == 0)"
                },
                "location_type",
                "evening",
            )
