# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests classes in the `features/subscriber/scores` modules such as EventScore
"""


import pytest

from flowmachine.core import JoinToLocation
from flowmachine.features import EventScore


@pytest.mark.usefixtures("skip_datecheck")
def test_event_score_column_names(exemplar_level_param):
    if exemplar_level_param["level"] not in JoinToLocation.allowed_levels:
        pytest.skip(f'{exemplar_level_param["level"]} not valid for this test')
    es = EventScore(start="2016-01-01", stop="2016-01-05", **exemplar_level_param)
    assert es.head(0).columns.tolist() == es.column_names


def test_whether_scores_are_within_score_bounds(get_dataframe):
    """
    Test whether the scores are within the bounds of maximum and minimum scores.
    """
    es = EventScore(start="2016-01-01", stop="2016-01-05", level="versioned-site")
    df = get_dataframe(es)
    max_score = df[["score_hour", "score_dow"]].max()
    min_score = df[["score_hour", "score_dow"]].min()
    assert all(max_score <= [1, 1])
    assert all(min_score >= [-1, -1])


@pytest.mark.parametrize("scorer", ["score_hour", "score_dow"])
@pytest.mark.parametrize("out_of_bounds_val", [-1.5, 2, "NOT_A_NUMBER"])
def test_out_of_bounds_score_raises(scorer, out_of_bounds_val, flowmachine_connect):
    """
    Test whether passing a scoring rule which is out of bounds errors.
    """
    scorers = dict(
        score_hour=dict.fromkeys(range(24), 0),
        score_dow=dict.fromkeys(
            {
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
            },
            0,
        ),
    )
    scorers[scorer][scorers[scorer].popitem()[0]] = out_of_bounds_val
    with pytest.raises(ValueError):
        es = EventScore(
            start="2016-01-01", stop="2016-01-05", level="versioned-site", **scorers
        )


def test_whether_zero_score_returns_only_zero(get_dataframe):
    """
    Test whether passing a scoring rule where all events are scored with 0 returns only 0 scores.
    """
    es = EventScore(
        start="2016-01-01",
        stop="2016-01-05",
        score_hour=dict.fromkeys(range(24), 0),
        score_dow=dict.fromkeys(
            {
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
            },
            0,
        ),
        level="versioned-site",
    )
    df = get_dataframe(es)
    valid = df[["score_hour", "score_dow"]] == 0

    assert all(valid.all())


def test_whether_score_that_do_not_cover_domain_raises(get_dataframe):
    """
    Test whether scoring rules that do not cover the whole domain is an error.
    """
    with pytest.raises(ValueError):
        es = EventScore(start="2016-01-01", stop="2016-01-05", score_hour={0: 0})
    with pytest.raises(ValueError):
        es = EventScore(start="2016-01-01", stop="2016-01-05", score_dow={"monday": 0})
