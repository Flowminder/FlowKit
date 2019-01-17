# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core import JoinToLocation
from flowmachine.features import EventScore
from flowmachine.features.subscriber.label_event_score import LabelEventScore


@pytest.mark.usefixtures("skip_datecheck")
def test_labelled_event_score_column_names(exemplar_level_param):
    if exemplar_level_param["level"] not in JoinToLocation.allowed_levels:
        pytest.skip(f'{exemplar_level_param["level"]} not valid for this test')
    es = EventScore(start="2016-01-01", stop="2016-01-05", **exemplar_level_param)
    labelled = LabelEventScore(es, required="evening")
    assert labelled.head(0).columns.tolist() == labelled.column_names


def test_locations_are_labelled_correctly(get_dataframe):
    """
    Test whether locations are labelled corrected.
    """
    es = EventScore(start="2016-01-01", stop="2016-01-05", level="versioned-site")

    ls = LabelEventScore(
        es,
        {
            "daytime": [
                {
                    "hour_lower_bound": -1.1,
                    "hour_upper_bound": 1.1,
                    "day_of_week_upper_bound": 1.1,
                    "day_of_week_lower_bound": -1.1,
                }
            ]
        },
    )
    df = get_dataframe(ls)
    assert list(df["label"].unique()) == ["daytime"]


def test_whether_passing_reserved_label_fails():
    """
    Test whether passing the reserved label 'unknown' fails.
    """
    es = EventScore(start="2016-01-01", stop="2016-01-05", level="versioned-site")

    with pytest.raises(ValueError):
        ls = LabelEventScore(es, {"unknown": []})


def test_whether_required_label_relabels(get_dataframe):
    """
    Test whether required label relabel the location of subscribers who did not originally have the required label.
    """
    es = EventScore(start="2016-01-01", stop="2016-01-05", level="versioned-site")

    ls = LabelEventScore(
        es,
        {
            "daytime": [
                {
                    "hour_lower_bound": -1.1,
                    "hour_upper_bound": 1.1,
                    "day_of_week_upper_bound": 1.1,
                    "day_of_week_lower_bound": -1.1,
                }
            ]
        },
        "evening",
    )
    df = get_dataframe(ls)
    assert list(df["label"].unique()) == ["evening"]


def test_check_overlap_no_overlap():
    """
    Should return False with no overlap.
    """
    assert not LabelEventScore.has_overlap(
        {
            "hour_lower_bound": 0.00001,
            "hour_upper_bound": 1,
            "day_of_week_upper_bound": 1,
            "day_of_week_lower_bound": 0.5,
        },
        {
            "hour_lower_bound": -1,
            "hour_upper_bound": 0,
            "day_of_week_upper_bound": 0.5,
            "day_of_week_lower_bound": -0.5,
        },
    )


def test_check_overlap():
    """
    Should return True with an overlap.
    """
    assert LabelEventScore.has_overlap(
        {
            "hour_lower_bound": 0,
            "hour_upper_bound": 1,
            "day_of_week_upper_bound": 1,
            "day_of_week_lower_bound": 0.5,
        },
        {
            "hour_lower_bound": -1,
            "hour_upper_bound": 0,
            "day_of_week_upper_bound": 0.5,
            "day_of_week_lower_bound": -0.5,
        },
    )


def test_overlaps_bounds_dict():
    """
    Should return False if there are no overlaps in the labels dict.
    """
    bounds_dict = {
        "evening": [
            {
                "hour_lower_bound": 0.00001,
                "hour_upper_bound": 1,
                "day_of_week_upper_bound": 1,
                "day_of_week_lower_bound": 0.5,
            },
            {
                "hour_lower_bound": 0.00001,
                "hour_upper_bound": 1,
                "day_of_week_upper_bound": -0.5,
                "day_of_week_lower_bound": -1,
            },
        ],
        "day": [
            {
                "hour_lower_bound": -1,
                "hour_upper_bound": 0,
                "day_of_week_upper_bound": 0.5,
                "day_of_week_lower_bound": -0.5,
            }
        ],
    }
    assert not LabelEventScore.bounds_dict_has_overlaps(bounds_dict)


def test_overlaps_bounds_dict_raises():
    """
    Should raise an error if an overlap is found.
    """
    bounds_dict = {
        "evening": [
            {
                "hour_lower_bound": 0,
                "hour_upper_bound": 1,
                "day_of_week_upper_bound": 1,
                "day_of_week_lower_bound": 0.5,
            },
            {
                "hour_lower_bound": 0.00001,
                "hour_upper_bound": 1,
                "day_of_week_upper_bound": -0.5,
                "day_of_week_lower_bound": -1,
            },
        ],
        "day": [
            {
                "hour_lower_bound": -1,
                "hour_upper_bound": 0,
                "day_of_week_upper_bound": 0.5,
                "day_of_week_lower_bound": -0.5,
            }
        ],
    }
    with pytest.raises(ValueError):
        LabelEventScore.bounds_dict_has_overlaps(bounds_dict)


def test_constructor_overlaps_bounds_dict_raises():
    """
    Should raise an error if an overlap is found in constructor.
    """
    bounds_dict = {
        "evening": [
            {
                "hour_lower_bound": 0,
                "hour_upper_bound": 1,
                "day_of_week_upper_bound": 1,
                "day_of_week_lower_bound": 0.5,
            },
            {
                "hour_lower_bound": 0.00001,
                "hour_upper_bound": 1,
                "day_of_week_upper_bound": -0.5,
                "day_of_week_lower_bound": -1,
            },
        ],
        "day": [
            {
                "hour_lower_bound": -1,
                "hour_upper_bound": 0,
                "day_of_week_upper_bound": 0.5,
                "day_of_week_lower_bound": -0.5,
            }
        ],
    }
    with pytest.raises(ValueError):
        LabelEventScore(EventScore(start="2016-01-01", stop="2016-01-05"), bounds_dict)


# Test bad bounds error handling

bad_bounds = [
    (
        {
            "hour_lower_bound": 0,
            "hour_upper_bound": 0,
            "day_of_week_upper_bound": 0.5,
            "day_of_week_lower_bound": -0.5,
        },
        ValueError,
    ),
    (
        {
            "hour_lower_bound": 1,
            "hour_upper_bound": 0,
            "day_of_week_upper_bound": 0.5,
            "day_of_week_lower_bound": -0.5,
        },
        ValueError,
    ),
    (
        {
            "hour_lower_bound": -1,
            "hour_upper_bound": 0,
            "day_of_week_upper_bound": -0.5,
            "day_of_week_lower_bound": -0.5,
        },
        ValueError,
    ),
    (
        {
            "hour_lower_bound": -1,
            "hour_upper_bound": 0,
            "day_of_week_upper_bound": 0.5,
            "day_of_week_lower_bound": 0.75,
        },
        ValueError,
    ),
    (
        {"hour_lower_bound": -1, "hour_upper_bound": 0, "day_of_week_upper_bound": 0.5},
        KeyError,
    ),
]


@pytest.mark.parametrize("bad_bound, expected_error", bad_bounds)
def test_check_bad_bound_raises_error(bad_bound, expected_error):
    """
    Should raise an error if the bounds dict has mistakes.
    """
    with pytest.raises(expected_error):
        LabelEventScore.check_bound_is_valid(bad_bound)


@pytest.mark.parametrize("bad_bound", next(zip(*bad_bounds)))
def test_constructor_raises_value_error(bad_bound):
    """
    Constructor should only raise value errors.
    """
    with pytest.raises(ValueError):
        LabelEventScore(
            EventScore(start="2016-01-01", stop="2016-01-05"),
            {"DUMMY_LABEL": [bad_bound]},
        )
