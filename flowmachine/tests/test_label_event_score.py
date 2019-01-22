# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core import JoinToLocation
from flowmachine.features import EventScore
from flowmachine.features.subscriber.label_event_score import LabelEventScore


@pytest.mark.usefixtures("skip_datecheck")
def test_labelled_event_score_column_names(
    exemplar_level_param, get_column_names_from_run
):
    if exemplar_level_param["level"] not in JoinToLocation.allowed_levels:
        pytest.skip(f'{exemplar_level_param["level"]} not valid for this test')
    es = EventScore(start="2016-01-01", stop="2016-01-05", **exemplar_level_param)
    labelled = LabelEventScore(scores=es, required="evening")
    assert get_column_names_from_run(labelled) == labelled.column_names


def test_locations_are_labelled_correctly(get_dataframe):
    """
    Test whether locations are labelled corrected.
    """
    es = EventScore(start="2016-01-01", stop="2016-01-05", level="versioned-site")

    ls = LabelEventScore(
        scores=es,
        labels={
            "daytime": {
                "type": "Polygon",
                "coordinates": [[[-1.1, -1.1], [-1, 1.1], [1.1, 1.1], [1.1, -1.1]]],
            }
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
        ls = LabelEventScore(
            scores=es,
            labels={
                "unknown": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-1.1, -1.1],
                            [-1.0, 1.1],
                            [1.1, 1.1],
                            [1.1, -1.1],
                            [-1.1, -1.1],
                        ]
                    ],
                }
            },
        )


def test_whether_required_label_relabels(get_dataframe):
    """
    Test whether required label relabel the location of subscribers who did not originally have the required label.
    """
    es = EventScore(start="2016-01-01", stop="2016-01-05", level="versioned-site")

    ls = LabelEventScore(
        scores=es,
        labels={
            "daytime": {
                "type": "Polygon",
                "coordinates": [
                    [[-1.1, -1.1], [-1.0, 1.1], [1.1, 1.1], [1.1, -1.1], [-1.1, -1.1]]
                ],
            }
        },
        required="evening",
    )
    df = get_dataframe(ls)
    assert list(df["label"].unique()) == ["evening"]


def test_overlaps_bounds_dict():
    """
    Should return False if there are no overlaps in the labels dict.
    """
    bounds_dict = {
        "evening": {
            "type": "MultiPolygon",
            "coordinates": [
                [[[0.000_001, 0.5], [0.000_001, 1], [1, 1], [1, 0.5]]],
                [[[0.000_001, -1], [0.000_001, -0.5], [1, -0.5], [1, -1]]],
            ],
        },
        "day": {
            "type": "Polygon",
            "coordinates": [[[-1, -0.5], [-1, 0.5], [0, 0.5], [0, -0.5]]],
        },
    }
    assert not LabelEventScore.bounds_dict_has_overlaps(
        LabelEventScore._make_bounds_dict(bounds_dict)
    )


def test_overlaps_bounds_dict_raises():
    """
    Should raise an error if an overlap is found.
    """
    bounds_dict = {
        "evening": {
            "type": "MultiPolygon",
            "coordinates": [
                [[[0.000_001, 0.5], [0.000_001, 1], [1, 1], [1, 0.5]]],
                [[[0.000_001, -1], [0.000_001, -0.5], [1, -0.5], [1, -1]]],
            ],
        },
        "day": {
            "type": "Polygon",
            "coordinates": [[[-1, -0.5], [-1, 0.75], [0, 0.5], [0.1, -0.75]]],
        },
    }
    with pytest.raises(ValueError):
        LabelEventScore.bounds_dict_has_overlaps(
            LabelEventScore._make_bounds_dict(bounds_dict)
        )


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
        LabelEventScore(
            scores=EventScore(start="2016-01-01", stop="2016-01-05"), labels=bounds_dict
        )


# Test bad bounds error handling

bad_bounds = [
    ({}, ValueError),
    ("BAD", ValueError),
    (
        {
            "type": "NOT_A_TYPE_OF_SHAPE",
            "coordinates": [
                [[[0.000_001, 0.5], [0.000_001, 1], [1, 1], [1, 0.5]]],
                [[[0.000_001, -1], [0.000_001, -0.5], [1, -0.5], [1, -1]]],
            ],
        },
        ValueError,
    ),
    (
        {
            "type": "NOT_A_TYPE_OF_SHAPE",
            "coordinates": [
                [0.000_001, 0.5],
                [0.000_001, 1],
                [1, 1],
                [1, 0.5],
                [[[0.000_001, -1], [0.000_001, -0.5], [1, -0.5], [1, -1]]],
            ],
        },
        ValueError,
    ),
]


@pytest.mark.parametrize("bad_bound", next(zip(*bad_bounds)))
def test_constructor_raises_value_error(bad_bound):
    """
    Constructor should only raise value errors.
    """
    with pytest.raises(ValueError):
        LabelEventScore(
            scores=EventScore(start="2016-01-01", stop="2016-01-05"),
            labels={"DUMMY_LABEL": [bad_bound]},
        )


def test_non_query_scores_obj_raises():
    """
    Passing a scores object which is not a query type should raise an error.
    """
    with pytest.raises(TypeError):
        LabelEventScore(
            scores="NOT_A_QUERY",
            labels={
                "DUMMY_LABEL": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-1.1, -1.1],
                            [-1.0, 1.1],
                            [1.1, 1.1],
                            [1.1, -1.1],
                            [-1.1, -1.1],
                        ]
                    ],
                }
            },
        )
