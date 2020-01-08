# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

import datetime

import pendulum

from autoflow.date_stencil import DateStencil, InvalidDateIntervalError


def test_date_stencil_intervals():
    """
    Test that a DateStencil contains the date intervals defined in the raw stencil passed to __init__.
    """
    raw_stencil = [
        pendulum.date(2016, 1, 1),
        -4,
        [pendulum.date(2016, 1, 2), pendulum.date(2016, 1, 3)],
        [-3, -1],
        [pendulum.date(2016, 1, 1), -1],
        [0, 1],
    ]
    expected_intervals = (
        (pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 2)),
        (-4, -3),
        (pendulum.date(2016, 1, 2), pendulum.date(2016, 1, 3)),
        (-3, -1),
        (pendulum.date(2016, 1, 1), -1),
        (0, 1),
    )
    date_stencil = DateStencil(raw_stencil)
    assert date_stencil._intervals == expected_intervals


@pytest.mark.parametrize(
    "raw_stencil,error",
    [
        (["BAD_ELEMENT"], TypeError),
        ([[-1, "BAD_ELEMENT"]], TypeError),
        (-1, TypeError),
        ([[-3, -2, -1]], ValueError),
        ([[-1, -2]], InvalidDateIntervalError),
        ([[-1, -1]], InvalidDateIntervalError),
        (
            [[pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 1)]],
            InvalidDateIntervalError,
        ),
    ],
)
def test_date_stencil_errors(raw_stencil, error):
    """
    Test that DateStencil raises the appropriate error when initialised with an invalid raw stencil.
    """
    with pytest.raises(error):
        date_stencil = DateStencil(raw_stencil)


@pytest.mark.parametrize(
    "offset,reference_date,expected",
    [
        (
            datetime.date(2016, 1, 2),
            pendulum.date(2016, 1, 1),
            pendulum.date(2016, 1, 2),
        ),
        (-2, pendulum.date(2016, 1, 1), pendulum.date(2015, 12, 30)),
        (1, datetime.date(2016, 1, 1), pendulum.date(2016, 1, 2)),
    ],
)
def test_offset_to_date(offset, reference_date, expected):
    """
    Test that DateStencil._offset_to_date returns the expected Date object.
    """
    offset_date = DateStencil._offset_to_date(offset, reference_date)
    assert isinstance(offset_date, pendulum.Date)
    assert offset_date == expected


def test_offset_to_date_raises_type_error():
    """
    Test that offset_to_date raises a TypeError if offset has an invalid type.
    """
    with pytest.raises(
        TypeError, match="Invalid type for offset: expected 'date' or 'int', not 'str'"
    ):
        offset_date = DateStencil._offset_to_date(
            "NOT_AN_OFFSET_OR_DATE", pendulum.date(2016, 1, 1)
        )


def test_as_date_pairs():
    """
    Test that DateStencil.as_date_pairs returns expected date pairs.
    """
    date_stencil = DateStencil(
        [
            [-1, 1],
            [pendulum.date(2016, 1, 1), 1],
            [-1, pendulum.date(2016, 1, 3)],
            [pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 3)],
        ]
    )
    reference_date = pendulum.date(2016, 1, 2)
    date_pairs = date_stencil.as_date_pairs(reference_date)
    assert len(date_pairs) == 4
    assert all(
        pair == (pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 2))
        for pair in date_pairs
    )


def test_as_date_pairs_errors():
    """
    Test that DateStencil.as_date_pairs raises an error if the stencil contains
    an invalid interval for the given reference date.
    """
    date_stencil = DateStencil([[0, pendulum.date(2016, 1, 1)]])
    with pytest.raises(InvalidDateIntervalError):
        date_pairs = date_stencil.as_date_pairs(
            reference_date=pendulum.date(2016, 1, 1)
        )


def test_as_set_of_dates():
    """
    Test that DateStencil.as_set_of_dates returns expected set of dates.
    """
    reference_date = pendulum.date(2016, 1, 7)
    date_stencil = DateStencil([[pendulum.date(2016, 1, 1), -3], -1, 0])
    expected_set = set(pendulum.date(2016, 1, d) for d in [1, 2, 3, 6, 7])
    set_of_dates = date_stencil.as_set_of_dates(reference_date)
    assert set_of_dates == expected_set


@pytest.mark.parametrize(
    "reference_date,available",
    [
        (pendulum.date(2016, 1, 5), False),
        (pendulum.date(2016, 1, 6), True),
        (pendulum.date(2016, 1, 7), False),
    ],
)
def test_dates_are_available(reference_date, available):
    """
    Test that DateStencil.dates_are_available correctly reports whether all dates in the stencil are available.
    """
    date_stencil = DateStencil([[pendulum.date(2016, 1, 2), -3], [-3, 0]])
    available_dates = [pendulum.date(2016, 1, d) for d in range(1, 6)]
    assert available == date_stencil.dates_are_available(
        reference_date, available_dates
    )


def test_date_stencil_eq():
    """
    Test that a DateStencil is equal to another DateStencil created using the same raw stencil,
    is not equal to a DateStencil created using a different raw stencil,
    and is not equal to an object that isn't a DateStencil.
    """
    date_stencil = DateStencil([-1, 0])
    assert date_stencil == DateStencil([-1, 0])
    assert date_stencil != DateStencil([-2, 0])
    assert date_stencil != "NOT_A_DATE_STENCIL"
