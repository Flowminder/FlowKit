# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
import datetime
import pendulum
from unittest.mock import patch

from flowtrigger.utils import *


def test_get_output_filename():
    """
    Test that get_output_filename returns the expected filename when a tag is provided.
    """
    now = pendulum.parse("2016-01-01")
    with patch("pendulum.now", lambda x: now):
        output_filename = get_output_filename("dummy_filename.suffix", "DUMMY_TAG")
    assert output_filename == "dummy_filename__DUMMY_TAG__20160101T000000Z.suffix"


def test_get_output_filename_without_tag():
    """
    Test that get_output_filename returns the expected filename when a tag is not provided.
    """
    now = pendulum.parse("2016-01-01")
    with patch("pendulum.now", lambda x: now):
        output_filename = get_output_filename("dummy_filename.suffix")
    assert output_filename == "dummy_filename__20160101T000000Z.suffix"


def test_get_different_params_hash_for_different_parameters():
    """
    Test that get_params_hash gives different results for different parameters.
    """
    hash1 = get_params_hash({"DUMMY_PARAM_1": 1})
    hash2 = get_params_hash({"DUMMY_PARAM_2": 2})
    assert hash1 != hash2


def test_params_hash_independent_of_order():
    """
    Test that get_params_hash gives the same result if order of parameters changes.
    """
    hash1 = get_params_hash({"DUMMY_PARAM_1": 1, "DUMMY_PARAM_2": 2})
    hash2 = get_params_hash({"DUMMY_PARAM_2": 2, "DUMMY_PARAM_1": 1})
    assert hash1 == hash2


def test_get_params_hash_can_handle_dates():
    """
    Test that get_params_hash returns a result if the parameters contain dates,
    and that different dates produce different hashes.
    """
    hash1 = get_params_hash({"DUMMY_PARAM": pendulum.date(2016, 1, 1)})
    hash2 = get_params_hash({"DUMMY_PARAM": pendulum.date(2016, 1, 2)})
    assert hash1 != hash2


@pytest.mark.parametrize(
    "offset,reference_date,expected",
    [
        (
            datetime.date(2016, 1, 2),
            pendulum.date(2016, 1, 1),
            pendulum.date(2016, 1, 2),
        ),
        (-1, pendulum.date(2016, 1, 1), pendulum.date(2015, 12, 31)),
        (1, datetime.date(2016, 1, 1), pendulum.date(2016, 1, 2)),
    ],
)
def test_offset_to_date(offset, reference_date, expected):
    """
    Test that offset_to_date returns the expected Date object.
    """
    offset_date = offset_to_date(offset, reference_date)
    assert isinstance(offset_date, pendulum.Date)
    assert offset_date == expected


def test_offset_to_date_raises_type_error():
    """
    Test that offset_to_date raises a TypeError if offset has an invalid type.
    """
    with pytest.raises(
        TypeError, match="Invalid type for offset: expected 'date' or 'int', not 'str'"
    ):
        offset_date = offset_to_date("NOT_AN_OFFSET_OR_DATE", pendulum.date(2016, 1, 1))


def test_stencil_to_date_pairs():
    """
    Test that stencil_to_date_pairs returns expected date pairs.
    """
    stencil = [
        pendulum.date(2016, 1, 1),
        -4,
        [pendulum.date(2016, 1, 2), pendulum.date(2016, 1, 3)],
        [-3, -1],
        [pendulum.date(2016, 1, 1), -1],
        [0, 0],
    ]
    reference_date = pendulum.date(2016, 1, 7)
    expected_date_pairs = [
        (pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 1)),
        (pendulum.date(2016, 1, 3), pendulum.date(2016, 1, 3)),
        (pendulum.date(2016, 1, 2), pendulum.date(2016, 1, 3)),
        (pendulum.date(2016, 1, 4), pendulum.date(2016, 1, 6)),
        (pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 6)),
        (pendulum.date(2016, 1, 7), pendulum.date(2016, 1, 7)),
    ]
    date_pairs = stencil_to_date_pairs(stencil, reference_date)
    assert date_pairs == expected_date_pairs


@pytest.mark.parametrize(
    "stencil,error",
    [
        (["BAD_ELEMENT"], TypeError),
        ([[-1, "BAD_ELEMENT"]], TypeError),
        (-1, TypeError),
        ([[-3, -2, -1]], ValueError),
        ([[-1, -2]], InvalidDatePairError),
    ],
)
def test_stencil_to_date_pairs_errors(stencil, error):
    """
    Test that stencil_to_date_pairs raises the correct errors for invalid stencils.
    """
    reference_date = pendulum.date(2016, 1, 1)
    with pytest.raises(error):
        date_pairs = stencil_to_date_pairs(stencil, reference_date)


def test_stencil_to_set_of_dates():
    """
    Test that stencil_to_set_of_dates returns expected set of dates.
    """
    reference_date = pendulum.date(2016, 1, 7)
    stencil = [[pendulum.date(2016, 1, 1), -3], -1, 0]
    expected_set = set(pendulum.date(2016, 1, d) for d in [1, 2, 3, 4, 6, 7])
    set_of_dates = stencil_to_set_of_dates(stencil, reference_date)
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
    stencil = [[pendulum.date(2016, 1, 2), -4], [-3, -1]]
    available_dates = [pendulum.date(2016, 1, d) for d in range(1, 6)]
    assert available == dates_are_available(stencil, reference_date, available_dates)
