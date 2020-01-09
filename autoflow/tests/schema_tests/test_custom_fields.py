# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

import datetime

from marshmallow import ValidationError
from prefect.schedules import Schedule

from autoflow.date_stencil import DateStencil
from autoflow.parser.custom_fields import (
    DateField,
    DateStencilField,
    ScheduleField,
)


def test_schedule_field():
    """
    Test that ScheduleField deserialises a cron string to a Schedule object.
    """
    schedule = ScheduleField().deserialize("0 0 * * *")
    assert isinstance(schedule, Schedule)
    assert schedule.clocks[0].cron == "0 0 * * *"


def test_schedule_field_none():
    """
    Test that ScheduleField deserialises None to None.
    """
    schedule = ScheduleField(allow_none=True).deserialize(None)
    assert schedule is None


def test_schedule_field_validation_error():
    """
    Test that ScheduleField raises a ValidationError for an invalid cron string.
    """
    with pytest.raises(ValidationError) as exc_info:
        schedule = ScheduleField().deserialize("NOT A CRON STRING")
    assert "Invalid cron string: 'NOT A CRON STRING'." in exc_info.value.messages


def test_date_field_deserialises_date():
    """
    Test that DateField can deserialise a date object.
    """
    input_date = datetime.date(2016, 1, 1)
    deserialised_date = DateField().deserialize(input_date)
    assert deserialised_date == input_date


def test_date_field_deserialises_string():
    """
    Test that DateField can deserialise an iso date string to a date object.
    """
    deserialised_date = DateField().deserialize("2016-01-01")
    assert deserialised_date == datetime.date(2016, 1, 1)


def test_date_stencil_field():
    """
    Test that DateStencilField deserialises a raw stencil to a DateStencil object.
    """
    raw_stencil = [[datetime.date(2016, 1, 1), datetime.date(2016, 1, 3)], [-2, -1], 0]
    date_stencil = DateStencilField().deserialize(raw_stencil)
    assert isinstance(date_stencil, DateStencil)
    assert date_stencil._intervals == (
        (datetime.date(2016, 1, 1), datetime.date(2016, 1, 3)),
        (-2, -1),
        (0, 1),
    )


@pytest.mark.parametrize(
    "raw_stencil,message",
    [
        ("NOT_A_LIST", "N is not an integer or date."),
        (["BAD_ELEMENT"], "BAD_ELEMENT is not an integer or date."),
        ([[-1, "BAD_ELEMENT"]], "BAD_ELEMENT is not an integer or date."),
        (
            [[-3, -2, -1]],
            "Expected date interval to have length 2 (in format [start, end]), but got sequence of length 3.",
        ),
        ([[-1, -2]], "Date stencil contains invalid interval (-1, -2)."),
    ],
)
def test_date_stencil_field_validation_error(raw_stencil, message):
    """
    Test that DateStencilField raises a ValidationError if the raw stencil is not valid.
    """
    with pytest.raises(ValidationError) as exc_info:
        date_stencil = DateStencilField().deserialize(raw_stencil)
    assert message in exc_info.value.messages
