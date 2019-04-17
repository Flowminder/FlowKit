# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# This file contains custom definitions of marshmallow fields for use
# by the flowmachine query schemas.

from marshmallow import fields
from marshmallow.validate import Range, Length, OneOf


class AggregationUnit(fields.String):
    """
    A string representing an aggregation unit (for example: "admin0", "admin1", "admin2", ...)
    """

    def __init__(self, required=True, **kwargs):
        validate = OneOf(["admin0", "admin1", "admin2", "admin3"])
        super().__init__(required=required, validate=validate, **kwargs)


class EventTypes(fields.List):
    """
    A string representing an event type, for example "calls", "sms", "mds", "topups".
    """

    def __init__(self, required=False, validate=None, **kwargs):
        if validate is not None:
            raise ValueError(
                "The EventTypes field provides its own validation "
                "and thus does not accept a the 'validate' argument."
            )

        super().__init__(
            fields.String(validate=OneOf(["calls", "sms", "mds", "topups"])),
            required=required,
            validate=Length(min=1),
            allow_none=True,
            **kwargs,
        )


class TotalBy(fields.String):
    """
    A string representing a period type, e.g. "day"
    """

    def __init__(self, required=False, **kwargs):
        validate = OneOf(
            ["second", "minute", "hour", "day", "month", "year"]
        )  # see total_network_objects.py
        super().__init__(required=required, validate=validate, **kwargs)


class AggregateBy(fields.String):
    """
    A string representing a period type, e.g. "day"
    """

    def __init__(self, required=False, validate=None, **kwargs):
        if validate is not None:
            raise ValueError(
                "The AggregateBy field provides its own validation "
                "and thus does not accept a the 'validate' argument."
            )

        validate = OneOf(
            ["second", "minute", "hour", "day", "month", "year", "century"]
        )  # see total_network_objects.py
        super().__init__(required=required, validate=validate, **kwargs)


class Statistic(fields.String):
    """
    A string representing a statistic type, e.g. "median"
    """

    def __init__(self, required=True, validate=None, **kwargs):
        if validate is not None:
            raise ValueError(
                "The Statistic field provides its own validation "
                "and thus does not accept a the 'validate' argument."
            )

        validate = OneOf(
            ["avg", "max", "min", "median", "mode", "stddev", "variance"]
        )  # see total_network_objects.py
        super().__init__(required=required, validate=validate, **kwargs)


class SubscriberSubset(fields.String):
    """
    Represents a subscriber subset. This can either be a string representing
    a query_id or `None`, meaning "all subscribers".
    """

    def __init__(self, required=False, allow_none=True, validate=None, **kwargs):
        if validate is not None:
            raise ValueError(
                "The SubscriberSubset field provides its own validation "
                "and thus does not accept a the 'validate' argument."
            )

        super().__init__(
            required=required, allow_none=allow_none, validate=OneOf([None]), **kwargs
        )


class TowerHourOfDayScores(fields.List):
    """
    A list of length 24 containing numerical scores in the range [-1.0, +1.0],
    """

    def __init__(self, **kwargs):
        super().__init__(
            fields.Float(validate=Range(min=-1.0, max=1.0)),
            validate=Length(equal=24),
            **kwargs,
        )


class TowerDayOfWeekScores(fields.Dict):
    """
    A dictionary mapping days of the week ("monday", "tuesday" etc.) to
    numerical scores in the range [-1.0, +1.0].
    """

    def __init__(self, **kwargs):
        days_of_week = [
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        ]
        super().__init__(
            keys=fields.String(validate=OneOf(days_of_week)),
            values=fields.Float(validate=Range(min=-1.0, max=1.0)),
            **kwargs,
        )


class DFSMetric(fields.String):
    """
    A string representing a DFS metric (for example: "amount", "commission", "fee", "discount")
    """

    def __init__(self, required=True, validate=None, **kwargs):
        if validate is not None:
            raise ValueError(
                "The DFSMetric field provides its own validation and"
                "thus does not accept a the 'validate' argument."
            )

        validate = OneOf(["amount", "commission", "fee", "discount"])
        super().__init__(required=required, validate=validate, **kwargs)
