# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# This file contains custom definitions of marshmallow fields for use
# by the flowmachine query schemas.

from marshmallow import fields
from marshmallow.validate import Range, Length, OneOf


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
