# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Custom marshmallow fields used for parsing a 'workflows.yml' file.
"""

import datetime

from marshmallow import (
    fields,
    ValidationError,
)
from prefect.schedules import CronSchedule

from autoflow.date_stencil import DateStencil, InvalidDateIntervalError


class ScheduleField(fields.String):
    """
    Custom field to deserialise a cron string as a prefect CronSchedule.
    """

    def _deserialize(
        self, value, attr, data, **kwargs
    ) -> "prefect.schedules.schedules.Schedule":
        """
        Deserialise a cron string as a cron schedule.

        Returns
        -------
        Schedule
            Prefect CronSchedule to run a flow according to the schedule
            defined by the input string.

        Raises
        ------
        ValidationError
            if the input value is not a valid cron string or None
        """
        cron_string = super()._deserialize(value, attr, data, **kwargs)
        try:
            schedule = CronSchedule(cron_string)
        except ValueError:
            raise ValidationError(f"Invalid cron string: '{cron_string}'.")
        return schedule


class DateField(fields.Date):
    """
    Custom field to deserialise a date, which will leave the input value unchanged
    if it is already a date object.
    """

    # yaml.safe_load converts iso-format dates to date objects, so we need a
    # field that can 'deserialise' dates that are already dates.
    def _deserialize(self, value, attr, data, **kwargs) -> datetime.date:
        """
        If the input value is a date object, return it unchanged.
        Otherwise use marshmallow.fields.Date to deserialise.
        """
        if isinstance(value, datetime.date):
            return value
        else:
            return super()._deserialize(value, attr, data, **kwargs)


class DateStencilField(fields.Field):
    """
    Custom field to deserialise a 'raw' date stencil (i.e. a list of dates, offsets and/or pairs)
    to a DateStencil object.
    """

    def _deserialize(self, value, attr, data, **kwargs) -> DateStencil:
        """
        Create a DateStencil object from the input value.

        Returns
        -------
        DateStencil

        Raises
        ------
        ValidationError
            if the input value is not a valid 'raw' stencil

        See also
        --------
        autoflow.date_stencil.DateStencil
        """
        try:
            return DateStencil(raw_stencil=value)
        except (TypeError, ValueError, InvalidDateIntervalError) as e:
            raise ValidationError(str(e))
