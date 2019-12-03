# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Defines the AvailableDatesSensorSchema class, for loading configuration parameters for the available dates sensor.
"""

from marshmallow import (
    fields,
    Schema,
    validate,
)

from autoflow.parser.custom_fields import ScheduleField
from autoflow.parser.workflow_config_schema import WorkflowConfigSchema


class AvailableDatesSensorSchema(Schema):
    """
    Schema for configuration parameters for the available dates sensor.

    Fields
    ------
    schedule : str or None
        Cron string describing the schedule on which the sensor should check
        available dates, or None for no schedule.
    cdr_types : list of str, optional
        Subset of CDR types for which available dates should be found.
        If not provided, defaults to None.
    workflows : list of Nested(WorkflowConfigSchema)
        List of workflows (and associated parameters) that the sensor should run.
    """

    schedule = ScheduleField(required=True, allow_none=True)
    cdr_types = fields.List(
        fields.String(validate=validate.OneOf(["calls", "sms", "mds", "topups"])),
        missing=None,
    )
    workflows = fields.List(fields.Nested(WorkflowConfigSchema), required=True)
