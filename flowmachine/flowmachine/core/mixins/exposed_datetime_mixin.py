# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Mixin that exposes start_date and end_date internally as datetime objects
"""


from datetime import datetime
from flowmachine.utils import standardise_date


class ExposedDatetimeMixin:
    """
    Mixin that adds a getter + setter for `start_date` and `end_date` such that they are exposed internally to
    a class as datetime objects (`_start_dt` and `_end_dt` respectively), but externally as strings.
    """

    @property
    def start_date(self):
        return standardise_date(self._start_dt)

    @start_date.setter
    def start_date(self, value):
        self._start_dt = datetime.strptime(standardise_date(value), "%Y-%m-%d %H:%M:%S")

    @property
    def end_date(self):
        return standardise_date(self._end_dt)

    @end_date.setter
    def end_date(self, value):
        self._end_dt = datetime.strptime(standardise_date(value), "%Y-%m-%d %H:%M:%S")
