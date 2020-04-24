# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime as dt

from flowmachine.utils import standardise_date, parse_datestring


class DateRange:
    """
    Represents a time period between a start date and an end date.
    """

    def __init__(self, start_date, end_date):
        self.start_date = parse_datestring(start_date)
        self.end_date = parse_datestring(end_date)
        self.start_date_as_str = standardise_date(start_date)
        self.end_date_as_str = standardise_date(end_date)

        self.one_day_past_end_date = self.end_date + dt.timedelta(days=1)
        self.one_day_past_end_date_as_str = standardise_date(self.one_day_past_end_date)

    def __repr__(self):
        return f"DatePeriod(start_date={self.start_date_as_str}, end_date={self.end_date_as_str})"
