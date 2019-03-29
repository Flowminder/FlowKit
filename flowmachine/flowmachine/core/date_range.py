# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime as dt


class DateRange:
    """
    Represents a time period between a start date and an end date.
    """

    def __init__(self, start_date, end_date):
        self.start_date = self._parse_date(start_date)
        self.end_date = self._parse_date(end_date)
        self.start_date_as_str = self.start_date.strftime("%Y-%m-%d")
        self.end_date_as_str = self.end_date.strftime("%Y-%m-%d")

        self.one_day_past_end_date = self.end_date + dt.timedelta(days=1)
        self.one_day_past_end_date_as_str = self.one_day_past_end_date.strftime(
            "%Y-%m-%d"
        )

    def __repr__(self):
        return f"DatePeriod(start_date={self.start_date_as_str}, end_date={self.end_date_as_str})"

    def _parse_date(self, input_date):
        if isinstance(input_date, dt.date):
            if isinstance(input_date, dt.datetime):
                # a bit of gymnastics because dt.date is a subtype of dt.datetime...
                raise TypeError(
                    "Date must be an instance of datetime.date, but got datetime.datetime"
                )
            else:
                return input_date
        elif isinstance(input_date, str):
            try:
                return dt.datetime.strptime(input_date, "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(
                    f"Date string must represent a valid date in the format 'YYYY-MM-DD'. Got: '{input_date}'"
                )
        else:
            raise TypeError(
                f"Date must be a string of the format YYYY-MM-DD or a datetime.date object. Got: {type(input_date)}"
            )
