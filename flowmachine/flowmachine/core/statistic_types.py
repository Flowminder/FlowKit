# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from enum import StrEnum


class Statistic(StrEnum):
    """
    Valid statistics for use with postgres.

    Convert a column name to a stat by doing f"{statistic:column_name}".

    For columns defined as a variable, do f"{statistic:'{column_name_variable}'}"
    """

    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MAX = "max"
    MIN = "min"
    MEDIAN = "median"
    STDDEV = "stddev"
    VARIANCE = "variance"
    MODE = "mode"

    def __format__(self, column_name=""):
        """
        Get a postgres statistics function by name.

        Parameters
        ----------
        column_name : str
            Column to aggregate

        Returns
        -------
        str
            A postgres format function call string

        """
        if column_name == "":
            return str(self.value)
        if self == "mode":
            return f"pg_catalog.mode() WITHIN GROUP (ORDER BY {column_name})"
        elif self == "mode":
            return f"pg_catalog.mode() WITHIN GROUP (ORDER BY {column_name}"
        elif self == "median":
            return f"percentile_cont(0.5) WITHIN GROUP (ORDER BY {column_name})"
        else:
            return f"{self}({column_name})"


class StringAggregate(StrEnum):
    COUNT = "count"
    MODE = "mode"
    ARRAY = "array"
    ARRAY_DESC = "array desc"

    def __format__(self, column_name=""):
        if column_name == "":
            return str(self.value)
        elif self == "array":
            return f"array_agg({column_name} ORDER BY {column_name})"
        elif self == "array desc":
            return f"array_agg({column_name} ORDER BY {column_name} DESC)"
        else:
            return f"{Statistic(self):{column_name}}"
