# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Utility class that allows the subscriber to iterate through arbitrary groups of fields
and apply a python function to the results.
"""
from typing import List

from ...core.query import Query
from .event_table_subset import EventTableSubset


class GroupValues(Query):
    """
    Query representing groups of a certain columns
    with the values of other columns as an array.


    Parameters
    ----------
    group : str, or list of strings
        Name of the column(s) that should be grouped e.g. msisdn_from
    value : str, or list of strings
        Name of the column(s) that should be returned as an array
    start, stop : str
        start and stop times of the analysis, in ISO-format
    kwargs : dict
        Passed to flowmachine EventTableSubset

    Examples
    --------
    >>> gv = GroupValues('msisdn_from', 'datetime')
    >>> for g,v in gv:
    >>>     print((g, str(max(v))))
    ('SubscriberA', 2016-01-01 23:00:01)
    ('Subscriberb', 2016-01-01 22:12:04)
    ...

    Notes
    -----

    - In the case when the subscriber passes more than one group or more than one values the results will be an iterator of the following form:
        - (group1, group2, array(value1), array(value2))
    - This class is mostly used through the method `ColumnMap` which maps a subscriber defined python function to the output of the iterator.

    """

    def __init__(self, group, value, start, stop, **kwargs):
        """

        """

        if type(group) is str:
            self.groups = [group]
        else:
            self.groups = group
        self.total_groups = len(self.groups)

        if type(value) is str:
            self.value = [value]
        else:
            self.value = value

        self.subset = EventTableSubset(start=start, stop=stop, **kwargs)

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.groups + self.value

    def _make_query(self):

        agg_values = ["array_agg({v}) AS {v}".format(v=v) for v in self.value]

        sql = """
                SELECT
                    {groups},
                    {agg_values}
                 FROM
                    ({subset}) AS subset
                 GROUP BY
                    {groups}
              """.format(
            subset=self.subset.get_query(),
            groups=",".join(self.groups),
            agg_values=",".join(agg_values),
        )

        return sql

    def ColumnMap(self, fn):
        """
        Maps a function to each of the returned arrays, and
        returns an iterator over the results.
        
        Examples
        --------
        >>> def highest_min(date_list):
        >>>     return max([x.minute for x in date_list])
        >>> gv = GroupValues('msisdn_from', 'datetime')
        >>> cm = gv.ColumnMap(highest_min)
        >>> for c in cm:
        >>>     print(c)
        ('BKMy1nYEZpnoEA7G', 58)
        ('DzpZJ2EaVQo2X5vM', 56)
        ('Zv4W9eak2QN1M5A7', 55)
        ('NQV3J52PeYgbLm2w', 54)
        ...
        """

        return (
            (results[: self.total_groups], fn(*results[self.total_groups :]))
            for results in self
        )
