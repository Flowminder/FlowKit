# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import Collection, List, Union

from flowmachine.core.query import Query
from flowmachine.core.errors import MissingColumnsError
from flowmachine.features.utilities.subscriber_locations import BaseLocation


class CombineFirst(Query):
    """
    Given two queries 'first_query' and 'other_query', fill null or missing values in the
    result of 'first_query' using those in the result of 'other_query'.

    Somewhat analogous to pandas.DataFrame.combine_first(), except that here we
    specify the columns on which the queries will be (full outer) joined.

    Parameters
    ----------
    first_query: Query
        Query whose nulls will be filled
    other_query: Query
        Query whose values will be used to fill nulls in first_query
    join_columns: str or collection of str
        Names of columns on which queries will be joined
    combine_columns: str or collection of str
        Names of columns in which null values will be filled

    Notes
    -----
    Relevant column names are assumed to be the same in both queries
    (i.e. nulls in column 'a' of first_query are filled with values from
    column 'a' of other_query)
    """

    def __init__(
        self,
        *,
        first_query: Query,
        other_query: Query,
        join_columns: Union[str, Collection[str]],
        combine_columns: Union[str, Collection[str]],
    ):
        self.first_query = first_query
        self.other_query = other_query

        # Ensure 'join_columns' attribute is always an ordered list
        if isinstance(join_columns, str):
            self._join_columns = [join_columns]
        else:
            self._join_columns = list(sorted(set(join_columns)))

        # Ensure 'combine_columns' attribute is always an ordered list
        if isinstance(combine_columns, str):
            self._combine_columns = [combine_columns]
        else:
            self._combine_columns = list(sorted(set(combine_columns)))

        # Check there is no overlap between 'join_columns' and 'combine_columns'
        if not set(self._join_columns).isdisjoint(self._combine_columns):
            raise ValueError(
                "Column names must not appear in both the 'join_columns' and 'combine_columns' arguments of CombineFirst"
            )

        # Both queries must include all of 'join_columns' and 'combine_columns'
        if not set(self.column_names).issubset(first_query.column_names):
            raise MissingColumnsError(
                first_query, set(self.column_names).difference(first_query.column_names)
            )
        if not set(self.column_names).issubset(other_query.column_names):
            raise MissingColumnsError(
                other_query, set(self.column_names).difference(other_query.column_names)
            )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self._join_columns + self._combine_columns

    def _make_query(self):
        join_columns_string = ",\n".join(self._join_columns)
        coalesce_string = ",\n".join(
            f"COALESCE(l.{col}, r.{col}) AS {col}" for col in self._combine_columns
        )

        sql = f"""
        SELECT {join_columns_string},
               {coalesce_string}
        FROM ({self.first_query.get_query()}) l
        FULL OUTER JOIN ({self.other_query.get_query()}) r
        USING ({join_columns_string})
        """

        return sql


class CoalescedLocation(BaseLocation, CombineFirst):
    """
    Wrapper around CombineFirst for the special case of coalescing locations
    from two subscriber location queries. For each subscriber appearing in
    either query, this will return their location from preferred_locations if
    they appear with non-null location in the result of preferred_locations, or
    their location from fallback_locations otherwise.

    Parameters
    ----------
    preferred_locations: Query
        Subscriber locations to be chosen preferentially
    fallback_locations: Query
        Subscriber locations to be assigned for subscribers who do not have a
        location in preferred_locations

    See Also
    --------
    CombineFirst
    """

    def __init__(self, preferred_locations: Query, fallback_locations: Query):
        self.spatial_unit = preferred_locations.spatial_unit
        super().__init__(
            first_query=preferred_locations,
            other_query=fallback_locations,
            join_columns="subscriber",
            combine_columns=self.spatial_unit.location_id_columns,
        )
