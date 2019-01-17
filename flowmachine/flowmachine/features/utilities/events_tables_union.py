import warnings
from typing import List

from ...core import Query
from ...core.errors import MissingDateError
from .event_table_subset import EventTableSubset


class EventsTablesUnion(Query):
    """
    Takes a list of subtables, subsets each of them
    by date and selects a specified list of columns
    from the result and unions (i.e. appends) all
    of these tables. This class is mostly used as an
    intermediate for other classes.

    Parameters
    ----------
    start, stop : str
        ISO-format date
    columns :
        list of columns to select
    tables : str or list of strings, default 'all'
        Can be a sting of a single table (with the schema)
        or a list of these. The keyword all is to select all
        subscriber tables
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    """

    def __init__(
        self,
        start,
        stop,
        *,
        columns,
        tables="all",
        hours="all",
        subscriber_subset=None,
        subscriber_identifier="msisdn",
    ):
        """

        """

        self.start = start
        self.stop = stop
        if "*" in columns and len(tables) != 1:
            raise ValueError(
                "Must give named tables when combining multiple event type tables."
            )
        self.columns = columns
        self.tables = self._parse_tables(tables)
        self.date_subsets = self._make_table_list(
            hours=hours,
            subscriber_subset=subscriber_subset,
            subscriber_identifier=subscriber_identifier,
        )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.date_subsets[
            0
        ].column_names  # Use in preference to self.columns which might be ["*"]

    def _parse_tables(self, tables):

        if isinstance(tables, str) and tables.lower() == "all":
            return [f"events.{t}" for t in self.connection.subscriber_tables]
        elif type(tables) is str:
            return [tables]
        else:
            return tables

    def _make_table_list(self, *, hours, subscriber_subset, subscriber_identifier):
        """
        Makes a list of EventTableSubset queries.
        """

        date_subsets = []
        for table in self.tables:
            try:
                sql = EventTableSubset(
                    self.start,
                    self.stop,
                    table=table,
                    columns=self.columns,
                    hours=hours,
                    subscriber_subset=subscriber_subset,
                    subscriber_identifier=subscriber_identifier,
                )
                date_subsets.append(sql)
            except MissingDateError:
                warnings.warn(
                    f"No data in {table} for {self.start}–{self.stop}", stacklevel=2
                )
        if not date_subsets:
            raise MissingDateError(self.start, self.stop)
        return date_subsets

    def _make_query(self):

        # Get the list of tables, select the relevant columns and union
        # them all
        sql = "\nUNION ALL\n".join(sd.get_query() for sd in self.date_subsets)

        return sql

    @property
    def table_name(self):
        # EventTableSubset are a simple select from events, and should not be cached
        raise NotImplementedError