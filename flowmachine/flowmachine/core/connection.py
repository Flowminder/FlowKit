# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Class(es) representing our connection to the database, along with utilities
regarding the database.
"""
import os
import re
import datetime
import warnings

from typing import Tuple, Union, Dict, List

import sqlalchemy

from urllib.parse import quote_plus as urlquote


import flowmachine
from cachetools import cached, TTLCache

from structlog import get_logger

logger = get_logger(__name__)


class Connection:
    """
    Establishes a connection with the database and provide methods for
    fetching data.

    Parameters
    -----------
    port, user, password, host, database : str
        Connection info for the database, as used by sqlalchemy.
    pool_size : int, optional
        Number of connections to the db to use
    overflow : int, optional
        Number of connections to the db to open temporarily

    Notes
    -----
    sqlalchemy will open at most `pool_size+overflow` connections to the db
    and requests for more will time out rapidly. sqlalchemy will not immediately
    open `pool_size` connections, but will always keep that many open once they
    have been. You will, ordinarily be OK to ignore this setting.
    """

    def __init__(
        self,
        *,
        port=None,
        user=None,
        password=None,
        host=None,
        database=None,
        pool_size=5,
        overflow=10,
        conn_str=None,
    ):
        if conn_str is None:
            if (
                (port is None)
                or (user is None)
                or (password is None)
                or (host is None)
                or (database is None)
            ):
                raise ValueError(
                    "If conn_str is not given then all of the arguments port, user, password, host, database must be provided."
                )
        else:
            if (
                (port is not None)
                or (user is not None)
                or (password is not None)
                or (host is not None)
                or (database is not None)
            ):
                raise ValueError(
                    "If conn_str is given, none of the arguments port, user, password, host, database are allowed."
                )

        if conn_str is None:
            conn_str = "postgresql://{user}:{password}@{host}:{port}/{database}".format(
                user=user,
                password=urlquote(password),
                host=host,
                port=port,
                database=database,
            )
        else:
            pass

        app_name = "flowmachine"
        try:
            app_name = "-".join((app_name, os.getlogin()))
        except (FileNotFoundError, OSError):
            logger.info(
                "Couldn't get username for application name, using 'flowmachine'"
            )
        connect_args = {"application_name": app_name}
        self.engine = sqlalchemy.create_engine(
            conn_str,
            echo=False,
            strategy="threadlocal",
            pool_size=pool_size,
            max_overflow=overflow,
            pool_timeout=None,
            connect_args=connect_args,
        )

        self.inspector = sqlalchemy.inspect(self.engine)
        self.max_connections = pool_size + overflow
        if self.max_connections > os.cpu_count():
            warnings.warn(
                "Maximum number of connections (pool size + overflow = {}) is ".format(
                    self.max_connections
                )
                + "greater than the available cpu cores ({}).".format(os.cpu_count())
            )
        self.__check_flowdb_version()

    def __check_flowdb_version(self):
        """
        Private method that checks what is the version
        of `flowdb` currently deployed.
        """

        from ..__init__ import __version__, __flowdb_version__

        query = "SELECT * FROM flowdb_version();"
        for i in self.fetch(query):
            flowdb_version = i[0]
            if __flowdb_version__ > flowdb_version.replace("v", ""):
                raise EnvironmentError(
                    "The current version of Flowdb "
                    + "(%s) is not supported. " % flowdb_version
                    + "FlowMachine (%s) only supports " % __version__
                    + "Flowdb %s or higher." % __flowdb_version__
                )

    def fetch(self, query):
        """
        Parameters
        ----------
        query : str
            SQL query string.

        Fetches a query from the database and return as a list of lists
        """
        # We actually bypass sqlalachemy here and use the raw dbapi
        # connection, because sqlalchemy will silently truncate/coerce to
        # strings output from explain (format XXXX).
        con = self.engine
        with con.connect() as c2:
            with c2.begin():
                curs = c2.connection.cursor()
                curs.execute(query)
                rs = curs.fetchall()
        return rs

    def tables(self, regex=None):
        """
        Parameters
        ----------
        regex : str
            Optional regular expression

        Returns
        -------
        list
            A list of table names that follow the given regular
            expression or if no argument is passed returns all table names
        """
        tables = self.inspector.get_table_names()
        if regex is None:
            return tables
        return [t for t in tables if re.search(regex, t)]

    def has_table(self, name, schema=None):
        """
        Check if a table exists in the database.

        Parameters
        ----------
        name : str
            Name of the table
        schema : str, default None
            Check only this schema, if none look for the table in 
            any schema

        Returns
        -------
        bool
         true if the given table exists, otherwise false.
        """
        exists_query = """
        SELECT * FROM information_schema.tables 
            WHERE table_name='{}'
        """.format(
            name
        )
        if schema is not None:
            exists_query = "{} AND table_schema='{}'".format(exists_query, schema)
        exists_query = "SELECT EXISTS({})".format(exists_query)
        with self.engine.begin():
            return self.engine.execute(exists_query).fetchall()[0][0]

    def _has_date(self, date, table="calls", schema="events"):
        """
        Returns true if this days CDR data has been ingested.
        Note that it rather crudely assumes that if there is a
        single data point for that day then the date exists. 

        Parameters
        ----------
        schema
        date : datetime
            Datetime object to look for
        table : str, default 'calls'
            The table to look at.
        schema : str
            Schema the table belongs to, defaults to 'events'

        Returns
        -------
        bool
            True if there is a table matching this date.
        """

        stop_date = date + datetime.timedelta(days=1)
        sql = f"""
                SELECT
                    *
                FROM
                    {schema}.{table}
                WHERE
                    datetime >= '{date}'::timestamptz
                  AND
                    datetime  < '{stop_date}'::timestamptz
                LIMIT 1
              """
        return len(self.fetch(sql)) == 1

    @cached(
        TTLCache(5, 120)
    )  # Only a few base tables to cache, two minutes seems reasonable to balance db access against speed boost
    def _known_dates(self, table, schema):
        """
        Get the dates of all subtables for a table.

        Parameters
        ----------
        table : str
            Name of table to get dated subtables of.
        schema : str
            Schema the table belongs to

        Returns
        -------
        list of datetime

        """
        # Last eight characters of the table name are the date
        qur = f"""SELECT SUBSTRING(table_name from '.{{8}}$') as date
                                    FROM information_schema.tables
                                    WHERE table_name like '{table}_%%'
                                    AND table_schema='{schema}'
                                    ORDER BY date;"""
        return sorted(
            datetime.datetime.strptime(x, "%Y%m%d")
            for x, in self.fetch(qur)
            if x.isnumeric()
        )

    @cached(
        TTLCache(1024, 120)
    )  # Many dates to cache, two minutes seems reasonable to balance db access against speed boost
    def has_date(self, date, table, strictness=2, schema="events"):
        """
        Check against the database tables with varying strictness whether there
        is data present for the provided date.

        Parameters
        ----------
        date : datetime
            Date to check for
        table : str
            Table under the event schema
        strictness : {0, 1, 2}
            Three levels of strictness are available - 0 checks only that the
            table _exists_. 1 checks the approximate number of rows in the table
            is non-zero, and 2 tries to retrieve some rows from the table.
        schema : str
            Schema the table belongs to

        Returns
        -------
        bool
            True if the date is available based on strictness criteria
        """
        date = datetime.datetime(date.year, date.month, date.day)
        valid_stricts = {0, 1, 2}
        if strictness not in valid_stricts:
            raise ValueError(
                "Strictness must be in {!r}, got {}.".format(valid_stricts, strictness)
            )
        known_dates = self._known_dates(table, schema)
        if date not in known_dates:
            return False
        if (
            strictness > 0
            and flowmachine.core.Table(
                f"{table}_{date.strftime('%Y%m%d')}", schema
            ).estimated_rowcount()
            == 0
        ):
            return False
        if strictness > 1 and not self._has_date(date, table, schema):
            return False

        return True

    @cached(
        TTLCache(256, 120)
    )  # Reasonable number of possible combinations, but fairly big output to cache
    # two minutes seems reasonable to balance db access against speed boost
    def available_dates(
        self,
        start: Union[None, str] = None,
        stop: Union[None, str] = None,
        table: Union[str, Tuple[str]] = "calls",
        strictness: int = 0,
        schema: str = "events",
    ) -> Dict[str, List[datetime.date]]:
        """

        Parameters
        ----------
        start : str, optional
            If specified, list only available dates after this one (inclusive) as iso format date string
        stop : str, optional
            If specified, list only available dates before this one (inclusive) as iso format date string
        table : str, or tuple of str, default 'calls'
            Name(s) of tables to check, checks all tables listed under self.subscriber_tables if 'all' is passed.
        strictness : {0, 1, 2}
            Three levels of strictness are available - 0 checks only that the
            table _exists_. 1 checks the approximate number of rows in the table
            is non-zero, and 2 checks performs a select on the actual table.
        schema : str
            Schema the table belongs to, defaults to 'events'

        Returns
        -------
        dict of lists
            Dict with tables as keys, containing lists of dates which are present

        """

        if isinstance(table, str) and table.lower() == "all":
            table = tuple(self.subscriber_tables)
        tables = table if isinstance(table, tuple) else [table]
        try:
            if not all(
                isinstance(x, str) for x in table
            ):  # Only strings or iterables of strings
                raise TypeError
        except (
            TypeError
        ):  # Capture of _tuples_ of strings is implicit because anything
            raise TypeError(  # that got here has been wrapped in a list f it wasn't a tuple
                f"Argument 'table' must be a string or tuple of strings. Got: {table}"
            )
        available = {}
        for table in tables:
            dates = self._known_dates(table, schema)
            try:
                dates = [
                    date
                    for date in dates
                    if date >= datetime.datetime.strptime(start, "%Y-%m-%d")
                ]
            except TypeError:
                pass  # No start date given
            try:
                dates = [
                    date
                    for date in dates
                    if date <= datetime.datetime.strptime(stop, "%Y-%m-%d")
                ]
            except TypeError:
                pass  # No stop date given
            available[table] = [
                date for date in dates if self.has_date(date, table, strictness, schema)
            ]

        return available

    def columns(self, tables):
        """
        Parameters
        ----------
        tables : str or list of str
            Tables to get column names for

        Returns
        -------
        dict
            A dictionary of column names for each table name passed
        """
        if isinstance(tables, str):
            tables = [tables]

        columns = {}
        for table in self.tables():
            if (tables is None) or (table in tables):
                columns[table] = []
                for column in self.inspector.get_columns(table):
                    columns[table].append(column["name"])

        return columns

    def min_date(self, table="calls", strictness=0, schema="events"):
        """
        Finds the minimum date in the given events table.

        Parameters
        ----------
        table : str, default 'calls'
            The table to check
        strictness : {0, 1, 2}
            Three levels of strictness are available - 0 checks only that the
            table _exists_. 1 checks the approximate number of rows in the table
            is non-zero, and 2 checks performs a select on the actual table.
        schema : str
            Schema the table belongs to, defaults to 'events'

        Returns
        -------
        datetime
        """
        if table == "all":
            return min(
                {
                    x
                    for row in self.available_dates(
                        table=table, strictness=strictness, schema=schema
                    ).values()
                    for x in row
                }
            )
        return min(
            self.available_dates(table=table, strictness=strictness, schema=schema)[
                table
            ]
        )

    def max_date(self, table="calls", strictness=0, schema="events"):
        """
        Finds the maximum date in the given events table.

        Parameters
        ----------
        table : str, default 'calls'
            The table to check
        strictness : {0, 1, 2}
            Three levels of strictness are available - 0 checks only that the
            table _exists_. 1 checks the approximate number of rows in the table
            is non-zero, and 2 checks performs a select on the actual table.
        schema : str
            Schema the table belongs to, defaults to 'events'

        Returns
        -------
        datetime
        """
        if table == "all":
            return max(
                {
                    x
                    for row in self.available_dates(
                        table=table, strictness=strictness, schema=schema
                    ).values()
                    for x in row
                }
            )
        return max(
            self.available_dates(table=table, strictness=strictness, schema=schema)[
                table
            ]
        )

    @property
    def location_table(self):
        """
        Get the name of the table referenced by the location_id field of
        the events tables.

        Returns
        -------
        str
            The fully qualified name of the table location_id references
        """
        return self.fetch("select location_table();")[0][0]

    @property
    def available_tables(self):
        return self.fetch("select * from available_tables();")

    @property
    def subscriber_tables(self):
        return [
            table
            for table, locations, subscribers, counterparts in self.available_tables
            if subscribers and counterparts
        ]

    @property
    def location_tables(self):
        return [
            table
            for table, locations, subscribers, counterparts in self.available_tables
            if locations
        ]

    def close(self):
        """
        Close the connection
        """
        self.engine.close()
