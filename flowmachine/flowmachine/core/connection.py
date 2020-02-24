# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Class(es) representing our connection to the database, along with utilities
regarding the database.
"""
import os
import datetime
import warnings
from _md5 import md5
from collections import defaultdict

from typing import Dict, List, Optional

import sqlalchemy

from urllib.parse import quote_plus as urlquote


from cachetools import cached, TTLCache

from structlog import get_logger

logger = get_logger(__name__)


class Connection:
    """
    Establishes a connection with the database and provide methods for
    fetching data.

    Parameters
    -----------
    port, user, password, host, database, conn_str: str
        Connection info for the database, as used by sqlalchemy.
        Provide either port, user, password, host and database or conn_str.
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
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        database: Optional[str] = None,
        pool_size: int = 5,
        overflow: int = 10,
        conn_str: Optional[str] = None,
    ) -> None:
        if conn_str is None:
            if any(arg is None for arg in (port, user, password, host, database)):
                raise ValueError(
                    "If conn_str is not given then all of the arguments port, user, password, host, database must be provided."
                )
            else:
                conn_str = (
                    f"postgresql://{user}:{urlquote(password)}@{host}:{port}/{database}"
                )
        else:
            if any(arg is not None for arg in (port, user, password, host, database)):
                raise ValueError(
                    "If conn_str is given, none of the arguments port, user, password, host, database are allowed."
                )

        self.app_name = "flowmachine"
        try:
            self.app_name = "-".join((self.app_name, os.getlogin()))
        except (FileNotFoundError, OSError):
            logger.info(
                f"Couldn't get username for application name, using '{self.app_name}'"
            )
        connect_args = {"application_name": self.app_name}
        self.engine = sqlalchemy.create_engine(
            conn_str,
            echo=False,
            strategy="threadlocal",
            pool_size=pool_size,
            max_overflow=overflow,
            pool_timeout=None,
            connect_args=connect_args,
        )
        # Unique-to-db id for this connection, to allow use of a common redis instance with
        # multiple databases
        conn_id = md5(str(self.engine.url.host).encode())
        conn_id.update(str(self.engine.url.port).encode())
        conn_id.update(str(self.engine.url.database).encode())
        self.conn_id = conn_id.hexdigest()

        self.max_connections = pool_size + overflow
        if self.max_connections > os.cpu_count():
            warnings.warn(
                f"Maximum number of connections (pool size + overflow = {self.max_connections}) is greater than the available cpu cores ({os.cpu_count()})."
            )
        self.__check_flowdb_version()

    def __check_flowdb_version(self):
        """
        Private method which checks that the version of `flowdb` currently deployed
        is compatible with this version of flowmachine.
        """

        from ..versions import __version__, __min_flowdb_version__

        query_output = self.fetch("SELECT * FROM flowdb_version();")
        flowdb_version = query_output[0][0]
        if __min_flowdb_version__ > flowdb_version.replace("v", ""):
            raise OSError(
                f"The current version of Flowdb ({flowdb_version}) is not supported. "
                f"FlowMachine ({__version__}) only supports "
                f"Flowdb {__min_flowdb_version__} or higher."
            )

    def fetch(self, query: str):
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

    def has_table(self, name: str, schema: Optional[str] = None) -> bool:
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

    @property
    def available_dates(self) -> Dict[str, List[datetime.date]]:
        """
        Returns
        -------
        defaultdict of lists
            Dict with tables as keys, containing lists of dates which are present

        """
        return self._available_dates()

    @cached(
        TTLCache(256, 120)
    )  # Reasonable number of possible combinations, but fairly big output to cache
    # two minutes seems reasonable to balance db access against speed boost
    def _available_dates(self) -> Dict[str, List[datetime.date]]:
        """
        Returns
        -------
        defaultdict of lists
            Dict with tables as keys, containing lists of dates which are present

        """

        return defaultdict(
            list,
            self.fetch(
                "SELECT cdr_type, array_agg(distinct cdr_date) FROM etl.etl_records WHERE state='ingested' GROUP BY cdr_type"
            ),
        )

    def min_date(self, table: str = "calls") -> datetime.date:
        """
        Finds the minimum date in the given events table.

        Parameters
        ----------
        table : str, default 'calls'
            The table to check

        Returns
        -------
        date
        """
        if table == "all":
            return min({x for row in self.available_dates.values() for x in row})
        return min(self.available_dates[table])

    def max_date(self, table: str = "calls") -> datetime.date:
        """
        Finds the maximum date in the given events table.

        Parameters
        ----------
        table : str, default 'calls'
            The table to check

        Returns
        -------
        date
        """
        if table == "all":
            return max({x for row in self.available_dates.values() for x in row})
        return max(self.available_dates[table])

    @property
    def location_table(self) -> str:
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
