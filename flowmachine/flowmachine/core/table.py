# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Simple utility class that represents arbitrary tables in the
database.
"""

from typing import List

from flowmachine.core.query_state import QueryStateMachine
from .errors import NotConnectedError
from .query import Query
from .subset import subset_factory

import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class Table(Query):
    """
    Provides an interface to query any table by name and (optionally)
    schema.

    Parameters
    ----------
    name : str
        Name of the table, may be fully qualified
    schema : str
        Optional if name is fully qualified
    columns : str
        Optional list of columns

    Examples
    --------

    >>> t = Table(name="calls", schema="events")
    >>> t.head()
                                id outgoing                  datetime  duration  \
    0  5wNJA-PdRJ4-jxEdG-yOXpZ     True 2016-01-01 22:38:06+00:00    3393.0
    1  5wNJA-PdRJ4-jxEdG-yOXpZ    False 2016-01-01 22:38:06+00:00    3393.0
    2  ZYK4w-9aAD2-NN7ev-MRnBp     True 2016-01-01 07:05:47+00:00    4533.0
    3  ZYK4w-9aAD2-NN7ev-MRnBp    False 2016-01-01 07:05:47+00:00    4533.0
    4  mQjOy-5eVrm-Ll5eE-P4V27     True 2016-01-01 10:18:31+00:00     422.0
    ...
    
    >>> t = Table(name="calls", schema="events", columns=["id", "duration"])
    >>> t.head()
                            id  duration
    0  5wNJA-PdRJ4-jxEdG-yOXpZ    3393.0
    1  5wNJA-PdRJ4-jxEdG-yOXpZ    3393.0
    2  ZYK4w-9aAD2-NN7ev-MRnBp    4533.0
    3  ZYK4w-9aAD2-NN7ev-MRnBp    4533.0
    4  mQjOy-5eVrm-Ll5eE-P4V27     422.0
    ...

    """

    def __init__(self, name=None, schema=None, columns=None):
        """

        """
        try:
            self.connection
        except AttributeError:
            raise NotConnectedError()

        if "." in name:
            extracted_schema, name = name.split(".")
            if schema is not None:
                if schema != extracted_schema:
                    raise ValueError("Two schema provided.")
            schema = extracted_schema
        elif schema is None:
            schema = "public"

        self.name = name
        self.schema = schema
        self.fqn = "{}.{}".format(schema, name) if schema else name
        if "." not in self.fqn:
            raise ValueError("{} is not a valid table.".format(self.fqn))
        if not self.is_stored:
            raise ValueError("{} is not a known table.".format(self.fqn))

        # Get actual columns of this table from the database
        db_columns = list(
            zip(
                *self.connection.fetch(
                    f"""SELECT column_name from INFORMATION_SCHEMA.COLUMNS
             WHERE table_name = '{self.name}' AND table_schema='{self.schema}'"""
                )
            )
        )[0]
        if (
            columns is None or columns == []
        ):  # No columns specified, setting them from the database
            columns = db_columns
        else:
            self.parent_table = Table(
                schema=self.schema, name=self.name
            )  # Point to the full table
            if isinstance(columns, str):  # Wrap strings in a list
                columns = [columns]
            logger.debug(
                f"Checking provided columns {columns} against db columns {db_columns}"
            )
            if not set(columns).issubset(db_columns):
                raise ValueError(
                    "{} are not columns of {}".format(
                        set(columns).difference(db_columns), self.fqn
                    )
                )

        # Record provided columns to ensure that md5 differs with different columns
        self.columns = columns
        super().__init__()
        # Table is immediately in a 'finished executing' state
        q_state_machine = QueryStateMachine(self.redis, self.md5)
        q_state_machine.enqueue()
        q_state_machine.execute()
        self._db_store_cache_metadata(compute_time=0)
        q_state_machine.finish()

    def __repr__(self):
        return f"<Table: '{self.schema}.{self.name}', query_id: '{self.md5}'>"

    @property
    def column_names(self) -> List[str]:
        return list(self.columns)

    def _make_query(self):
        try:
            cols = ",".join(self.columns)
        except (AttributeError, TypeError):
            cols = "*"
        return "SELECT {cols} FROM {fqn}".format(fqn=self.fqn, cols=cols)

    def get_query(self):
        with self.connection.engine.begin():
            self.connection.engine.execute(
                "UPDATE cache.cached SET last_accessed = NOW(), access_count = access_count + 1 WHERE query_id ='{}'".format(
                    self.md5
                )
            )
        return self._make_query()

    @property
    def is_stored(self):
        return self.connection.has_table(self.name, self.schema)

    @property
    def fully_qualified_table_name(self):
        return self.fqn

    def get_table(self):
        return self

    def estimated_rowcount(self, include_children=True):
        """
        Parameters
        ----------
        include_children : bool
            Set to false to exclude the rows of child tables

        Returns
        -------
        int
            An estimate of the number of rows in this table.
        """
        qur = "WITH counts AS ("
        if include_children:
            qur += """
                SELECT c.oid as oid
                    FROM pg_inherits
                    JOIN pg_class AS c ON (inhrelid=c.oid)
                    JOIN pg_class as p ON (inhparent=p.oid)
                    JOIN pg_namespace pn ON pn.oid = p.relnamespace
                    JOIN pg_namespace cn ON cn.oid = c.relnamespace
                    WHERE p.relname = '{tn}' and pn.nspname = '{sc}'
                UNION"""

        qur += """
              SELECT oid
                FROM pg_class
                WHERE oid='{sc}.{tn}'::regclass
            )
            SELECT SUM(reltuples::bigint) FROM pg_class, counts
            WHERE pg_class.oid=counts.oid
            """

        ct = self.connection.fetch(qur.format(sc=self.schema, tn=self.name))[0][0]
        return int(ct)

    def has_children(self):
        """
        Returns
        -------
        bool
            True if this table has subtables
        """
        number_child = self.connection.fetch(
            """
                            SELECT COUNT(*) as oid
                                FROM pg_inherits
                                JOIN pg_class AS c ON (inhrelid=c.oid)
                                JOIN pg_class as p ON (inhparent=p.oid)
                                JOIN pg_namespace pn ON pn.oid = p.relnamespace
                                JOIN pg_namespace cn ON cn.oid = c.relnamespace
                                WHERE p.relname = '{tn}' and pn.nspname = '{sc}'
                            """.format(
                sc=self.schema, tn=self.name
            )
        )[0][0]
        return number_child > 0

    def invalidate_db_cache(self, name=None, schema=None, cascade=True, drop=False):
        """
        Helper function for store, optionally drops this table, and (by default) any
        cached tables that depend on it, as well as removing them from
        the cache metadata table.
        
        Parameters
        ------
        name : str
            Name of the table
        schema : str
            Schema of the table
        cascade : bool
            Set to False to remove only this table from cache
        drop : bool
            Set to True to drop the table in addition to removing from cache

        """
        super().invalidate_db_cache(
            name=name, schema=schema, cascade=cascade, drop=drop
        )

    def random_sample(
        self, size=None, fraction=None, method="system_rows", estimate_count=True
    ):
        from .random import random_factory

        random_class = random_factory(Query)
        return random_class(
            query=self,
            size=size,
            fraction=fraction,
            method=method,
            estimate_count=estimate_count,
        )

    def subset(self, col, subset):
        """
        Subsets one of the columns to a specified subset of values

        Parameters
        ----------
        col : str
            Name of the column to subset, e.g. subscriber, cell etc.
        subset : list
            List of values to subset to

        Returns
        -------
        Subset
        """

        subset_class = subset_factory(Query)
        return subset_class(self, col, subset)
