# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
This is the base class that defines any query on our database.  It simply
defines methods that returns the query as a string and as a pandas dataframe.

"""

import os
import pickle
import logging
import weakref
from concurrent.futures import Future
from typing import List, Union

import psycopg2
import networkx as nx
import pandas as pd

from hashlib import md5

from sqlalchemy.exc import ResourceClosedError

from flowmachine.core.cache import rescore
from flowmachine.utils.utils import rlock
from abc import ABCMeta, abstractmethod

from .errors import NameTooLongError, NotConnectedError

import flowmachine

logger = logging.getLogger("flowmachine").getChild(__name__)

# This is the maximum length that postgres will allow for its
# table name. This should only be changed if postgres is updated
# and removes this restriction.
MAX_POSTGRES_NAME_LENGTH = 63


class Query(metaclass=ABCMeta):
    """
    The core base class of the flowmachine module. This should handle
    all input and output methods for our sql queries, so that
    inherited classes should only need to deal with the logic of
    actually making the sql statement itself.

    Parameters
    ----------
    cache : bool, default True
        Will store the resultant dataframes in memory. One can turn this
        off with turn_off_caching, and back on with turn_on_caching.
    """

    _QueryPool = weakref.WeakValueDictionary()

    def __init__(self, cache=True):
        obj = Query._QueryPool.get(self.md5)
        if obj is None:
            try:
                self.connection
            except AttributeError:
                raise NotConnectedError()

            self._cache = cache
            Query._QueryPool[self.md5] = self
        else:
            self.__dict__ = obj.__dict__

    @property
    def md5(self):
        """
        Generate a uniquely identifying hash of this query,
        based on the parameters of it and the subqueries it is
        composed of.

        Returns
        -------
        str
            md5 hash string
        """
        try:
            return self._md5
        except:
            state = self.__getstate__()
            hashes = sorted([x.md5 for x in self.dependencies])
            for key, item in sorted(state.items()):
                try:
                    if isinstance(item, list) or isinstance(item, tuple):
                        item = sorted(item)
                    elif isinstance(item, dict):
                        item = sorted(item.items())

                    try:
                        hashes.append(str(item))
                    except TypeError:
                        pass
                except:
                    pass
            hashes.append(self.__class__.__name__)
            hashes.sort()
            self._md5 = md5(str(hashes).encode()).hexdigest()
            return self._md5

    @abstractmethod
    def _make_query(self):

        raise NotImplementedError

    def __repr__(self):

        # Default representation, derived classes might want to
        # add something more specific
        return "Query object of type : " + self.__class__.__name__

    def __iter__(self):
        con = self.connection.engine
        qur = self.get_query()
        with con.begin():
            self._query_object = con.execute(qur)

        return self

    def __next__(self):
        N = self._query_object.fetchone()
        if N:
            return N
        else:
            raise StopIteration

    def __len__(self):

        try:
            return self._len
        except AttributeError:
            sql = """
                  SELECT count(*) FROM ({everything}) AS foo
                  """.format(
                everything=self.get_query()
            )
            self._len = self.connection.fetch(sql)[0][0]
            return self._len

    def turn_on_caching(self):
        """
        Turn on the caching, so that a computed dataframe is retained.
        """
        self._cache = True

    def turn_off_caching(self):
        """
        Turn the caching off, the object forgets previously calculated
        dataframes, and won't store further calculations
        """
        try:
            del self._len
        except AttributeError:
            pass
        try:
            del self._df
        except AttributeError:
            pass

        self._cache = False

    @property
    def cache(self):
        """

        Returns
        -------
        bool
            True is caching is switched on.

        """
        return self._cache

    def get_query(self):
        """
        Returns a  string representing an SQL query. The string will point
        to the database cache of this query if it exists.

        Returns
        -------
        str
            SQL query string.

        """
        try:
            table_name = self.table_name
            schema, name = table_name.split(".")
            with rlock(self.redis, self.md5):
                if self.connection.has_table(schema=schema, name=name):
                    try:
                        new_score = rescore(
                            self.connection,
                            self,
                            int(os.getenv("CACHE_HALF_LIFE", 1000)),
                        )
                        self.connection.engine.execute(
                            f"UPDATE cache.cached SET last_accessed = NOW(), access_count = access_count + 1, cache_score = {new_score} WHERE query_id ='{self.md5}'"
                        )
                    except ValueError:
                        pass  # Cache record not written yet
                    return "SELECT * FROM {}".format(table_name)
        except NotImplementedError:
            pass
        return self._make_query()

    def get_dataframe_async(self):
        """
        Execute the query in a worker thread and return a future object
        which will contain the result as a pandas dataframe when complete.

        Returns
        -------
        Future
            Future object which can be used to get the resulting dataframe

        Notes
        -----
        This should be executed with care, as the results may consume
        large amounts of memory

        """

        def do_get():
            if self._cache:
                try:
                    return self._df.copy()
                except AttributeError:
                    qur = self.get_query()
                    with self.connection.engine.begin():
                        self._df = pd.read_sql_query(qur, con=self.connection.engine)

                    return self._df.copy()
            else:
                qur = self.get_query()
                with self.connection.engine.begin():
                    return pd.read_sql_query(qur, con=self.connection.engine)

        df_future = self.tp.submit(do_get)
        return df_future

    def get_dataframe(self):
        """
        Executes the query and return the result as a pandas dataframe.
        This should be executed with care, as the results may consume large
        amounts of memory.

        Returns
        -------
        pandas.DataFrame
            DataFrame containing results of the query.

        """
        return self.get_dataframe_async().result()

    @property
    def column_names(self) -> List[str]:
        """
        Returns the column names.

        Returns
        -------
        list of str
            List of the column names of this query.

        """
        try:
            return list(self._cols)
        except AttributeError:
            self._cols = self.head(0).columns.tolist()
            return list(self._cols)

    def head(self, n=5):
        """
        Return the first n results of the query

        Parameters
        ----------
        n : int
            Number of results to return

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing n results
        """
        try:
            return self._df.head(n)
        except AttributeError:
            Q = "SELECT * FROM ({}) h LIMIT {};".format(self.get_query(), n)
            con = self.connection.engine
            with con.begin():
                df = pd.read_sql_query(Q, con=con)
                return df

    def get_table(self):
        """
        If this Query is stored, return a Table object referencing
        the stored version. If it is not stored, raise an exception.

        Returns
        -------
        flowmachine.core.Table
            The stored version of this Query as a Table object
        """
        return flowmachine.core.Table(self.table_name)

    def union(self, other, all=True):
        """
        Returns a Query representing a the union of the two queries.
        This is simply the two tables concatenated. By passing the 
        argument all as true the duplicates are also removed.
        
        Parameters
        ----------
        other : Query
            An instance of a query object.
        all : Bool
            If true returns sql UNION ALL else returns UNION

        Returns
        -------
        Union
            Query representing the concatenation of the two queries

        Examples
        --------
        >>> dl1 = daily_location('2016-01-01', level='cell')
        >>> dl2 = daily_location('2016-01-02', level='cell')
        >>> dl1.union(dl2).get_query()
        'cell_msisdn_20160101 UNION ALL cell_msisdn_20160102'

        >>> dl1.union(dl2,all=False).get_query()
        'cell_msisdn_20160101 UNION cell_msisdn_20160102'
        """

        from .union import Union

        return Union(self, other, all)

    def join(
        self,
        other,
        on_left,
        on_right=None,
        how="inner",
        left_append="",
        right_append="",
    ):
        """

        Parameters
        ----------
        other : Query
            Query to join to
        on_left : str
            Field of this query to join on
        on_right : str
            Field of this query to join on
        how : {'left', 'outer', 'inner', 'full'}
            Method of joining to the other
        left_append : str
        right_append : str

        Returns
        -------
        Join
            Query object representing the two queries joined together
        """

        from .join import Join

        return Join(self, other, on_left, on_right, how, left_append, right_append)

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
        Subset object
        """

        from .subset import subset_factory

        subset_class = subset_factory(self.__class__)
        return subset_class(self, col, subset)

    def numeric_subset(self, col, low, high):
        """
        Subsets one of the columns to a specified range of numerical values.

        Parameters
        ----------
        col : str
            Name of the column to subset, e.g. subscriber, cell etc.
        low : float
                Lower bound of interval to subset on
        high : float
            Upper bound of interval to subset on

        Returns
        -------
        Numeric subset object
        """

        from .subset import subset_numbers_factory

        subset_class = subset_numbers_factory(self.__class__)
        return subset_class(self, col, low, high)

    def _make_sql(
        self, name: str, schema: Union[str, None] = None, force: bool = False
    ) -> List[str]:
        """
        Create the SQL necessary to store the result of the calculation back
        into the database.

        Parameters
        ----------
        name : str,
            name of the table
        schema : str, default None
            Name of an existing schema. If none will use the postgres default,
            see postgres docs for more info.
        force : bool, default False
            Will overwrite an existing table if the name already exists

        Returns
        -------
        list
            Ordered list of SQL strings to execute.
        """

        if schema is not None:
            full_name = "{}.{}".format(schema, name)
        else:
            full_name = name
        queries = []
        # Deal with the table already existing potentially
        if self.connection.has_table(name, schema=schema) and (not force):
            logger.info("Table already exists")
            return []

        Q = f"""EXPLAIN (ANALYZE TRUE, TIMING FALSE, FORMAT JSON) CREATE TABLE {full_name} AS 
            ({self._make_query() if force else self.get_query()})"""
        queries.append(Q)
        for ix in self.index_cols:
            queries.append(
                "CREATE INDEX ON {tbl} ({ixen})".format(
                    tbl=full_name, ixen=",".join(ix) if isinstance(ix, list) else ix
                )
            )
        return queries

    def to_sql(
        self, name: str, schema: Union[str, None] = None, force: bool = False
    ) -> Future:
        """
        Store the result of the calculation back into the database.

        Parameters
        ----------
        name : str
            name of the table
        schema : str, default None
            Name of an existing schema. If none will use the postgres default,
            see postgres docs for more info.
        force : bool, default False
            Will overwrite an existing table if the name already exists

        Returns
        -------
        Future
            Future object, containing this query and any result information.

        Notes
        -----

        This method will return a Future immediately.
        """
        if len(name) > MAX_POSTGRES_NAME_LENGTH:
            err_msg = (
                "The table name {} is too long ({}) chars. Postgres allows only table names"
                " of length {}"
            ).format(name, len(name), MAX_POSTGRES_NAME_LENGTH)
            raise NameTooLongError(err_msg)

        def do_query() -> Query:
            logger.debug("Getting storage lock.")
            with rlock(self.redis, self.md5):
                logger.debug("Obtained storage lock.")
                Qs = self._make_sql(name, schema=schema, force=force)
                logger.debug("Made SQL.")
                con = self.connection.engine
                if force:
                    self.invalidate_db_cache(name, schema=schema)
                plan_time = 0
                with con.begin():
                    rs = []
                    for Q in Qs:
                        try:
                            r = con.execute(Q)
                        except Exception as e:
                            logger.error(f"Error executing SQL: '{Q}'. Error was {e}")
                            raise e
                        try:
                            rs.append(r.fetchall())
                        except ResourceClosedError:
                            pass  # Nothing to do here
                        for r in rs:
                            try:
                                plan = r[0][0][0]
                                plan_time += plan["Execution Time"]
                            except (IndexError, KeyError):
                                pass  # Not an explain result
                    logger.debug("Executed queries.")
                    if schema == "cache":
                        self._db_store_cache_metadata(compute_time=plan_time)
            logger.debug("Released storage lock.")
            return self

        store_future = self.tp.submit(do_query)
        return store_future

    def explain(self, format="text", analyse=False):
        """
        Returns the postgres SQL explanation string.

        Parameters
        ----------
        format : {'text', 'json', 'yaml', 'xml'}, default 'text'
            Output format for the explanation.
        analyse : bool
            Set to true to run the query and return actual timings.

        Returns
        -------
        str or list of dict
            If `format='json'` is set, then this returns a list of dicts

        See Also
        --------
        https://www.postgresql.org/docs/current/static/sql-explain.html
        """
        format = format.upper()
        if format not in {"TEXT", "JSON", "YAML", "XML"}:
            raise ValueError("{} is not a valid explain output format.".format(format))
        opts = ["FORMAT {}".format(format)]
        if analyse:
            opts.append("ANALYZE")
        Q = "EXPLAIN ({})".format(", ".join(opts)) + self.get_query()

        exp = self.connection.fetch(Q)

        if format == "TEXT":
            return "\n".join(
                x[0] for x in exp
            )  # TEXT comes back as multiple rows, i.e. a list of tuple(str, )
        return exp[0][0]  # Everything else comes as one

    @property
    def table_name(self):
        """
        Returns a unique name for the query to be stored under, based on
        a hash of the parameters, class, and subqueries.

        Returns
        -------
        str
            String form of the table's fqn
        """
        return "cache.x{}".format(self.md5)

    @property
    def is_stored(self):
        """
        Returns
        -------
        bool
            True if the table is stored, and False otherwise.
        """

        try:
            schema, name = self.table_name.split(".")
            return self.connection.has_table(name, schema)
        except NotImplementedError:
            return False

    def store(self, force=False):
        """
        Store the results of this computation with the correct table
        name using a background thread.

        Parameters
        ----------
        force : bool, default False
            Will overwrite an existing table if the name already exists

        Returns
        -------
        Future
            Future object which can be queried to check the query
            is stored.
        """

        try:
            table_name = self.table_name
        except NotImplementedError:
            raise ValueError("Cannot store an object of this type with these params")

        schema, name = table_name.split(".")

        store_future = self.to_sql(name, schema=schema, force=force)
        return store_future

    def _db_store_cache_metadata(self, compute_time=None):
        """
        Helper function for store, updates flowmachine metadata table to
        log that this query is stored, but does not actually store
        the query.
        """

        from ..__init__ import __version__

        con = self.connection.engine

        self_storage = b""
        try:
            self_storage = pickle.dumps(self)
        except:
            logger.debug("Can't pickle, attempting to cache anyway.")
            pass

        try:
            in_cache = bool(
                self.connection.fetch(
                    "SELECT * FROM cache.cached WHERE query_id='{}'".format(self.md5)
                )
            )

            with con.begin():
                cache_record_insert = """
                INSERT INTO cache.cached 
                (query_id, version, query, created, access_count, last_accessed, compute_time, 
                cache_score, class, schema, tablename, obj) 
                VALUES (%s, %s, %s, NOW(), 0, NOW(), %s, 0, %s, %s, %s, %s)
                 ON CONFLICT (query_id) DO UPDATE SET last_accessed = NOW();"""
                con.execute(
                    cache_record_insert,
                    (
                        self.md5,
                        __version__,
                        self._make_query(),
                        compute_time,
                        self.__class__.__name__,
                        *self.table_name.split("."),
                        psycopg2.Binary(self_storage),
                    ),
                )
                logger.debug("{} added to cache.".format(self.table_name))
                if not in_cache:
                    for dep in self._get_deps(root=True):
                        con.execute(
                            "INSERT INTO cache.dependencies values (%s, %s) ON CONFLICT DO NOTHING",
                            (self.md5, dep.md5),
                        )
        except NotImplementedError:
            logger.debug("Table has no standard name.")

    @property
    def dependencies(self):
        """

        Returns
        -------
        set
            Query's this one is directly dependent on
        """
        return self._adjacent()

    def _adjacent(self):
        """
        Returns
        -------
        set
            Query's this one is directly dependent on
        """
        dependencies = set()
        for x in self.__dict__.values():
            if isinstance(x, Query):
                dependencies.add(x)
        lists = [
            x
            for x in self.__dict__.values()
            if isinstance(x, list) or isinstance(x, tuple)
        ]
        for l in lists:
            for x in l:
                if isinstance(x, Query):
                    dependencies.add(x)

        return dependencies

    def _get_deps(self, root=False, stored_dependencies=None):
        """

        Parameters
        ----------
        root : bool
            Set to true to exclude this query from the resulting set
        stored_dependencies : set
            Keeps track of dependencies already discovered

        Returns
        -------
        set
            The set of all stored queries this one depends on

        """
        if stored_dependencies is None:
            stored_dependencies = set()
        if not root and self.is_stored:
            stored_dependencies.add(self)
        else:
            for d in self.dependencies - stored_dependencies:
                d._get_deps(stored_dependencies=stored_dependencies)
        return stored_dependencies.difference([self])

    def invalidate_db_cache(self, name=None, schema=None, cascade=True, drop=True):
        """
        Helper function for store, drops this table, and (by default) any
        that depend on it, as well as removing them from
        the cache metadata table.

        Parameters
        ----------
        name : str
            Name of the table
        schema : str
            Schema of the table
        cascade : bool
            Set to false to remove only this table from cache
        drop : bool
            Set to false to remove the cache record without dropping the table
        """
        with rlock(self.redis, self.md5):
            con = self.connection.engine
            try:
                table_form = self.get_table()
                if table_form is not self:
                    table_form.invalidate_db_cache(
                        cascade=cascade, drop=drop
                    )  # Remove any Table pointing as this query
            except (ValueError, NotImplementedError) as e:
                pass  # This cache record isn't actually stored
            try:
                deps = self.connection.fetch(
                    """SELECT obj FROM cache.cached LEFT JOIN cache.dependencies
                    ON cache.cached.query_id=cache.dependencies.query_id
                    WHERE depends_on='{}'""".format(
                        self.md5
                    )
                )
                with con.begin():
                    con.execute(
                        "DELETE FROM cache.cached WHERE query_id=%s", (self.md5,)
                    )
                    logger.debug("Deleted cache record for {}.".format(self.table_name))
                    if drop:
                        con.execute("DROP TABLE IF EXISTS {}".format(self.table_name))
                        logger.debug(
                            "Dropped cache for for {}.".format(self.table_name)
                        )

                if cascade:
                    for rec in deps:
                        dep = pickle.loads(rec[0])
                        logger.debug(
                            "Cascading to {} from cache record for {}.".format(
                                dep.table_name, self.table_name
                            )
                        )
                        dep.invalidate_db_cache()
                else:
                    logger.debug("Not cascading to dependents.")
            except NotImplementedError:
                logger.info("Table has no standard name.")
            if schema is not None:
                full_name = "{}.{}".format(schema, name)
            else:
                full_name = name
            logger.debug("Dropping {}".format(full_name))
            with con.begin():
                con.execute("DROP TABLE IF EXISTS {}".format(full_name))

    @property
    def index_cols(self):
        """
        A list of columns to use as indexes when storing this query.


        Returns
        -------
        ixen : list
            By default, returns the location columns if they are present
            and self.level is defined, and the subscriber column.

        Examples
        --------
        >>> daily_location("2016-01-01").index_cols
        [['name'], '"subscriber"']
        """
        from flowmachine.utils.utils import (
            get_columns_for_level,
        )  # Local import to avoid circular import

        cols = self.column_names
        ixen = []
        try:
            # Not all objects define the attribute column_name so we'll fall
            # back to the default if it is not defined
            try:
                loc_cols = get_columns_for_level(self.level, self.column_name)
            except AttributeError:
                loc_cols = get_columns_for_level(self.level)
            if set(loc_cols).issubset(cols):
                ixen.append(loc_cols)
        except AttributeError:
            pass
        try:
            if self.subscriber_identifier in cols:
                ixen.append(self.subscriber_identifier)
            else:
                ixen.append('"subscriber"')
        except AttributeError:
            pass
        return ixen

    def __getstate__(self):
        """
        Removes properties which should not be pickled, or hashed. Override
        this method in your subclass if you need to add more.

        Returns
        -------
        dict
            A picklable and hash-safe copy of this objects internal dict.
        """
        state = self.__dict__.copy()
        bad_keys = [
            "_cache",
            "_df",
            "_query_lock",
            "_len",
            "_query_object",
            "_cols",
            "_md5",
            "_runtime",
        ]
        for k in bad_keys:
            try:
                del state[k]
            except:
                pass
        if (
            "table" in state
            and isinstance(state["table"], str)
            and state["table"].lower() == "all"
        ):
            state["table"] = "all"

        return state

    def __setstate__(self, state):
        """
        Helper for unpickling objects.

        Parameters
        ----------
        state : dict
            A dictionary to use in recreating the object
        """
        # Recreate lock.
        self.__dict__.update(state)
        self._cache = False

    @classmethod
    def get_stored(cls):
        """
        Get a list of stored query objects of this type

        Returns
        -------
        list
            All cached instances of this Query type, or any if called with
            Query.

        """
        try:
            Query.connection
        except:
            raise NotConnectedError()

        if cls is Query:
            qry = "SELECT obj FROM cache.cached"
        else:
            qry = "SELECT obj FROM cache.cached WHERE class='{}'".format(cls.__name__)
        logger.debug(qry)
        objs = Query.connection.fetch(qry)
        return (pickle.loads(obj[0]) for obj in objs)

    def random_sample(
        self,
        size=None,
        fraction=None,
        method="system_rows",
        estimate_count=True,
        seed=None,
    ):
        """
        Draws a random sample from this query.

        Parameters
        ----------
        size : int
            Number of rows to draw
        fraction : float
            Fraction of total rows to draw
        method : {'system', 'system_rows', 'bernoulli', 'random_ids'}, default 'system_rows'
            Specifies the method used to select the random sample.
            'system_rows': performs block-level sampling by randomly sampling
                each physical storage page of the underlying relation. This
                sampling method is guaranteed to provide a sample of the specified
                size
            'system': performs block-level sampling by randomly sampling each
                physical storage page for the underlying relation. This
                sampling method is not guaranteed to generate a sample of the
                specified size, but an approximation. This method may not
                produce a sample at all, so it might be worth running it again
                if it returns an empty dataframe.
            'bernoulli': samples directly on each row of the underlying
                relation. This sampling method is slower and is not guaranteed to
                generate a sample of the specified size, but an approximation
            'random_ids': Assumes that the table contains a column named 'id'
                with random numbers from 1 to the total number of rows in the
                table. This method samples the ids from this table.
        estimate_count : bool, default True
            Whether to estimate the number of rows in the table using
            information contained in the `pg_class` or whether to perform an
            actual count in the number of rows.
        seed : float, optional
            Optionally provide a seed for repeatable random samples, which should be between -/+1.
            Not available in combination with the system_rows method.

        Returns
        -------
        Random
            A special query object which contains a random sample from this one

        See Also
        --------
        flowmachine.utils.random.random_factory

        Notes
        -----

        Random samples may only be stored if a seed is supplied.

        """
        if seed is not None:
            if seed > 1 or seed < -1:
                raise ValueError("Seed must be between -1 and 1.")
            if method == "system_rows":
                raise ValueError("Seed is not supported with system_rows method.")

        from .random import random_factory

        random_class = random_factory(self.__class__)
        return random_class(
            query=self,
            size=size,
            fraction=fraction,
            method=method,
            estimate_count=estimate_count,
            seed=seed,
        )

    def dependency_graph(self, analyse=False):
        """
        Produce a graph of all the queries that go into producing this
        one, with their estimated run costs, and whether they are stored
        as node attributes.

        The resulting networkx object can then be visualised, or analysed.

        Parameters
        ----------
        query : Query
            Query object to produce a dependency graph fot
        analyse : bool
            Set to True to get actual runtimes for queries, note that this will actually
            run the query!

        Returns
        -------
        networkx.DiGraph

        Examples
        --------
        >>> import flowmachine
        >>> flowmachine.connect()
        >>> from flowmachine.features import daily_location
        >>> g = daily_location("2016-01-01").dependency_graph()
        >>> from networkx.drawing.nx_agraph import write_dot
        >>> write_dot(g, "daily_location_dependencies.dot")
        >>> g = daily_location("2016-01-01").dependency_graph(True)
        >>> from networkx.drawing.nx_agraph import write_dot
        >>> write_dot(g, "daily_location_dependencies_runtimes.dot")

        Notes
        -----
        The queries listed as dependencies are not _guaranteed_ to be
        used in the actual running of a query, only to be referenced by it.

        """
        g = nx.DiGraph()
        openlist = [(0, self)]
        deps = []

        while openlist:
            y, x = openlist.pop()
            deps.append((y, x))

            openlist += list(zip([x] * len(x.dependencies), x.dependencies))

        _, y = zip(*deps)
        for n in set(y):
            expl = n.explain(format="json", analyse=analyse)[0]
            attrs = {
                "name": n.__class__.__name__,
                "cost": expl["Plan"]["Total Cost"],
                "stored": n.is_stored,
                "shape": "rect",
            }
            if analyse:
                attrs["runtime"] = expl["Execution Time"]
            attrs["label"] = "{}. Cost: {}.".format(attrs["name"], attrs["cost"])
            if analyse:
                attrs["label"] += " Actual runtime: {}.".format(attrs["runtime"])
            if attrs["stored"]:
                attrs["fillcolor"] = "green"
                attrs["style"] = "filled"
            g.add_node("x{}".format(n.md5), **attrs)

        for x, y in deps:
            if x != 0:
                g.add_edge(*["x{}".format(z.md5) for z in (x, y)])

        return g
