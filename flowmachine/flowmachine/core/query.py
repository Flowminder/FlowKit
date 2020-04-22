# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
This is the base class that defines any query on our database.  It simply
defines methods that returns the query as a string and as a pandas dataframe.

"""
import rapidjson as json
import pickle
import weakref
from concurrent.futures import Future


import structlog
from typing import List, Union

import psycopg2
import pandas as pd

from hashlib import md5

from sqlalchemy.engine import Engine
from sqlalchemy.exc import ResourceClosedError

from flowmachine.core.cache import touch_cache
from flowmachine.core.context import (
    get_db,
    get_redis,
    submit_to_executor,
)
from flowmachine.core.errors.flowmachine_errors import QueryResetFailedException
from flowmachine.core.query_state import QueryStateMachine
from abc import ABCMeta, abstractmethod

from flowmachine.core.errors import (
    NameTooLongError,
    NotConnectedError,
    UnstorableQueryError,
)

import flowmachine
from flowmachine.utils import _sleep
from flowmachine.core.dependency_graph import (
    store_all_unstored_dependencies,
    store_queries_in_order,
    unstored_dependencies_graph,
)

from flowmachine.core.cache import write_query_to_cache

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)

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
        obj = Query._QueryPool.get(self.query_id)
        if obj is None:
            self._cache = cache
            Query._QueryPool[self.query_id] = self
        else:
            self.__dict__ = obj.__dict__

    @property
    def query_id(self):
        """
        Generate a uniquely identifying hash of this query,
        based on the parameters of it and the subqueries it is
        composed of.

        Returns
        -------
        str
            query_id hash string
        """
        try:
            return self._md5
        except:
            dependencies = self.dependencies
            state = self.__getstate__()
            hashes = sorted([x.query_id for x in dependencies])
            for key, item in sorted(state.items()):
                if isinstance(item, Query) and item in dependencies:
                    # this item is already included in `hashes`
                    continue
                elif isinstance(item, list) or isinstance(item, tuple):
                    item = sorted(
                        item,
                        key=lambda x: x.query_id if isinstance(x, Query) else str(x),
                    )
                elif isinstance(item, dict):
                    item = json.dumps(item, sort_keys=True, default=str)
                else:
                    # if it's not a list or a dict we leave the item as it is
                    pass

                hashes.append(str(item))
            hashes.append(self.__class__.__name__)
            hashes.sort()
            self._md5 = md5(str(hashes).encode()).hexdigest()
            return self._md5

    @abstractmethod
    def _make_query(self):

        raise NotImplementedError

    def __repr__(self):
        # Default representation, derived classes might want to add something more specific
        return format(self, "query_id")

    def __format__(self, fmt=""):
        """
        Return a formatted string representation of this query object.

        Parameters
        ----------
        fmt : str, optional
            This should be the empty string or a comma-separated list of
            query attributes that will be included in the formatted string.

        Examples
        --------

        >>> dl = daily_location(date="2016-01-01", method="last")
        >>> format(dl)
        <Query of type: LastLocation>
        >>> format(dl, "query_id,is_stored")
        <Query of type: LastLocation, query_id: 'd9537c9bc11580f868e3fc372dafdb94', is_stored: True>
        >>> print(f"{dl:is_stored,query_state}")
        <Query of type: LastLocation, is_stored: True, query_state: <QueryState.COMPLETED: 'completed'>
        """
        query_descr = f"Query of type: {self.__class__.__name__}"
        attrs_to_include = [] if fmt == "" else fmt.split(",")
        attr_descriptions = []
        for attr in attrs_to_include:
            try:
                attr_descriptions.append(f"{attr}: {getattr(self, attr)!r}")
            except AttributeError:
                raise ValueError(
                    f"Format string contains invalid query attribute: '{attr}'"
                )

        all_descriptions = [query_descr] + attr_descriptions
        return f"<{', '.join(all_descriptions)}>"

    def __iter__(self):
        con = get_db().engine
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
            self._len = get_db().fetch(sql)[0][0]
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

    @property
    def query_state(self) -> "QueryState":
        """
        Return the current query state.

        Returns
        -------
        flowmachine.core.query_state.QueryState
            The current query state
        """
        state_machine = QueryStateMachine(get_redis(), self.query_id, get_db().conn_id)
        return state_machine.current_query_state

    @property
    def query_state_str(self) -> str:
        """
        Return the current query state as a string

        Returns
        -------
        str
            The current query state. The possible values are the ones
            defined in `flowmachine.core.query_state.QueryState`.
        """
        return self.query_state.value

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
            table_name = self.fully_qualified_table_name
            schema, name = table_name.split(".")
            state_machine = QueryStateMachine(
                get_redis(), self.query_id, get_db().conn_id
            )
            state_machine.wait_until_complete()
            if state_machine.is_completed and get_db().has_table(
                schema=schema, name=name
            ):
                try:
                    touch_cache(get_db(), self.query_id)
                except ValueError:
                    pass  # Cache record not written yet, which can happen for Models
                    # which will call through to this method from their `_make_query` method while writing metadata.
                # In that scenario, the table _is_ written, but won't be visible from the connection touch_cache uses
                # as the cache metadata transaction isn't complete!
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
                    qur = f"SELECT {self.column_names_as_string_list} FROM ({self.get_query()}) _"
                    with get_db().engine.begin():
                        self._df = pd.read_sql_query(qur, con=get_db().engine)

                    return self._df.copy()
            else:
                qur = f"SELECT {self.column_names_as_string_list} FROM ({self.get_query()}) _"
                with get_db().engine.begin():
                    return pd.read_sql_query(qur, con=get_db().engine)

        df_future = submit_to_executor(do_get)
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
    @abstractmethod
    def column_names(self) -> List[str]:
        """
        Returns the column names.

        Returns
        -------
        list of str
            List of the column names of this query.
        """
        pass

    @property
    def column_names_as_string_list(self) -> str:
        """
        Get the column names as a comma separated list

        Returns
        -------
        str
            Comma separated list of column names
        """
        return ", ".join(self.column_names)

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
            Q = f"SELECT {self.column_names_as_string_list} FROM ({self.get_query()}) h LIMIT {n};"
            con = get_db().engine
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
        return flowmachine.core.Table(self.fully_qualified_table_name)

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
        >>> dl1 = daily_location('2016-01-01', spatial_unit=CellSpatialUnit())
        >>> dl2 = daily_location('2016-01-02', spatial_unit=CellSpatialUnit())
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

    def _make_sql(self, name: str, schema: Union[str, None] = None) -> List[str]:
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
        if get_db().has_table(name, schema=schema):
            logger.info("Table already exists")
            return []

        Q = f"""EXPLAIN (ANALYZE TRUE, TIMING FALSE, FORMAT JSON) CREATE TABLE {full_name} AS 
        (SELECT {self.column_names_as_string_list} FROM ({self._make_query()}) _)"""
        queries.append(Q)
        for ix in self.index_cols:
            queries.append(
                "CREATE INDEX ON {tbl} ({ixen})".format(
                    tbl=full_name, ixen=",".join(ix) if isinstance(ix, list) else ix
                )
            )
        return queries

    def to_sql(
        self,
        name: str,
        schema: Union[str, None] = None,
        store_dependencies: bool = False,
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
        store_dependencies : bool, default False
            If True, store the dependencies of this query.

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

        def write_query(query_ddl_ops: List[str], connection: Engine) -> float:
            plan_time = 0
            ddl_op_results = []
            for ddl_op in query_ddl_ops:
                try:
                    ddl_op_result = connection.execute(ddl_op)
                except Exception as e:
                    logger.error(f"Error executing SQL: '{ddl_op}'. Error was {e}")
                    raise e
                try:
                    ddl_op_results.append(ddl_op_result.fetchall())
                except ResourceClosedError:
                    pass  # Nothing to do here
                for ddl_op_result in ddl_op_results:
                    try:
                        plan = ddl_op_result[0][0][0]  # Should be a query plan
                        plan_time += plan["Execution Time"]
                    except (IndexError, KeyError):
                        pass  # Not an explain result
            logger.debug("Executed queries.")
            return plan_time

        if store_dependencies:
            store_queries_in_order(
                unstored_dependencies_graph(self)
            )  # Need to ensure we're behind our deps in the queue

        ddl_ops_func = self._make_sql

        current_state, changed_to_queue = QueryStateMachine(
            get_redis(), self.query_id, get_db().conn_id
        ).enqueue()
        logger.debug(
            f"Attempted to enqueue query '{self.query_id}', query state is now {current_state} and change happened {'here and now' if changed_to_queue else 'elsewhere'}."
        )
        # name, redis, query, connection, ddl_ops_func, write_func, schema = None, sleep_duration = 1
        store_future = submit_to_executor(
            write_query_to_cache,
            name=name,
            schema=schema,
            query=self,
            connection=get_db(),
            redis=get_redis(),
            ddl_ops_func=ddl_ops_func,
            write_func=write_query,
        )
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

        exp = get_db().fetch(Q)

        if format == "TEXT":
            return "\n".join(
                x[0] for x in exp
            )  # TEXT comes back as multiple rows, i.e. a list of tuple(str, )
        return exp[0][0]  # Everything else comes as one

    @property
    def fully_qualified_table_name(self):
        """
        Returns a unique fully qualified name for the query to be stored as under the cache schema, based on
        a hash of the parameters, class, and subqueries.

        Returns
        -------
        str
            String form of the table's fqn
        """
        return f"cache.{self.table_name}"

    @property
    def table_name(self):
        """
        Returns a uniquename for the query to be stored as, based on
        a hash of the parameters, class, and subqueries.

        Returns
        -------
        str
            String form of the table's fqn
        """
        return f"x{self.query_id}"

    @property
    def is_stored(self):
        """
        Returns
        -------
        bool
            True if the table is stored, and False otherwise.
        """

        try:
            schema, name = self.fully_qualified_table_name.split(".")
            return get_db().has_table(name, schema)
        except NotImplementedError:
            return False

    def store(self, store_dependencies: bool = False) -> Future:
        """
        Store the results of this computation with the correct table
        name using a background thread.

        Parameters
        ----------
        store_dependencies : bool, default False
            If True, store the dependencies of this query.

        Returns
        -------
        Future
            Future object which can be queried to check the query
            is stored.
        """

        try:
            table_name = self.fully_qualified_table_name
        except NotImplementedError:
            raise UnstorableQueryError(self)

        schema, name = table_name.split(".")

        store_future = self.to_sql(
            name, schema=schema, store_dependencies=store_dependencies
        )
        return store_future

    @property
    def dependencies(self):
        """

        Returns
        -------
        set
            The set of queries which this one is directly dependent on.
        """
        dependencies = set()
        for x in self.__dict__.values():
            if isinstance(x, Query):
                dependencies.add(x)
        lists = []
        for x in self.__dict__.values():
            if isinstance(x, list) or isinstance(x, tuple):
                lists.append(x)
            else:
                parent_classes = [cls.__name__ for cls in x.__class__.__mro__]
                if "SubscriberSubsetterBase" in parent_classes:
                    # special case for subscriber subsetters, because they may contain
                    # attributes which are Query object but do not derive from Query
                    # themselves
                    lists.append(x.__dict__.values())
        for l in lists:
            for x in l:
                if isinstance(x, Query):
                    dependencies.add(x)

        return dependencies

    def _get_stored_dependencies(
        self, exclude_self=False, discovered_dependencies=None
    ):
        """

        Parameters
        ----------
        exclude_self : bool
            Set to true to exclude this query from the resulting set.
        discovered_dependencies : set
            Keeps track of dependencies already discovered (during recursive calls).

        Returns
        -------
        set
            The set of all stored queries this one depends on

        """
        if discovered_dependencies is None:
            discovered_dependencies = set()
        if not exclude_self and self.is_stored:
            discovered_dependencies.add(self)
        else:
            for d in self.dependencies - discovered_dependencies:
                d._get_stored_dependencies(
                    discovered_dependencies=discovered_dependencies
                )
        return discovered_dependencies.difference([self])

    def invalidate_db_cache(self, name=None, schema=None, cascade=True, drop=True):
        """
        Drops this table, and (by default) any that depend on it, as well as removing them from
        the cache metadata table. If the table is currently being dropped from elsewhere, this
        method will block and return when the table has been removed.

        Raises
        ------
        QueryResetFailedException
            If the query wasn't succesfully removed

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
        q_state_machine = QueryStateMachine(
            get_redis(), self.query_id, get_db().conn_id
        )
        current_state, this_thread_is_owner = q_state_machine.reset()
        if this_thread_is_owner:
            con = get_db().engine
            try:
                table_reference_to_this_query = self.get_table()
                if table_reference_to_this_query is not self:
                    table_reference_to_this_query.invalidate_db_cache(
                        cascade=cascade, drop=drop
                    )  # Remove any Table pointing as this query
            except (ValueError, NotImplementedError) as e:
                pass  # This cache record isn't actually stored
            try:
                deps = get_db().fetch(
                    """SELECT obj FROM cache.cached LEFT JOIN cache.dependencies
                    ON cache.cached.query_id=cache.dependencies.query_id
                    WHERE depends_on='{}'""".format(
                        self.query_id
                    )
                )
                with con.begin():
                    con.execute(
                        "DELETE FROM cache.cached WHERE query_id=%s", (self.query_id,)
                    )
                    logger.debug(
                        "Deleted cache record for {}.".format(
                            self.fully_qualified_table_name
                        )
                    )
                    if drop:
                        con.execute(
                            "DROP TABLE IF EXISTS {}".format(
                                self.fully_qualified_table_name
                            )
                        )
                        logger.debug(
                            "Dropped cache for for {}.".format(
                                self.fully_qualified_table_name
                            )
                        )

                if cascade:
                    for rec in deps:
                        dep = pickle.loads(rec[0])
                        logger.debug(
                            "Cascading to {} from cache record for {}.".format(
                                dep.fully_qualified_table_name,
                                self.fully_qualified_table_name,
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
            q_state_machine.finish_resetting()
        elif q_state_machine.is_resetting:
            logger.debug(
                f"Query '{self.query_id}' is being reset from elsewhere, waiting for reset to finish."
            )
            while q_state_machine.is_resetting:
                _sleep(1)
        if not q_state_machine.is_known:
            raise QueryResetFailedException(self.query_id)

    @property
    def index_cols(self):
        """
        A list of columns to use as indexes when storing this query.


        Returns
        -------
        ixen : list
            By default, returns the location columns if they are present
            and self.spatial_unit is defined, and the subscriber column.

        Examples
        --------
        >>> daily_location("2016-01-01").index_cols
        [['name'], '"subscriber"']
        """
        cols = self.column_names
        ixen = []
        try:
            loc_cols = self.spatial_unit.location_id_columns
            if set(loc_cols).issubset(cols):
                ixen.append(loc_cols)
        except AttributeError:
            pass
        try:
            if self.subscriber_identifier in cols:
                ixen.append(self.subscriber_identifier)
            elif "subscriber" in cols:
                ixen.append('"subscriber"')
            else:
                pass
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
            get_db()
        except:
            raise NotConnectedError()

        if cls is Query:
            qry = "SELECT obj FROM cache.cached"
        else:
            qry = "SELECT obj FROM cache.cached WHERE class='{}'".format(cls.__name__)
        logger.debug(qry)
        objs = get_db().fetch(qry)
        return (pickle.loads(obj[0]) for obj in objs)

    def random_sample(self, sampling_method="random_ids", **params):
        """
        Draws a random sample from this query.

        Parameters
        ----------
        sampling_method : {'system', 'system_rows', 'bernoulli', 'random_ids'}, default 'random_ids'
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
            'random_ids': samples rows by randomly sampling the row number.
        size : int, optional
            The number of rows to draw.
            Exactly one of the 'size' or 'fraction' arguments must be provided.
        fraction : float, optional
            Fraction of rows to draw.
            Exactly one of the 'size' or 'fraction' arguments must be provided.
        estimate_count : bool, default False
            Whether to estimate the number of rows in the table using
            information contained in the `pg_class` or whether to perform an
            actual count in the number of rows.
        seed : float, optional
            Optionally provide a seed for repeatable random samples.
            If using random_ids method, seed must be between -/+1.
            Not available in combination with the system_rows method.

        Returns
        -------
        Random
            A special query object which contains a random sample from this one

        See Also
        --------
        flowmachine.core.random.random_factory

        Notes
        -----
        Random samples may only be stored if a seed is supplied.
        """
        from .random import random_factory

        random_class = random_factory(self.__class__, sampling_method=sampling_method)
        return random_class(query=self, **params)

    def __hash__(self):
        return hash(self.query_id)
