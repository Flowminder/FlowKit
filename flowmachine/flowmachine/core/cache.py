# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

"""
Functions which deal with inspecting and managing the query cache.
"""
import asyncio
import pickle
from contextvars import copy_context
from concurrent.futures import Executor, TimeoutError
from functools import partial

from typing import TYPE_CHECKING, Tuple, List, Callable, Optional

from redis import StrictRedis
import psycopg2
from sqlalchemy.engine import Engine

from flowmachine.core.errors.flowmachine_errors import (
    QueryCancelledException,
    QueryErroredException,
    StoreFailedException,
)
from flowmachine.core.query_state import QueryStateMachine, QueryEvent
from flowmachine import __version__

if TYPE_CHECKING:
    from .query import Query
    from .connection import Connection

import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


def get_obj_or_stub(connection: "Connection", query_id: str):
    """
    Get a query object by ID if the object can be recreated. If it cannot
    then get a stub which keeps the query's dependency relationships as recorded
    in the database.

    Parameters
    ----------
    connection : Connection
        DB connection to get the object from
    query_id : str
        Unique identifier of the query

    Returns
    -------
    Query
        The query object if it can be recreated, or a 'stub' class which records
        the dependencies it had if not.

    """
    from flowmachine.core.query import Query

    class QStub(Query):
        def __init__(self, deps, qid):
            self.deps = deps
            self._md5 = qid
            super().__init__()

        def _make_query(self):
            pass

        @property
        def column_names(self):
            pass

    try:
        return get_query_object_by_id(connection, query_id)
    except (
        EOFError,
        ModuleNotFoundError,
        AttributeError,
        pickle.UnpicklingError,
        IndexError,
    ) as exc:
        logger.debug("Can't unpickle, creating stub.", query_id=query_id, exception=exc)
        qry = f"SELECT depends_on FROM cache.dependencies WHERE query_id='{query_id}'"
        return QStub(
            deps=[get_obj_or_stub(connection, res[0]) for res in connection.fetch(qry)],
            qid=query_id,
        )


def write_query_to_cache(
    *,
    name: str,
    redis: StrictRedis,
    query: "Query",
    connection: "Connection",
    ddl_ops_func: Callable[[str, str], List[str]],
    write_func: Callable[[List[str], Engine], float],
    schema: Optional[str] = "cache",
    sleep_duration: Optional[int] = 1,
) -> "Query":
    """
    Write a Query object into a postgres table and update the cache metadata about it.
    Attempts to update the query's state in redis to `executing`, and if successful, tries to run
    it. If unable to update the state to `executing`, will block until the query is in a `completed`,
    `errored`, or `cancelled` state.

    Parameters
    ----------
    name : str
        Name of the table to write to
    redis : StrictRedis
        Redis connection to use to update state
    query : Query
        Query object to write
    connection : Connection
        Flowmachine connection to use for writing
    ddl_ops_func : Callable[[str, str], List[str]]
        Function that will be called to generate a list of SQL statements to run. Should accept name and
        schema as arguments and return a list of SQL strings.
    write_func : Callable[[List[str], Engine], float]
        Function which will be called with the result of ddl_ops_func to perform the actual write.
        Should take a list of SQL strings and an SQLAlchemy Engine as arguments and return the runtime
        of the query.
    schema : str, default "cache"
        Name of the schema to write to
    sleep_duration : int, default 1
        Number of seconds to wait between polls when monitoring a query being written from elsewhere

    Returns
    -------
    Query
        The query object which was written once the write has completed successfully.


    Raises
    ------
    QueryCancelledException
        If execution of the query was interrupted by a user
    QueryErroredException
        If running the ddl ops failed
    StoreFailedException
        If something unexpected went wrong while storing the query

    Notes
    -----
    This is a _blocking function_, and will not return until the query is no longer in an executing state.

    """
    logger.debug(f"Trying to switch '{query.query_id}' to executing state.")
    q_state_machine = QueryStateMachine(redis, query.query_id, connection.conn_id)
    current_state, this_thread_is_owner = q_state_machine.execute()
    if this_thread_is_owner:
        logger.debug(f"In charge of executing '{query.query_id}'.")
        try:
            query_ddl_ops = ddl_ops_func(name, schema)
        except Exception as exc:
            q_state_machine.raise_error()
            logger.error(f"Error generating SQL. Error was {exc}")
            raise exc
        logger.debug("Made SQL.")
        con = connection.engine
        with con.begin():
            try:
                plan_time = write_func(query_ddl_ops, con)
                logger.debug("Executed queries.")
            except Exception as exc:
                q_state_machine.raise_error()
                logger.error(f"Error executing SQL. Error was {exc}")
                raise exc
            if schema == "cache":
                try:
                    write_cache_metadata(connection, query, compute_time=plan_time)
                except Exception as exc:
                    q_state_machine.raise_error()
                    logger.error(f"Error writing cache metadata. Error was {exc}")
                    raise exc
        q_state_machine.finish()

    q_state_machine.wait_until_complete(sleep_duration=sleep_duration)
    if q_state_machine.is_completed:
        return query
    elif q_state_machine.is_cancelled:
        logger.error(f"Query '{query.query_id}' was cancelled.")
        raise QueryCancelledException(query.query_id)
    elif q_state_machine.is_errored:
        logger.error(f"Query '{query.query_id}' finished with an error.")
        raise QueryErroredException(query.query_id)
    else:
        logger.error(
            f"Query '{query.query_id}' not stored. State is {q_state_machine.current_query_state}"
        )
        raise StoreFailedException(query.query_id)


def write_cache_metadata(
    connection: "Connection", query: "Query", compute_time: Optional[float] = None
):
    """
    Helper function for store, updates flowmachine metadata table to
    log that this query is stored, but does not actually store
    the query.

    Parameters
    ----------
    connection : Connection
        Flowmachine connection object to use
    query : Query
        Query object to write metadata about
    compute_time : float, default None
        Optionally provide the compute time for the query

    """

    con = connection.engine

    self_storage = b""

    try:
        in_cache = bool(
            connection.fetch(
                f"SELECT * FROM cache.cached WHERE query_id='{query.query_id}'"
            )
        )
        if not in_cache:
            try:
                self_storage = pickle.dumps(query)
            except Exception as e:
                logger.debug(f"Can't pickle ({e}), attempting to cache anyway.")
                pass

        with con.begin():
            cache_record_insert = """
            INSERT INTO cache.cached 
            (query_id, version, query, created, access_count, last_accessed, compute_time, 
            cache_score_multiplier, class, schema, tablename, obj) 
            VALUES (%s, %s, %s, NOW(), 0, NOW(), %s, 0, %s, %s, %s, %s)
             ON CONFLICT (query_id) DO UPDATE SET last_accessed = NOW();"""
            con.execute(
                cache_record_insert,
                (
                    query.query_id,
                    __version__,
                    query._make_query(),
                    compute_time,
                    query.__class__.__name__,
                    *query.fully_qualified_table_name.split("."),
                    psycopg2.Binary(self_storage),
                ),
            )
            con.execute("SELECT touch_cache(%s);", query.query_id)

            if not in_cache:
                for dep in query._get_stored_dependencies(exclude_self=True):
                    con.execute(
                        "INSERT INTO cache.dependencies values (%s, %s) ON CONFLICT DO NOTHING",
                        (query.query_id, dep.query_id),
                    )
                logger.debug(f"{query.fully_qualified_table_name} added to cache.")
            else:
                logger.debug(f"Touched cache for {query.fully_qualified_table_name}.")
    except NotImplementedError:
        logger.debug("Table has no standard name.")


def touch_cache(connection: "Connection", query_id: str) -> float:
    """
    'Touch' a cache record and update the cache score for it.

    Parameters
    ----------
    connection : Connection
    query_id : str
        Unique id of the query to touch

    Returns
    -------
    float
        The new cache score
    """
    try:
        return float(connection.fetch(f"SELECT touch_cache('{query_id}')")[0][0])
    except (IndexError, psycopg2.InternalError):
        raise ValueError(f"Query id '{query_id}' is not in cache on this connection.")


def reset_cache(
    connection: "Connection", redis: StrictRedis, protect_table_objects: bool = True
) -> None:
    """
    Reset the query cache. Deletes any tables under cache schema, resets the cache count,
    clears the cached and dependencies tables.

    Parameters
    ----------
    connection : Connection
    redis : StrictRedis
    protect_table_objects : bool, default True
        Set to False to also remove cache metadata for Table objects which point to tables outside the cache schema

    Notes
    -----
    You _must_ ensure that no queries are currently running when calling this function.
    Any queries currently running will no longer be tracked by redis, and UNDEFINED BEHAVIOUR
    will occur.

    In addition, you should restart any interpreter running flowmachine after this command is run
    to ensure that the local objects have not drifted out of sync with the database.
    """
    # For deletion purposes, we ignore Table objects. These either point to something
    # outside the cache schema and hence shouldn't be removed by calling this, or
    # they point to a cache table and hence are a duplicate of a Query entry which
    # will also point to that table.

    with connection.engine.begin() as trans:
        qry = f"SELECT tablename FROM cache.cached WHERE schema='cache'"
        tables = trans.execute(qry).fetchall()

    with connection.engine.begin() as trans:
        trans.execute("SELECT setval('cache.cache_touches', 1)")
    for table in tables:
        with connection.engine.begin() as trans:
            trans.execute(f"DROP TABLE IF EXISTS cache.{table[0]} CASCADE")
    if protect_table_objects:
        with connection.engine.begin() as trans:
            trans.execute(f"DELETE FROM cache.cached WHERE schema='cache'")
    else:
        with connection.engine.begin() as trans:
            trans.execute("TRUNCATE cache.cached CASCADE")
    resync_redis_with_cache(connection=connection, redis=redis)


def resync_redis_with_cache(connection: "Connection", redis: StrictRedis) -> None:
    """
    Reset redis to be in sync with the current contents of the cache.

    Parameters
    ----------
    connection : Connection
    redis : StrictRedis

    Returns
    -------
    None

    Notes
    -----
    You _must_ ensure that no queries are currently running when calling this function.
    Any queries currently running will no longer be tracked by redis, and UNDEFINED BEHAVIOUR
    will occur.
    """
    logger.debug("Redis resync")
    qry = f"SELECT query_id FROM cache.cached"
    queries_in_cache = connection.fetch(qry)
    logger.debug("Redis resync", queries_in_cache=queries_in_cache)
    redis.flushdb()
    logger.debug("Flushing redis.")
    for event in (QueryEvent.QUEUE, QueryEvent.EXECUTE, QueryEvent.FINISH):
        for qid in queries_in_cache:
            new_state, changed = QueryStateMachine(
                redis, qid[0], connection.conn_id
            ).trigger_event(event)
            logger.debug(
                "Redis resync",
                fast_forwarded=qid[0],
                new_state=new_state,
                fast_forward_succeeded=changed,
            )
            if not changed:
                raise RuntimeError(
                    f"Failed to trigger {event} on '{qid[0]}', ensure nobody else is accessing redis!"
                )


def invalidate_cache_by_id(
    connection: "Connection", query_id: str, cascade=False
) -> "Query":
    """
    Remove a query object from cache by id.

    Parameters
    ----------
    connection : Connection
    query_id : str
        Unique id of the query
    cascade : bool, default False
        Set to true to remove any queries that depend on the one being removed

    Returns
    -------
    Query
        The original query object.

    """
    query_obj = get_query_object_by_id(connection, query_id)
    query_obj.invalidate_db_cache(cascade=cascade)
    return query_obj


def get_query_object_by_id(connection: "Connection", query_id: str) -> "Query":
    """
    Get a query object from cache by id.

    Parameters
    ----------
    connection : Connection
    query_id : str
        Unique id of the query

    Returns
    -------
    Query
        The original query object.

    """
    qry = f"SELECT obj FROM cache.cached WHERE query_id='{query_id}'"
    try:
        obj = connection.fetch(qry)[0][0]
        return pickle.loads(obj)
    except IndexError:
        raise ValueError(f"Query id '{query_id}' is not in cache on this connection.")


def get_cached_query_objects_ordered_by_score(
    connection: "Connection",
    protected_period: Optional[int] = None,
) -> Tuple["Query", int]:
    """
    Get all cached query objects in ascending cache score order.

    Parameters
    ----------
    connection : Connection
    protected_period : int, default None
        Optionally specify a number of seconds within which cache entries are excluded. If None,
        the value stored in cache.cache_config will be used.Set to a negative number to ignore cache protection
        completely.

    Yields
    -------
    tuples
        Returns a list of cached Query objects with their on disk sizes

    """
    protected_period_clause = (
        (f" AND NOW()-created > INTERVAL '{protected_period} seconds'")
        if protected_period is not None
        else " AND NOW()-created > (cache_protected_period()*INTERVAL '1 seconds')"
    )
    qry = f"""SELECT query_id, table_size(tablename, schema) as table_size
        FROM cache.cached
        WHERE cached.class!='Table' AND cached.class!='GeoTable'
        {protected_period_clause}
        ORDER BY cache_score(cache_score_multiplier, compute_time, table_size(tablename, schema)) ASC
        """
    cache_queries = connection.fetch(qry)
    for query_id, table_size in cache_queries:
        yield get_obj_or_stub(connection, query_id), table_size


def shrink_one(
    connection: "Connection",
    dry_run: bool = False,
    protected_period: Optional[int] = None,
) -> "Query":
    """
    Remove the lowest scoring cached query from cache and return it and size of it
    in bytes.

    Parameters
    ----------
    connection : "Connection"
    dry_run : bool, default False
        Set to true to just report the object that would be removed and not remove it
     protected_period : int, default None
        Optionally specify a number of seconds within which cache entries are excluded. If None,
        the value stored in cache.cache_config will be used.Set to a negative number to ignore cache protection
        completely.

    Yields
    -------
    tuple of "Query", int
        The "Query" object that was removed from cache and the size of it
    """
    for obj_to_remove, obj_size in get_cached_query_objects_ordered_by_score(
        connection, protected_period=protected_period
    ):
        logger.info(
            "Remove cache record.",
            dry_run=dry_run,
            query_id=obj_to_remove.query_id,
            table=obj_to_remove.fully_qualified_table_name,
            table_size=obj_size,
        )

        if not dry_run:
            obj_to_remove.invalidate_db_cache(cascade=False, drop=True)
            logger.info(
                "Removed cache record.",
                dry_run=dry_run,
                query_id=obj_to_remove.query_id,
                table=obj_to_remove.fully_qualified_table_name,
                table_size=obj_size,
            )
        yield obj_to_remove, obj_size


def shrink_below_size(
    connection: "Connection",
    size_threshold: int = None,
    dry_run: bool = False,
    protected_period: Optional[int] = None,
) -> "Query":
    """
    Remove queries from the cache until it is below a specified size threshold.

    Parameters
    ----------
    connection : "Connection"
    size_threshold : int, default None
        Optionally override the maximum cache size set in flowdb.
    dry_run : bool, default False
        Set to true to just report the objects that would be removed and not remove them
    protected_period : int, default None
        Optionally specify a number of seconds within which cache entries are excluded. If None,
        the value stored in cache.cache_config will be used.Set to a negative number to ignore cache protection
        completely.

    Returns
    -------
    list of "Query"
        List of the queries that were removed
    """
    initial_cache_size = get_size_of_cache(connection)
    if size_threshold is None:
        size_threshold = get_max_size_of_cache(connection)
    removed = []
    logger.info(
        f"Shrinking cache from {initial_cache_size} to below {size_threshold}{' (dry run)' if dry_run else ''}.",
        initial_cache_size=initial_cache_size,
        size_threshold=size_threshold,
        dry_run=dry_run,
    )

    if dry_run:
        cached_queries = get_cached_query_objects_ordered_by_score(
            connection, protected_period=protected_period
        )

        def dry_run_shrink(connection, protected_period):
            for obj, obj_size in cached_queries:
                logger.info(
                    f"Would remove cache record for {obj.query_id} of type {obj.__class__}"
                )
                logger.info(
                    f"Table {obj.fully_qualified_table_name} ({obj_size} bytes) would be removed."
                )
                yield obj, obj_size

        shrink = dry_run_shrink(connection, protected_period=protected_period)
    else:
        shrink = shrink_one(connection, protected_period=protected_period)

    current_cache_size = initial_cache_size
    try:
        while current_cache_size > size_threshold:
            obj_removed, cache_reduction = next(shrink)
            removed.append(obj_removed)
            current_cache_size -= cache_reduction
        logger.info(
            f"New cache size {'would' if dry_run else 'will'} be {current_cache_size}.",
            removed=[q.query_id for q in removed],
            dry_run=dry_run,
            initial_cache_size=initial_cache_size,
            current_cache_size=current_cache_size,
            size_threshold=size_threshold,
        )
    except StopIteration:
        logger.info(
            "Unable to shrink cache. No cache items eligible to be removed.",
            dry_run=dry_run,
            initial_cache_size=initial_cache_size,
            current_cache_size=current_cache_size,
            size_threshold=size_threshold,
        )
    return removed


def get_size_of_table(
    connection: "Connection", table_name: str, table_schema: str
) -> int:
    """
    Get the size on disk in bytes of a table in the database.

    Parameters
    ----------
    connection : "Connection"
    table_name : str
        Name of table to get size of
    table_schema : str
        Schema of the table

    Returns
    -------
    int
        Number of bytes on disk this table uses in total

    """
    sql = f"SELECT table_size('{table_name}', '{table_schema}')"
    try:
        return int(connection.fetch(sql)[0][0])
    except IndexError:
        raise ValueError(
            f"Table '{table_schema}.{table_name}' does not exist on this connection."
        )


def get_size_of_cache(connection: "Connection") -> int:
    """
    Get the total size in bytes of all cache tables.

    Parameters
    ----------
    connection : "Connection"

    Returns
    -------
    int
        Number of bytes in total used by cache tables

    """
    sql = """SELECT sum(table_size(tablename, schema)) as total_bytes 
        FROM cache.cached  
        WHERE cached.class!='Table' AND cached.class!='GeoTable'"""
    cache_bytes = connection.fetch(sql)[0][0]
    return 0 if cache_bytes is None else int(cache_bytes)


def get_max_size_of_cache(connection: "Connection") -> int:
    """
    Get the upper limit set in FlowDB for the cache size, in bytes.

    Parameters
    ----------
    connection : "Connection"

    Returns
    -------
    int
        Number of bytes in total available to cache tables

    """
    sql = "SELECT cache_max_size()"
    return int(connection.fetch(sql)[0][0])


def get_cache_half_life(connection: "Connection") -> float:
    """
    Get the current setting for cache half-life.

    Parameters
    ----------
    connection : "Connection"

    Returns
    -------
    float
        Cache half-life setting

    """
    sql = "SELECT cache_half_life()"
    return float(connection.fetch(sql)[0][0])


def set_cache_half_life(connection: "Connection", cache_half_life: float) -> None:
    """
    Set the cache's half-life.

    Parameters
    ----------
    connection : "Connection"
    cache_half_life : float
        Setting for half-life

    Notes
    -----

    Changing this setting without flushing the cache first may have unpredictable consequences
    and should be avoided.
    """

    sql = f"UPDATE cache.cache_config SET value='{float(cache_half_life)}' WHERE key='half_life'"
    with connection.engine.begin() as trans:
        trans.execute(sql)


def get_cache_protected_period(connection: "Connection") -> float:
    """
    Get the current setting for cache protected period.

    Parameters
    ----------
    connection : "Connection"

    Returns
    -------
    int
        Cache protected period

    """
    sql = "SELECT cache_protected_period()"
    return float(connection.fetch(sql)[0][0])


def set_cache_protected_period(connection: "Connection", protected_period: int) -> None:
    """
    Set the cache's half-life.

    Parameters
    ----------
    connection : "Connection"
    protected_period : int, default None
        Optionally specify a number of seconds within which cache entries are excluded.
    """

    sql = f"UPDATE cache.cache_config SET value='{int(protected_period)}' WHERE key='protected_period'"
    with connection.engine.begin() as trans:
        trans.execute(sql)


def set_max_size_of_cache(connection: "Connection", cache_size_limit: int) -> None:
    """
    Set the upper limit set in FlowDB for the cache size, in bytes.

    Parameters
    ----------
    connection : "Connection"
    cache_size_limit : int
        Size in bytes to set as the cache limit

    """
    sql = f"UPDATE cache.cache_config SET value='{int(cache_size_limit)}' WHERE key='cache_size'"
    with connection.engine.begin() as trans:
        trans.execute(sql)


def get_compute_time(connection: "Connection", query_id: str) -> float:
    """
    Get the time in seconds that a cached query took to compute.

    Parameters
    ----------
    connection : "Connection"
    query_id : str
        Unique id of the query

    Returns
    -------
    float
        Number of seconds the query took to compute

    """
    try:
        return float(
            connection.fetch(
                f"SELECT compute_time FROM cache.cached WHERE query_id='{query_id}'"
            )[0][0]
            / 1000
        )
    except IndexError:
        raise ValueError(f"Query id '{query_id}' is not in cache on this connection.")


def get_score(connection: "Connection", query_id: str) -> float:
    """
    Get the current cache score for a cached query.

    Parameters
    ----------
    connection: "Connection"
    query_id : str
        Unique id of the cached query

    Returns
    -------
    float
        Current cache score of this query

    """
    try:
        return float(
            connection.fetch(
                f"""SELECT cache_score(cache_score_multiplier, compute_time, table_size(tablename, schema))
                 FROM cache.cached WHERE query_id='{query_id}'"""
            )[0][0]
        )
    except IndexError:
        raise ValueError(f"Query id '{query_id}' is not in cache on this connection.")


def cache_table_exists(connection: "Connection", query_id: str) -> bool:
    """
    Return True if a cache table for the query with
    id `query_id` exist, otherwise return False.

    Parameters
    ----------
    connection: "Connection"
    query_id : str
        Unique id of the cached query

    Returns
    -------
    bool
    """
    try:
        _ = get_query_object_by_id(connection, query_id)
        return True
    except ValueError:
        return False


async def watch_and_shrink_cache(
    *,
    flowdb_connection: "Connection",
    pool: Executor,
    sleep_time: int = 86400,
    timeout: Optional[int] = 600,
    loop: bool = True,
    size_threshold: int = None,
    dry_run: bool = False,
    protected_period: Optional[int] = None,
) -> None:
    """
    Background task to periodically trigger a shrink of the cache.

    Parameters
    ----------
    flowdb_connection : Connection
        Flowdb connection to check dates on
    pool : Executor
        Executor to run the date check with
    sleep_time : int, default 86400
        Number of seconds to sleep for between checks
    timeout : int or None, default 600
        Seconds to wait for a cache shrink to complete before cancelling it
    loop : bool, default True
        Set to false to return after the first check
    size_threshold : int, default None
        Optionally override the maximum cache size set in flowdb.
    dry_run : bool, default False
        Set to true to just report the objects that would be removed and not remove them
    protected_period : int, default None
        Optionally specify a number of seconds within which cache entries are excluded. If None,
        the value stored in cache.cache_config will be used.Set to a negative number to ignore cache protection
        completely.

    Returns
    -------
    None

    """
    shrink_func = partial(
        shrink_below_size,
        connection=flowdb_connection,
        size_threshold=size_threshold,
        dry_run=dry_run,
        protected_period=protected_period,
    )
    while True:
        logger.debug("Checking if cache should be shrunk.")

        try:  # Set the shrink function running with a copy of the current execution context (db conn etc) in background thread
            await asyncio.wait_for(
                asyncio.get_running_loop().run_in_executor(
                    pool, copy_context().run, shrink_func
                ),
                timeout=timeout,
            )
        except (TimeoutError, asyncio.exceptions.TimeoutError):
            logger.error(
                f"Failed to complete cache shrink within {timeout}s. Trying again in {sleep_time}s."
            )
        if not loop:
            break
        await asyncio.sleep(sleep_time)
