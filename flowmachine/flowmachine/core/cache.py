# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

"""
Functions which deal with inspecting and managing the query cache.
"""

import pickle

from typing import TYPE_CHECKING, Tuple, List, Callable, Optional

from psycopg2 import InternalError

from redis import StrictRedis
import psycopg2
from sqlalchemy.engine import Engine

from flowmachine.core.errors.flowmachine_errors import (
    QueryCancelledException,
    QueryErroredException,
    StoreFailedException,
)
from flowmachine.core.query_state import QueryStateMachine, QueryEvent

if TYPE_CHECKING:
    from .query import Query
    from .connection import Connection

import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


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
    logger.debug(f"Trying to switch '{query.md5}' to executing state.")
    q_state_machine = QueryStateMachine(redis, query.md5)
    current_state, this_thread_is_owner = q_state_machine.execute()
    if this_thread_is_owner:
        logger.debug(f"In charge of executing '{query.md5}'.")
        query_ddl_ops = ddl_ops_func(name, schema)
        logger.debug("Made SQL.")
        con = connection.engine
        with con.begin():
            try:
                plan_time = write_func(query_ddl_ops, con)
                logger.debug("Executed queries.")
            except Exception as e:
                q_state_machine.raise_error()
                logger.error(f"Error executing SQL. Error was {e}")
                raise e
            if schema == "cache":
                write_cache_metadata(connection, query, compute_time=plan_time)
        q_state_machine.finish()

    q_state_machine.wait_until_complete(sleep_duration=sleep_duration)
    if q_state_machine.is_completed:
        return query
    elif q_state_machine.is_cancelled:
        logger.error(f"Query '{query.md5}' was cancelled.")
        raise QueryCancelledException(query.md5)
    elif q_state_machine.is_errored:
        logger.error(f"Query '{query.md5}' finished with an error.")
        raise QueryErroredException(query.md5)
    else:
        logger.error(
            f"Query '{query.md5}' not stored. State is {q_state_machine.current_query_state}"
        )
        raise StoreFailedException(query.md5)


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

    from ..__init__ import __version__

    con = connection.engine

    self_storage = b""
    try:
        self_storage = pickle.dumps(query)
    except Exception as e:
        logger.debug("Can't pickle ({e}), attempting to cache anyway.")
        pass

    try:
        in_cache = bool(
            connection.fetch(f"SELECT * FROM cache.cached WHERE query_id='{query.md5}'")
        )

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
                    query.md5,
                    __version__,
                    query._make_query(),
                    compute_time,
                    query.__class__.__name__,
                    *query.fully_qualified_table_name.split("."),
                    psycopg2.Binary(self_storage),
                ),
            )
            con.execute("SELECT touch_cache(%s);", query.md5)
            con.execute("SELECT pg_notify(%s, 'Done.')", query.md5)
            logger.debug("{} added to cache.".format(query.fully_qualified_table_name))
            if not in_cache:
                for dep in query._get_stored_dependencies(exclude_self=True):
                    con.execute(
                        "INSERT INTO cache.dependencies values (%s, %s) ON CONFLICT DO NOTHING",
                        (query.md5, dep.md5),
                    )
    except NotImplementedError:
        logger.debug("Table has no standard name.")


def touch_cache(connection: "Connection", query_id: str) -> float:
    """
    'Touch' a cache record and update the cache score for it.

    Parameters
    ----------
    connection : Connection
    query_id : str
        md5 id of the query to touch

    Returns
    -------
    float
        The new cache score
    """
    try:
        return float(connection.fetch(f"SELECT touch_cache('{query_id}')")[0][0])
    except (IndexError, InternalError):
        raise ValueError(f"Query id '{query_id}' is not in cache on this connection.")


def reset_cache(connection: "Connection", redis: StrictRedis) -> None:
    """
    Reset the query cache. Deletes any tables under cache schema, resets the cache count,
    clears the cached and dependencies tables.

    Parameters
    ----------
    connection : Connection
    redis : StrictRedis

    Notes
    -----
    You _must_ ensure that no queries are currently running when calling this function.
    Any queries currently running will no longer be tracked by redis, and UNDEFINED BEHAVIOUR
    will occur.
    """
    # For deletion purposes, we ignore Table objects. These either point to something
    # outside the cache schema and hence shouldn't be removed by calling this, or
    # they point to a cache table and hence are a duplicate of a Query entry which
    # will also point to that table.

    qry = f"SELECT tablename FROM cache.cached WHERE NOT class='Table'"
    tables = connection.fetch(qry)
    with connection.engine.begin() as trans:
        trans.execute("SELECT setval('cache.cache_touches', 1)")
        for table in tables:
            trans.execute(f"DROP TABLE IF EXISTS cache.{table[0]} CASCADE")
        trans.execute("TRUNCATE cache.cached CASCADE")
        trans.execute("TRUNCATE cache.dependencies CASCADE")
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
            new_state, changed = QueryStateMachine(redis, qid[0]).trigger_event(event)
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
        md5 id of the query
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
        md5 id of the query

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
    connection: "Connection"
) -> List[Tuple["Query", int]]:
    """
    Get all cached query objects in ascending cache score order.

    Parameters
    ----------
    connection : Connection

    Returns
    -------
    list of tuples
        Returns a list of cached Query objects with their on disk sizes

    """
    qry = """SELECT obj, table_size(tablename, schema) as table_size
        FROM cache.cached
        WHERE NOT cached.class='Table'
        ORDER BY cache_score(cache_score_multiplier, compute_time, table_size(tablename, schema)) ASC
        """
    cache_queries = connection.fetch(qry)
    return [(pickle.loads(obj), table_size) for obj, table_size in cache_queries]


def shrink_one(connection: "Connection", dry_run: bool = False) -> "Query":
    """
    Remove the lowest scoring cached query from cache and return it and size of it
    in bytes.

    Parameters
    ----------
    connection : "Connection"
    dry_run : bool, default False
        Set to true to just report the object that would be removed and not remove it

    Returns
    -------
    tuple of "Query", int
        The "Query" object that was removed from cache and the size of it
    """
    obj_to_remove, obj_size = get_cached_query_objects_ordered_by_score(connection)[0]

    logger.info(
        f"{'Would' if dry_run else 'Will'} remove cache record for {obj_to_remove.md5} of type {obj_to_remove.__class__}"
    )
    logger.info(
        f"Table {obj_to_remove.fully_qualified_table_name} ({obj_size} bytes) {'would' if dry_run else 'will'} be removed."
    )

    if not dry_run:
        obj_to_remove.invalidate_db_cache(cascade=False, drop=True)
    return obj_to_remove, obj_size


def shrink_below_size(
    connection: "Connection", size_threshold: int = None, dry_run: bool = False
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

    Returns
    -------
    list of "Query"
        List of the queries that were removed
    """
    initial_cache_size = get_size_of_cache(connection)
    removed = []
    logger.info(
        f"Shrinking cache from {initial_cache_size} to below {size_threshold}{' (dry run)' if dry_run else ''}."
    )

    if dry_run:
        cached_queries = iter(get_cached_query_objects_ordered_by_score(connection))

        def dry_run_shrink(connection):
            obj, obj_size = cached_queries.__next__()
            logger.info(
                f"Would remove cache record for {obj.md5} of type {obj.__class__}"
            )
            logger.info(
                f"Table {obj.fully_qualified_table_name} ({obj_size} bytes) would be removed."
            )
            return obj, obj_size

        shrink = dry_run_shrink
    else:
        shrink = shrink_one

    if size_threshold is None:
        size_threshold = get_max_size_of_cache(connection)

    while initial_cache_size > size_threshold:
        obj_removed, cache_reduction = shrink(connection)
        removed.append(obj_removed)
        initial_cache_size -= cache_reduction
    logger.info(
        f"New cache size {'would' if dry_run else 'will'} be {initial_cache_size}."
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
        WHERE NOT cached.class='Table'"""
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
        md5 identifier of the query

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
        md5 id of the cached query

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
        md5 id of the cached query

    Returns
    -------
    bool
    """
    try:
        _ = get_query_object_by_id(connection, query_id)
        return True
    except ValueError:
        return False
