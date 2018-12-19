# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

"""
Functions which deal with inspecting cached tables.
"""
import logging
import pickle

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .query import Query
    from .connection import Connection

logger = logging.getLogger("flowmachine").getChild(__name__)


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
    "Query"
        The "Query" object that was removed from cache
    """
    qry = "SELECT tablename, schema, obj FROM cache.cached WHERE NOT class='Table' ORDER BY cache_score ASC LIMIT 1"
    tablename, schema, obj = connection.fetch(qry)[0]
    table_size = size_of_table(connection, tablename, schema)
    obj_to_remove = pickle.loads(obj)

    logger.info(
        f"{'Would' if dry_run else 'Will'} remove cache record for {obj_to_remove.md5} of type {obj_to_remove.__class__}"
    )
    logger.info(
        f"Table {schema}.{tablename} ({table_size} bytes) {'would' if dry_run else 'will'} be removed."
    )

    if not dry_run:
        obj_to_remove.invalidate_db_cache(
            name=tablename, schema=schema, cascade=False, drop=True
        )
    return obj_to_remove, table_size


def shrink_below_size(
    connection: "Connection", size_threshold: int, dry_run: bool = False
) -> "Query":
    """
    Remove queries from the cache until it is below a specified size threshold.

    Parameters
    ----------
    connection : "Connection"
    size_threshold : int
        Size (in bytes) to reduce the cache below
    dry_run : bool, default False
        Set to true to just report the objects that would be removed and not remove them

    Returns
    -------
    list of "Query"
        List of the queries that were removed
    """
    initial_cache_size = size_of_cache(connection)
    removed = []
    logger.info(
        f"Shrinking cache from {initial_cache_size} to below {size_threshold}{'(dry run)' if dry_run else ''}."
    )

    while initial_cache_size > size_threshold:
        obj_removed, cache_reduction = shrink_one(connection, dry_run)
        removed.append(obj_removed)
        initial_cache_size -= cache_reduction
    logger.info(
        f"New cache size {'would' if dry_run else 'will'} be {initial_cache_size}."
    )
    return removed


def size_of_table(connection: "Connection", table_name: str, table_schema: str) -> int:
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
    sql = f"""
          SELECT pg_total_relation_size(c.oid) AS total_bytes
              FROM pg_class c
              LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE relkind = 'r' AND relname='{table_name}' AND nspname='{table_schema}'
    """
    try:
        return int(connection.fetch(sql)[0][0])
    except IndexError:
        raise ValueError(
            f"Table '{table_schema}.{table_name}' does not exist on this connection."
        )


def size_of_cache(connection: "Connection") -> int:
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
    sql = """SELECT sum(pg_total_relation_size(c.oid)) as total_bytes 
        FROM pg_class c 
            LEFT JOIN pg_namespace n 
        ON n.oid=c.relnamespace 
        INNER JOIN cache.cached ON
         relname=cached.tablename AND nspname=cached.schema 
        WHERE NOT cached.class='Table'"""
    cache_bytes = connection.fetch(sql)[0][0]
    return 0 if cache_bytes is None else int(cache_bytes)


def compute_time(connection: "Connection", query_id: str) -> float:
    """
    Get the time in ms that a cached query took to compute.

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


def score(connection: "Connection", query_id: str) -> float:
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
                f"SELECT cache_score FROM cache.cached WHERE query_id='{query_id}'"
            )[0][0]
        )
    except IndexError:
        raise ValueError(f"Query id '{query_id}' is not in cache on this connection.")


def rescore(connection: "Connection", query: "Query", half_life: float) -> float:
    """
    Calculate a new cache score for a cached query object.

    Parameters
    ----------
    connection : "Connection"
    query : "Query"
        "Query" object to score
    half_life : float
        Memory decay halflife. Smaller values will decay more slowly.

    Returns
    -------
    float
        Updated cache score
    """
    byte_size = size_of_table(connection, *query.table_name.split(".")[::-1])
    runtime = compute_time(connection, query.md5)
    last_score = score(connection, query.md5)
    return last_score + runtime / byte_size * (1 + half_life)
