# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

"""
Functions which deal with inspecting cached tables.
"""
import logging
import pickle

from flowmachine.core import Query, Connection

logger = logging.getLogger("flowmachine").getChild(__name__)


def shrink_one(connection: Connection, dry_run: bool = False) -> Query:
    """
    Remove the lowest scoring cached query from cache and return it.

    Parameters
    ----------
    connection : Connection
    dry_run : bool, default False
        Set to true to just report the object that would be removed and not remove it

    Returns
    -------
    Query
        The Query object that was removed from cache
    """
    qry = "SELECT tablename, schema, obj FROM cache.cached WHERE NOT class='Table' ORDER BY cache_score ASC LIMIT 1"
    tablename, schema, obj = connection.fetch(qry)[0]
    obj_to_remove = pickle.loads(obj)
    if not dry_run:
        logger.info(
            f"Removing cache record for {obj_to_remove.md5} of type {obj_to_remove.__cls__}"
        )
        logger.info(f"Table {schema}.{tablename} will be removed.")
        obj_to_remove.invalidate_db_cache(name=tablename, schema=schema, cascade=False)
    return obj_to_remove


def size_of_table(connection: Connection, table_name: str, table_schema: str) -> int:
    """
    Get the size on disk in bytes of a table in the database.

    Parameters
    ----------
    connection : Connection
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
    return connection.fetch(sql)[0][0]


def size_of_cache(connection: Connection) -> int:
    """
    Get the total size in bytes of all cache tables.

    Parameters
    ----------
    connection : Connection

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
    return connection.fetch(sql)[0][0]


def compute_time(connection: Connection, query_id: str) -> float:
    """
    Get the time in ms that a cached query took to compute.

    Parameters
    ----------
    connection : Connection
    query_id : str
        md5 identifier of the query

    Returns
    -------
    float
        Number of seconds the query took to compute

    """
    return float(
        connection.fetch(
            f"SELECT compute_time FROM cache.cached WHERE query_id='{query_id}'"
        )[0][0]
        / 1000
    )


def score(connection: Connection, query_id: str) -> float:
    """
    Get the current cache score for a cached query.

    Parameters
    ----------
    connection: Connection
    query_id : str
        md5 id of the cached query

    Returns
    -------
    float
        Current cache score of this query

    """
    return float(
        connection.fetch(
            f"SELECT cache_score FROM cache.cached WHERE query_id='{query_id}'"
        )[0][0]
    )


def rescore(connection: Connection, query: Query, halflife: float) -> float:
    """
    Calculate a new cache score for a cached query object.

    Parameters
    ----------
    connection : Connection
    query : Query
        Query object to score
    halflife : float
        Memory decay halflife. Smaller values will decay more slowly.

    Returns
    -------
    float
        Updated cache score
    """
    byte_size = size_of_table(connection, *query.table_name.split(".")[::-1])
    runtime = compute_time(connection, query.md5)
    last_score = score(connection, query.md5)
    return last_score + runtime / byte_size * (1 + halflife)
