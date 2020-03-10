# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import csv
from itertools import chain

import rapidjson as json
from quart import current_app, request


async def stream_result_as_json(
    sql_query, result_name="query_result", additional_elements=None
):
    """
    Generate a JSON representation of a query result.

    Parameters
    ----------
    sql_query : str
        SQL query to stream output of
    result_name : str
        Name of the JSON item containing the rows of the result
    additional_elements : dict
        Additional JSON elements to include along with the query result

    Yields
    ------
    bytes
        Encoded lines of JSON

    """
    logger = current_app.flowapi_logger
    db_conn_pool = current_app.db_conn_pool
    prefix = "{"
    if additional_elements:
        for key, value in additional_elements.items():
            prefix += f'"{key}":{json.dumps(value)}, '
    prefix += f'"{result_name}":['
    yield prefix.encode()
    prepend = ""
    logger.debug("Starting generator.", request_id=request.request_id)
    async with db_conn_pool.acquire() as connection:
        # Configure asyncpg to encode/decode JSON values
        await connection.set_type_codec(
            "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )
        logger.debug("Connected.", request_id=request.request_id)
        async with connection.transaction():
            logger.debug("Got transaction.", request_id=request.request_id)
            logger.debug(f"Running {sql_query}", request_id=request.request_id)
            try:
                async for row in connection.cursor(sql_query):
                    yield f"{prepend}{json.dumps(dict(row.items()), number_mode=json.NM_DECIMAL, datetime_mode=json.DM_ISO8601)}".encode()
                    prepend = ", "
                logger.debug("Finishing up.", request_id=request.request_id)
                yield b"]}"
            except Exception as e:
                logger.error(e)


class Line(object):
    def __init__(self):
        self._line = None

    def write(self, line):
        self._line = line

    def read(self):
        return self._line.encode()


async def stream_result_as_csv(sql_query, result_name=None, additional_elements=None):
    """
    Generate a JSON representation of a query result.

    Parameters
    ----------
    sql_query : str
        SQL query to stream output of
    result_name : str
        Name of the JSON item containing the rows of the result
    additional_elements : dict
        Additional columns elements to include along with the query result

    Yields
    ------
    bytes
        Encoded lines of JSON

    """
    logger = current_app.flowapi_logger
    db_conn_pool = current_app.db_conn_pool
    logger.debug("Starting generator.", request_id=request.request_id)
    yield_header = True
    line = Line()
    writer = csv.writer(line)
    if additional_elements is None:
        additional_elements = {}
    async with db_conn_pool.acquire() as connection:
        # Configure asyncpg to encode/decode JSON values
        await connection.set_type_codec(
            "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )
        logger.debug("Connected.", request_id=request.request_id)
        async with connection.transaction():
            logger.debug("Got transaction.", request_id=request.request_id)
            logger.debug(f"Running {sql_query}", request_id=request.request_id)
            try:
                async for row in connection.cursor(sql_query):
                    if yield_header:
                        writer.writerow(chain(row.keys(), additional_elements.keys()))
                        yield line.read()
                        yield_header = False
                    writer.writerow(chain(row.values(), additional_elements.values()))
                    yield line.read()
                logger.debug("Finishing up.", request_id=request.request_id)
            except Exception as e:
                logger.error(e)
