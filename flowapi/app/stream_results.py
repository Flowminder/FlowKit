# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import ujson as json
from quart import current_app


async def stream_result_as_json(sql_query, headers={}, result_name="query_result"):
    """
    Generate a JSON representation of a query result.
    Parameters
    ----------
    sql_query : str
        SQL query to stream output of
    headers : dict
        Additional items to encode along with the query result
    result_name : str
        Name of the JSON item containing the rows of the result

    Yields
    ------
    bytes
        Encoded lines of JSON

    """
    logger = current_app.logger
    pool = current_app.pool
    prefix = "{"
    for key in headers:
        prefix += f'"{key}":{json.dumps(headers[key])}, '
    prefix += f'"{result_name}":['
    yield prefix.encode()
    prepend = ""
    logger.debug("Starting generator.")
    async with pool.acquire() as connection:
        # Configure asyncpg to encode/decode JSON values
        await connection.set_type_codec(
            "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )
        logger.debug("Connected.")
        async with connection.transaction():
            logger.debug("Got transaction.")
            logger.debug(f"Running {sql_query}")
            try:
                async for row in connection.cursor(sql_query):
                    yield f"{prepend}{json.dumps(dict(row.items()))}".encode()
                    prepend = ", "
                logger.debug("Finishing up.")
                yield b"]}"
            except Exception as e:
                logger.error(e)
