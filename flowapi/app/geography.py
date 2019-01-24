# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from quart import Blueprint, current_app, request, stream_with_context, jsonify
from .check_claims import check_geography_claims

blueprint = Blueprint(__name__, __name__)


@blueprint.route("/geography/<aggregation_unit>")
@check_geography_claims()
async def get_geography(aggregation_unit):
    request.socket.send_json(
        {
            "request_id": request.request_id,
            "action": "get_geography",
            "params": {"aggregation_unit": aggregation_unit},
        }
    )
    #  Get the reply.
    message = await request.socket.recv_json()
    current_app.logger.debug(f"Got message: {message}")
    if message["status"] == "done":
        results_streamer = stream_with_context(assemble_geojson_feature_collection)(
            message["sql"], message["crs"]
        )
        mimetype = "application/geo+json"

        current_app.logger.debug(f"Returning {aggregation_unit} geography data.")
        return (
            results_streamer,
            200,
            {
                "Transfer-Encoding": "chunked",
                "Content-Disposition": f"attachment;filename={aggregation_unit}.geojson",
                "Content-type": mimetype,
            },
        )
    elif message["status"] == "error":
        return jsonify({"status": "Error", "msg": message["error"]}), 403
    else:
        return jsonify({}), 404


async def assemble_geojson_feature_collection(sql_query, crs):
    """
    Assemble the GeoJSON "Feature" objects from the query response into a
    "FeatureCollection" object.

    Parameters
    ----------
    sql_query : str
        SQL query to stream output of
    crs : str
        Coordinate reference system

    Yields
    ------
    bytes
        Encoded lines of JSON

    """
    logger = current_app.logger
    pool = current_app.pool
    yield f'{{"properties":{{"crs":"{crs}"}}, "type":"FeatureCollection", "features":['.encode()
    prepend = ""
    logger.debug("Starting generator.")
    async with pool.acquire() as connection:
        logger.debug("Connected.")
        async with connection.transaction():
            logger.debug("Got transaction.")
            logger.debug(f"Running {sql_query}")
            try:
                async for row in connection.cursor(sql_query):
                    yield f"{prepend}{row[0]}".encode()
                    prepend = ", "
                logger.debug("Finishing up.")
                yield b"]}"
            except Exception as e:
                logger.error(e)
