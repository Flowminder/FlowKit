# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import rapidjson
import structlog
from redis import StrictRedis

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class QueryInfoLookupError(Exception):
    """
    Exception indicating an error with the query info lookup.
    """


class UnkownQueryIdError(QueryInfoLookupError):
    """
    Exception indicating an error with the query info lookup.
    """


class QueryInfoLookup:
    """
    Implements a lookup from the query_id to query parameters and vice versa (backed by redis).
    """

    def __init__(self, redis_client: StrictRedis):
        self.redis_client = redis_client

    def register_query(self, query_id: str, query_params: dict) -> None:
        logger.debug(
            f"Registering query lookup for query_id='{query_id}' with query_params {query_params}"
        )

        if "query_kind" not in query_params:
            raise QueryInfoLookupError(
                "Query params must contain a 'query_kind' entry."
            )
        query_params_str = rapidjson.dumps(query_params)
        self.redis_client.set(query_id, query_params_str)
        self.redis_client.set(query_params_str, query_id)

    def query_is_known(self, query_id: str) -> bool:
        is_known = self.redis_client.get(query_id) is not None
        return is_known

    def get_query_params(self, query_id: str) -> dict:
        query_params_str = self.redis_client.get(query_id)
        if query_params_str is None:
            raise UnkownQueryIdError(f"Unknown query_id: '{query_id}'")
        return rapidjson.loads(query_params_str)

    def get_query_kind(self, query_id: str) -> str:
        query_params = self.get_query_params(query_id)
        return query_params["query_kind"]

    def get_query_id(self, query_params: dict) -> str:
        query_params_str = rapidjson.dumps(query_params)
        query_id = self.redis_client.get(query_params_str)
        if query_id is None:
            raise QueryInfoLookupError("No id for these params.")
        return query_id.decode()
