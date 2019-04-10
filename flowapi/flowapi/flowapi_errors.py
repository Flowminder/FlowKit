# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from quart import Response
from quart.exceptions import BadRequest
import rapidjson as json


class JSONHTTPException(BadRequest):
    def get_body(self) -> str:
        return json.dumps({"msg": self.description})

    def get_response(self) -> Response:
        return Response(
            self.get_body(), status=self.status_code, headers=self.get_headers()
        )

    def get_headers(self) -> dict:
        return {"Content-Type": "application/json"}


class MissingQueryKindError(JSONHTTPException):
    description = "Query kind must be specified when running a query."
    name = "Bad query."


class MissingAggregationUnitError(JSONHTTPException):
    description = "Query kind must be specified when running a query."
    name = "Bad query."
