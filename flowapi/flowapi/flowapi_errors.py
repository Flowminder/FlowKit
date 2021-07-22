# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from werkzeug.exceptions import BadRequest
import rapidjson as json


class JSONHTTPException(BadRequest):
    def get_body(self, environ, scope) -> str:
        return json.dumps({"msg": self.description})

    def get_headers(self, environ, scope) -> dict:
        return [("Content-Type", "application/json")]


class BadQueryError(JSONHTTPException):
    name = "Bad query"

    def __init__(self):
        super().__init__(
            description="Could not parse query spec.",
        )


class MissingQueryKindError(JSONHTTPException):
    def __init__(self):
        super().__init__(
            description="Query kind must be specified when running a query.",
        )


class MissingAggregationUnitError(JSONHTTPException):
    def __init__(self):
        super().__init__(
            description="Aggregation unit must be specified when running a query.",
        )
