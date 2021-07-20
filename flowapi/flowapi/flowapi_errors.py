# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from werkzeug.exceptions import HTTPException
import rapidjson as json


class JSONHTTPException(HTTPException):
    def get_body(self) -> str:
        return json.dumps({"msg": self.description})

    def get_headers(self) -> dict:
        return {"Content-Type": "application/json"}


class BadQueryError(JSONHTTPException):
    def __init__(self):
        super().__init__(
            status_code=400, description="Could not parse query spec.", name="Bad query"
        )


class MissingQueryKindError(JSONHTTPException):
    def __init__(self):
        super().__init__(
            status_code=400,
            description="Query kind must be specified when running a query.",
            name="Bad query",
        )


class MissingAggregationUnitError(JSONHTTPException):
    def __init__(self):
        super().__init__(
            status_code=400,
            description="Aggregation unit must be specified when running a query.",
            name="Bad query",
        )
