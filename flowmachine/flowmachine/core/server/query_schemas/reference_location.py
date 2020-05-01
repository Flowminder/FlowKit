# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow_oneofschema import OneOfSchema
from flowmachine.core.server.query_schemas.util import get_type_schemas_from_entrypoint


class ReferenceLocationSchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = get_type_schemas_from_entrypoint("reference_location_queries")
