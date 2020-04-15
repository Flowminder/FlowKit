# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import Dict, Type
from collections import ChainMap

import pkg_resources
import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


def get_type_schemas_from_entrypoint(entry_point_name: str) -> Dict[str, Type]:
    logger.info("Loading type schemas.", entry_point=entry_point_name)
    type_schemas = dict()
    for entry_point in (
        entry_point
        for entry_point in pkg_resources.iter_entry_points("flowkit.queries")
        if entry_point.name == entry_point_name
    ):
        try:
            schemas = entry_point.load()
            logger.info(
                "Loaded type schemas", entry_point=entry_point_name, schemas=schemas
            )
            type_schemas = dict(type_schemas, **schemas)
        except Exception as exc:
            logger.error(
                "Failed to load type schema", entry_point=entry_point, exception=exc
            )
    return type_schemas
