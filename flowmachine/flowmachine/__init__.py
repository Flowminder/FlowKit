# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
FlowMachine is a Python based SQL wrapper built specifically for Flowminder's
PostgreSQL CDR database: Flowdb.

It provides a standardised way of interfacing with our postgres database. 
FlowMachine provides SQL based queries that compute commonly used features and
metrics to do with CDR data, such as `DailyLocations`, `ModalLocations`,
`Flows` and `RadiusOfGyration`.

The heart of flowmachine is the AbstractBaseClass Query, which defines how an
SQL-based feature behaves. All metrics define an SQL querty, 
and inherit from FlowMachine's main `Query()` class.

"""


from ._version import get_versions

__version__ = get_versions()["version"]
__flowdb_version__ = "0.2.0"

del get_versions

import structlog
import rapidjson
import logging

root_logger = logging.getLogger("flowmachine")
root_logger.setLevel(logging.DEBUG)
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(serializer=rapidjson.dumps),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

from .core.init import connect
from .features.utilities import GroupValues, feature_collection
import flowmachine.models
import flowmachine.features
import flowmachine.utils
import flowmachine.core

methods = ["GroupValues", "feature_collection", "connect"]

sub_modules = ["core", "features", "utils", "models"]


__all__ = methods + sub_modules
