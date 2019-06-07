# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Definition of the main Abstract Base Class of flowmachine the Query class
and associated code.
"""

from .connection import Connection
from .query import Query
from .table import Table
from .geotable import GeoTable
from .init import connect
from .logging import init_logging, set_log_level
from .spatial_unit import make_spatial_unit
from .join_to_location import JoinToLocation, location_joined_query
from .custom_query import CustomQuery
from .grid import Grid

sub_modules = ["errors", "mixins", "api"]

methods = [
    "Query",
    "Table",
    "GeoTable",
    "Connection",
    "connect",
    "make_spatial_unit",
    "JoinToLocation",
    "location_joined_query",
    "CustomQuery",
    "Grid",
]

__all__ = methods + sub_modules
