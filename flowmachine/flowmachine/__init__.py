# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
FlowMachine is a Python based SQL wrapper built specifically for Flowminder's
PostgreSQL CDR database: Flowdb.

It provides a standardised way of interfacing with our postgres database. 
FlowMachine provides SQL based queries that compute commonly used features and
metrics to do with CDR data, such as `DailyLocations`, `HomeLocations`, 
`Flows` and `RadiusOfGyration`.

The heart of flowmachine is the AbstractBaseClass Query, which defines how an
SQL-based feature behaves. All metrics define an SQL querty, 
and inherit from FlowMachine's main `Query()` class.

"""

from ._version import get_versions

__version__ = get_versions()["version"]
__flowdb_version__ = "*"

del get_versions

from .core.init import connect
from .features.utilities import GroupValues, FeatureCollection
import flowmachine.models
import flowmachine.features
import flowmachine.utils
import flowmachine.core

methods = ["GroupValues", "FeatureCollection", "connect"]

sub_modules = ["core", "features", "utils", "models"]


__all__ = methods + sub_modules
