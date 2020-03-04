# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Definition of the main Abstract Base Class of flowmachine_core the Query class
and associated code.
"""

from .connection import Connection
from .init import connect
from .logging import init_logging, set_log_level

sub_modules = ["errors", "mixins", "api"]

methods = [
    "Connection",
    "connect",
]

__all__ = methods + sub_modules
