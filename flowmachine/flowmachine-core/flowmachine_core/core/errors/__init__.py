# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Custom Errors
"""

from .flowmachine_errors import (
    NameTooLongError,
    NotConnectedError,
    InvalidSpatialUnitError,
    MissingDateError,
    UnstorableQueryError,
)

__all__ = [
    "NameTooLongError",
    "NotConnectedError",
    "InvalidSpatialUnitError",
    "MissingDateError",
    "UnstorableQueryError",
]
