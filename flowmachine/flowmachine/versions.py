# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

__min_flowdb_version__ = "0.5.0"
