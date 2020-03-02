# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
The FlowETL module augments AirFlow with several special purpose operators and sensors, default qa checks,
and a utility function for easily creating ETL DAGs for CDR data.

FlowETL is built into the FlowETL docker container, but can also be installed independently.
"""

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
