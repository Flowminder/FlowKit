# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
autoflow.parser contains definitions for the 'parse_workflows_yaml' function and marshmallow schemas
for parsing a 'workflows.yml' file to define workflows and configure the available dates sensor.
"""

from .parser import parse_workflows_yaml
