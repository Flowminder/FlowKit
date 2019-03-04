# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import sys

app_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "flowapi")
)
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "flowapi"))
)

# import app
