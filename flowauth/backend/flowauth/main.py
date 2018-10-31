# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import sys, os

PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, PATH)

from . import create_app

app = create_app()
