# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import sys

import structlog


# Logger for all queries run or accessed
query_run_log = logging.getLogger("flowmachine").getChild("query_run_log")
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
query_run_log.addHandler(ch)
query_run_log = structlog.wrap_logger(query_run_log)
