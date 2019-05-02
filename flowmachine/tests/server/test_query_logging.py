# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
import sys
from logging import getLogger

import pytest
from flowmachine.core.init import _set_log_level
from flowmachine.core.server.server import get_reply_for_message
from flowmachine.core import Query


def test_query_run_logged(json_log):
    # Local import so pytest can capture stdout
    logger = getLogger("flowmachine.query_run_log")
    logger.handlers[0].stream = sys.stdout  # Reset log stream for capsys
    msg_contents = {
        "action": "run_query",
        "request_id": "DUMMY_API_REQUEST_ID",
        "params": {"query_kind": "dummy_query", "dummy_param": "DUMMY"},
    }
    _set_log_level("ERROR")  # Logging of query runs should be independent of other logs
    Query.redis.get.return_value = (
        b"known"
    )  # Mock enough redis to get to the log messages
    reply = get_reply_for_message(json.dumps(msg_contents))

    log_lines = json_log()
    print(reply)
    log_lines = log_lines.out
    assert log_lines[0]["request_id"] == "DUMMY_API_REQUEST_ID"
    assert log_lines[0]["action"] == "run_query"
    assert log_lines[0]["logger"] == "flowmachine.query_run_log"
