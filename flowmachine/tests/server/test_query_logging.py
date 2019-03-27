# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from flowmachine.core.init import _init_logging

from flowmachine.core import Query


@pytest.mark.asyncio
async def test_query_run_logged(json_log):
    # Local import so pytest can capture stdout
    from flowmachine.core.server.server import get_reply_for_message
    from flowmachine.core.server.zmq_interface import ZMQMultipartMessage

    msg_contents = b'{"action": "run_query", "request_id": "DUMMY_API_REQUEST_ID", "query_kind": "foobar", "params":{"param1": "some_value", "param2": "another_value"}}'
    multipart_msg = (b"DUMMY_RETURN_ADDRESS", b"", msg_contents)
    zmq_message = ZMQMultipartMessage(multipart_msg)
    _init_logging("ERROR")  # Logging of query runs should be independent of other logs
    Query.redis.get.return_value = (
        b"known"
    )  # Mock enough redis to get to the log messages
    await get_reply_for_message(zmq_message)
    log_lines = json_log()

    log_lines = log_lines.out
    assert log_lines[0]["request_id"] == "DUMMY_API_REQUEST_ID"
    assert log_lines[0]["query_id"] == "known"
    assert log_lines[0]["event"] == "run_query"
    assert log_lines[0]["logger"] == "flowmachine.query_run_log"
