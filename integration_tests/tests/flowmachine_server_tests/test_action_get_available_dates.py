# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core.server.utils import send_zmq_message_and_receive_reply

# TODO: add test for code path that raises QueryProxyError with the 'get_params' action


def test_get_available_dates(zmq_port, zmq_host):
    """
    action 'get_available_dates' against an existing query_id returns the expected parameters with which the query was run.
    """
    msg = {"action": "get_available_dates", "request_id": "DUMMY_ID"}

    reply = send_zmq_message_and_receive_reply(msg, port=zmq_port, host=zmq_host)
    assert reply["status"] == "success"

    expected_reply = {
        "status": "success",
        "msg": "",
        "payload": {
            "calls": [
                "2016-01-01",
                "2016-01-02",
                "2016-01-03",
                "2016-01-04",
                "2016-01-05",
                "2016-01-06",
                "2016-01-07",
            ],
            "mds": [
                "2016-01-01",
                "2016-01-02",
                "2016-01-03",
                "2016-01-04",
                "2016-01-05",
                "2016-01-06",
                "2016-01-07",
            ],
            "topups": [
                "2016-01-01",
                "2016-01-02",
                "2016-01-03",
                "2016-01-04",
                "2016-01-05",
                "2016-01-06",
                "2016-01-07",
            ],
            "sms": [
                "2016-01-01",
                "2016-01-02",
                "2016-01-03",
                "2016-01-04",
                "2016-01-05",
                "2016-01-06",
                "2016-01-07",
            ],
        },
    }
    assert expected_reply == reply
