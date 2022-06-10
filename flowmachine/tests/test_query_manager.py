# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the query manger.
"""
from flowmachine.core.context import get_interpreter_id
from flowmachine.core.query_manager import set_managing, unset_managing, release_managed


def test_set_managing(dummy_redis):
    set_managing("DUMMY_ID", "DUMMY_DB_ID", dummy_redis)
    assert (
        dummy_redis.get("manager")[b"DUMMY_DB_ID-DUMMY_ID"]
        == get_interpreter_id().encode()
    )
    assert b"DUMMY_DB_ID-DUMMY_ID" in dummy_redis.get(
        f"managing:{get_interpreter_id()}"
    )


def test_unset_managing(dummy_redis):
    test_set_managing(dummy_redis)
    unset_managing("DUMMY_ID", "DUMMY_DB_ID", dummy_redis)
    assert b"DUMMY_DB_ID-DUMMY_ID" not in dummy_redis.get("manager")
    assert b"DUMMY_DB_ID-DUMMY_ID" not in dummy_redis.get(
        f"managing:{get_interpreter_id()}"
    )


def test_release_managed(dummy_redis):
    test_set_managing(dummy_redis)
    release_managed(dummy_redis)
    assert b"DUMMY_DB_ID-DUMMY_ID" not in dummy_redis.get("manager")
    assert b"DUMMY_DB_ID-DUMMY_ID" not in dummy_redis.get(
        f"managing:{get_interpreter_id()}"
    )
