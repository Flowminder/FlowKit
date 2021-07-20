# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from unittest.mock import Mock
import importlib

import pytest
from flowmachine.core.context import get_interpreter_id, get_executor


@pytest.fixture(autouse=True)
def reload_context(monkeypatch):
    """
    Need to forcibly reimport context at exit because we've fiddled with it
    in the tests.
    """
    import flowmachine

    try:
        yield
    finally:
        monkeypatch.delattr(flowmachine.core.context, "get_ipython", raising=False)
        importlib.reload(flowmachine.core.context)


@pytest.fixture
def flowmachine_connect():  # Override the autoused fixture from the parent
    pass


def test_consistent_interpreter_id():
    import flowmachine

    flowmachine.connect()
    assert get_executor().submit(get_interpreter_id).result() == get_interpreter_id()


def test_context_manager():
    import flowmachine

    flowmachine.core.context.bind_context(
        Mock(mock_name="db"),
        Mock(mock_name="pool"),
        Mock(name="cm_redis", mock_name="redis"),
    )
    assert flowmachine.core.context.get_db().mock_name == "db"
    assert flowmachine.core.context.get_executor().mock_name == "pool"
    assert flowmachine.core.context.get_redis().mock_name == "redis"
    with flowmachine.core.context.context(
        Mock(mock_name="db_2"), Mock(mock_name="pool_2"), Mock(mock_name="redis_2")
    ):
        assert flowmachine.core.context.get_db().mock_name == "db_2"
        assert flowmachine.core.context.get_executor().mock_name == "pool_2"
        assert flowmachine.core.context.get_redis().mock_name == "redis_2"


@pytest.mark.parametrize(
    "shell, expected_result",
    [
        ("ZMQInteractiveShell", True),
        ("NOT_A_SHELL", False),
        ("TerminalInteractiveShell", False),
    ],
)
def test_notebook_detection_with_ipython_shell(shell, expected_result, monkeypatch):
    """
    If get_ipython is defined, test that we're detecting if in a notebook.
    """
    get_ipython = Mock()
    get_ipython.return_value = Mock(__class__=Mock(__name__=shell))
    import flowmachine

    monkeypatch.setattr(
        flowmachine.core.context, "get_ipython", get_ipython, raising=False
    )
    importlib.reload(flowmachine.core.context)

    assert flowmachine.core.context._is_notebook == expected_result


def test_notebook_detection_without_ipython_shell(monkeypatch):
    """
    Test that we aren't detecting ipython if it isn't there.
    """
    import flowmachine

    monkeypatch.delattr(flowmachine.core.context, "get_ipython", raising=False)
    importlib.reload(flowmachine.core.context)

    assert not flowmachine.core.context._is_notebook
    assert len(flowmachine.core.context._jupyter_context) == 1


def test_notebook_workaround(monkeypatch):
    """
    If get_ipython is defined, test that we're detecting if in a notebook and applying workaround.
    """
    get_ipython = Mock()
    get_ipython.return_value = Mock(__class__=Mock(__name__="ZMQInteractiveShell"))
    import flowmachine

    monkeypatch.setattr(
        flowmachine.core.context, "get_ipython", get_ipython, raising=False
    )
    importlib.reload(flowmachine.core.context)

    flowmachine.core.context.bind_context(
        Mock(mock_name="db"),
        Mock(mock_name="pool"),
        Mock(name="nb_workaround_redis", mock_name="redis"),
    )
    assert flowmachine.core.context._jupyter_context["db"].mock_name == "db"
    assert flowmachine.core.context._jupyter_context["executor"].mock_name == "pool"
    assert (
        flowmachine.core.context._jupyter_context["redis_connection"].mock_name
        == "redis"
    )
    assert flowmachine.core.context.get_db().mock_name == "db"
    assert flowmachine.core.context.get_executor().mock_name == "pool"
    assert flowmachine.core.context.get_redis().mock_name == "redis"
