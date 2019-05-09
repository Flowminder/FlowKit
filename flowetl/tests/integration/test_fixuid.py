import pytest


def test_uid(flowetl_run_command):
    """
    test that we can run the flowetl container with a specific user
    """

    user = "1002:1003"
    out = flowetl_run_command("bash -c 'id -u'", user=user)
    assert out.strip() == "1002"


def test_gid(flowetl_run_command):
    """
    test that we can run the flowetl container with a specific user
    """

    user = "1002:1003"
    out = flowetl_run_command("bash -c 'id -g'", user=user)
    assert out.strip() == "1003"
