import pytest


def test_uid(flowetl_container):
    """
    test that we can run the flowetl container with a specific user
    """

    user = "1002:1003"
    out = flowetl_container.exec_run("bash -c 'id -u'", user=user).output.decode(
        "utf-8"
    )
    assert out.strip() == "1002"


def test_gid(flowetl_container):
    """
    test that we can run the flowetl container with a specific user
    """

    user = "1002:1003"
    out = flowetl_container.exec_run("bash -c 'id -g'", user=user).output.decode(
        "utf-8"
    )
    assert out.strip() == "1003"
