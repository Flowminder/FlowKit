# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
import os
import pytest

from unittest.mock import Mock
from etl.etl_utils import get_session


def test_get_session_default(monkeypatch):
    """
    Make sure we are picking up the flowdb connection string from env
    """
    db_name = "bob"
    host = "steve"
    password = "jimmy"
    port = 6666
    user = "sarah"

    monkeypatch.setattr(
        "os.environ",
        {
            **os.environ,
            **{
                "AIRFLOW_CONN_FLOWDB": f"postgres://{user}:{password}@{host}:{port}/{db_name}"
            },
        },
    )
    mock_psycopg2_connect = Mock()
    monkeypatch.setattr("psycopg2.connect", mock_psycopg2_connect)

    s = get_session()

    try:
        s.connection()
    except TypeError:
        # we get an exception because not a real
        # connection catching and ignoring
        pass

    mock_psycopg2_connect.assert_called_once_with(
        dbname=db_name, host=host, password=password, port=port, user=user
    )


def test_get_session_fails_if_env_var_not_set(monkeypatch):
    """
    Make sure we get ValueError if env var AIRFLOW_CONN_FLOWDB is not set.
    """
    monkeypatch.delenv("AIRFLOW_CONN_FLOWDB", raising=False)
    mock_psycopg2_connect = Mock()
    monkeypatch.setattr("psycopg2.connect", mock_psycopg2_connect)

    with pytest.raises(ValueError):
        s = get_session()


def test_get_session_non_default(monkeypatch):
    """
    Make sure we are picking up the connection string from env
    when non default postgres_conn_id is used
    """
    db_name = "bob"
    host = "steve"
    password = "jimmy"
    port = 6666
    user = "sarah"

    monkeypatch.setattr(
        "os.environ",
        {
            **os.environ,
            **{
                "AIRFLOW_CONN_SOMEID": f"postgres://{user}:{password}@{host}:{port}/{db_name}"
            },
        },
    )
    mock_psycopg2_connect = Mock()
    monkeypatch.setattr("psycopg2.connect", mock_psycopg2_connect)

    s = get_session(postgres_conn_id="someid")

    try:
        s.connection()
    except TypeError:
        # we get an exception because not a real
        # connection catching and ignoring
        pass

    mock_psycopg2_connect.assert_called_once_with(
        dbname=db_name, host=host, password=password, port=port, user=user
    )


def test_get_session_non_default_fails_if_env_not_set(monkeypatch):
    """
    Make sure if env not set we get ValueError when using non default
    postgres_conn_id
    """
    mock_psycopg2_connect = Mock()
    monkeypatch.setattr("psycopg2.connect", mock_psycopg2_connect)

    with pytest.raises(ValueError):
        s = get_session(postgres_conn_id="someid")
