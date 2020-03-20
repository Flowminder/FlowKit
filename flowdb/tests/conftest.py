# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Commonly used testing fixtures for flowdb.
"""

import os
import re
import pytest
import logging
import psycopg2 as pg
import psycopg2.extras

from collections import OrderedDict

logger = logging.getLogger()


def pytest_configure(config):

    config.addinivalue_line(
        "markers",
        "skip_usrs([]): skip the given test if it uses the usr fixture and the usr is included in the list passed to the marker.",
    )


def pytest_itemcollected(item):
    """
    Custom hook which improves stdout logging from from pytest's default.

    Instead of just printing the filename and no description of the test
    (as would be the default) it also prints the docstring.
    """
    if item.obj.__doc__:
        item._nodeid = f'{item._nodeid} ({" ".join(item.obj.__doc__.split())})'


class DBConn:
    """Manages a single database connection, creating and disposing of it.

    Parameters
    ----------
    request: pytest.FixtureRequest
        The fixture request which contains information about the connection
        which ones want to establish.
    """

    def __init__(self, request, env):
        self.env = env
        db_params = self._get_db_params(request)
        self.db_conn = pg.connect(
            f"postgresql://{db_params['usr']}:{db_params['pwd']}@localhost:{db_params['port']}/{db_params['db']}"
        )

    def _get_attr(self, request, attr, default):
        """Gets the relevant fixture value fron the request.

        Since it is not possible to know in advance whether the user will
        require to customize its access to the database, it is not possible to
        know whether specific database parameters will be available in the
        request. Therefore we attempt to recover the requested `attr` from the
        `request` and in case of failure, we simply return the default value
        specified by the function caller.
        """
        try:
            v = request.getfixturevalue(attr)
        except:
            v = default
        return v

    def _get_db_params(self, request):
        """Gets the DB params required to open a connection."""
        defaults = OrderedDict(
            [
                ("usr", self.env["POSTGRES_USER"]),
                ("pwd", self.env["POSTGRES_PASSWORD"]),
                ("port", self.env["FLOWDB_PORT"]),
                ("db", "flowdb"),
            ]
        )
        params = {k: self._get_attr(request, k, v) for k, v in defaults.items()}
        return params

    def get(self):
        """Returns the opened connection."""
        return self.db_conn

    def close(self):
        """Close the opened connection."""
        self.db_conn.close()


@pytest.fixture(autouse=True)
def _skip_usr(request):
    """Skip if the usr_env_prefix is listed in the list provided to the `skip_usrs` mark."""
    # based on
    # https://stackoverflow.com/questions/28179026/how-to-skip-a-pytest-using-an-external-fixture
    if request.node.get_closest_marker("skip_usrs"):
        user = request.getfixturevalue("user")
        if user in request.node.get_closest_marker("skip_usrs").args[0]:
            pytest.xfail(f"Should fail for {user}")


@pytest.fixture(scope="session")
def env():
    """The shell environment in which the tests are run.

    The list of variables needed to run the full test suite succesfully follows
    below with their default values, although not all tests have defaults and
    not all tests need all of the variables to run succesfully.

        FLOWDB_VERSION
        FLOWDB_RELEASE_DATE
        FLOWDB_INGESTION_DIR=flowdb/tests/data
        FLOWDB_DATA_DIR
        FLOWDB_PORT
        ORACLE_DB_PORT
        SYNTHETIC_DATA_DB_PORT
        POSTGRES_USER=flowdb
        POSTGRES_PASSWORD=flowflow
        FLOWMACHINE_FLOWDB_USER=flowmachine
        FLOWAPI_FLOWDB_USER=flowapi
        FLOWMACHINE_FLOWDB_PASSWORD=foo
        FLOWAPI_FLOWDB_PASSWORD=foo
        POSTGRES_GID
        POSTGRES_UID

    Returns
    -------
    dict
        A dictionary with the defined environment variables.
    """
    env = {
        "FLOWDB_VERSION": None,
        "FLOWDB_RELEASE_DATE": None,
        "FLOWDB_INGESTION_DIR": os.path.abspath(
            os.path.join(os.path.dirname(__file__), "data")
        ),
        "FLOWDB_DATA_DIR": None,
        "FLOWDB_PORT": None,
        "ORACLE_DB_PORT": None,
        "SYNTHETIC_DATA_DB_PORT": None,
        "POSTGRES_USER": "flowdb",
        "POSTGRES_PASSWORD": "flowflow",
        "FLOWMACHINE_FLOWDB_USER": "flowmachine",
        "FLOWAPI_FLOWDB_USER": "flowapi",
        "FLOWMACHINE_FLOWDB_PASSWORD": "foo",
        "FLOWAPI_FLOWDB_PASSWORD": "foo",
        "POSTGRES_GID": None,
        "POSTGRES_UID": None,
    }
    env = {k: os.getenv(k, v) for k, v in env.items()}
    return env


@pytest.fixture()
def db_conn(request, env):
    """
    A connection instance. The user can specify db params (usr, pwd, port, db)
    as fixtures at the module level, and the appropriate connection will be
    returned. Once the test is finished, the connection is closed.

    Yields
    ------
    psycopg2.extensions.connection
        A connection to the database consistent with the request fixtures.
    """
    db_conn = DBConn(request, env)
    yield db_conn.get()
    db_conn.close()


@pytest.fixture()
def cursor(db_conn):
    """
    A psycopg2.extras.RealDictCursor instance. The user can specify db params
    (usr, pwd, port, db) as fixtures at the module level, and the appropriate
    cursor will be returned. Once the test is finished, the cursor is closed
    and all changes are rolled back.

    Yields
    ------
    psycopg2.extras.RealDictCursor
        A cursor to the database consistent with the request fixtures.
    """
    try:
        with db_conn:
            with db_conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cursor:
                yield cursor
                raise ConnectionError  # Trigger a rollback
    except ConnectionError:
        pass


@pytest.fixture
def create_test_tables(request, env, test_tables):
    """
    This fixture handles test table creations. The user is required to specify
    a `test_tables` fixture which returns a dictionary whose key is the name of
    the table created and the value is the SQL query for its creation. The test
    table will be destroyed at the end of the module's lifecycle.
    """
    # create a connection to exclusively manage test tables, in order to avoid
    # conflicts with the db_conn fixture
    db_params = OrderedDict(
        [
            ("usr", env["POSTGRES_USER"]),
            ("pwd", env["POSTGRES_PASSWORD"]),
            ("port", env["FLOWDB_PORT"]),
            ("db", "flowdb"),
        ]
    )
    the_conn = pg.connect(
        f"postgresql://{db_params['usr']}:{db_params['pwd']}@localhost:{db_params['port']}/{db_params['db']}"
    )
    the_cursor = the_conn.cursor()

    # create the test tables
    tables = []
    for table, sql in test_tables.items():
        the_cursor.execute(sql)
        is_foreign = "FOREIGN" in sql
        tables.append((table, is_foreign))

    the_conn.commit()
    the_cursor.close()
    the_conn.close()

    yield

    # we need to re-open our connection in case all connections are closed in
    # in one of the tests
    the_conn = pg.connect(
        f"postgresql://{db_params['usr']}:{db_params['pwd']}@localhost:{db_params['port']}/{db_params['db']}"
    )
    the_cursor = the_conn.cursor()

    # clean up
    for table, is_foreign in tables:
        if is_foreign:
            the_cursor.execute("DROP FOREIGN TABLE IF EXISTS {}".format(table))
        else:
            the_cursor.execute("DROP TABLE IF EXISTS {}".format(table))

    the_conn.commit()
    the_cursor.close()
    the_conn.close()


@pytest.fixture()
def pg_available_extensions(cursor):
    """The list of `pg_available_extensions`.

    Returns
    -------
    list
        List of the `pg_available_extensions` names.
    """
    cursor.execute("SELECT * FROM pg_available_extensions")
    pg_available_extensions = []
    for i in cursor.fetchall():
        pg_available_extensions.append(i["name"])
    return pg_available_extensions


@pytest.fixture()
def shared_preload_libraries(cursor):
    cursor.execute("SHOW shared_preload_libraries;")
    shared_preload_libraries = cursor.fetchall()[0]["shared_preload_libraries"]
    shared_preload_libraries = shared_preload_libraries.split(",")
    return shared_preload_libraries
