# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Test for the database roles.

The database has three different roles:

    * `flowdb`: database super user.
    * `analyst`: FlowMachine user with read access to raw tables.
    * `reporter`: has read access to public tables only and
                  reference tables.
"""

import pytest
import psycopg2 as pg


@pytest.fixture
def test_tables():
    """Define the test tables for testing the analyst and reporter roles.

    Returns
    -------
    dict
        Dictionary whose key is the test table name and the value is the query for creating it.
    """
    tables = {
        "events.calls_20160101": """
            CREATE TABLE IF NOT EXISTS
                events.calls_20160101 ()
                INHERITS (events.calls)
            """,
        "routing.foo": """
            CREATE TABLE IF NOT EXISTS
                routing.foo ()
            """,
        "cache.blah": """
            CREATE TABLE IF NOT EXISTS
                cache.blah();
            ALTER TABLE cache.blah OWNER TO analyst;
            """,
        "geography.admin0": """
            CREATE TABLE IF NOT EXISTS 
                geography.admin0()
            """,
    }
    return tables


@pytest.mark.usefixtures("create_test_tables")
@pytest.mark.parametrize("usr", ["analyst", "reporter"], scope="class")
class TestRole(object):
    @pytest.fixture(scope="class", autouse=True)
    def pwd(self, usr, env):
        """
        Returns the password for the given usr.

        Returns
        -------
        str
            The usr password.
        """
        return env["{}_PASSWORD".format(usr.upper())]

    def test_cannot_drop_events(self, cursor):
        """Role cannot DROP TABLE on events."""
        with pytest.raises(pg.ProgrammingError):
            cursor.execute("DROP TABLE events.calls")

    @pytest.mark.skip_usrs(["reporter"])
    def test_select_events(self, cursor):
        """Role can do SELECT on events.calls."""
        cursor.execute("SELECT * FROM events.calls")

        cursor.execute("SELECT * FROM routing.foo")

    @pytest.mark.skip_usrs(["reporter"])
    def test_select_new_events(self, cursor):
        """Role can read new tables created under the events schema."""
        cursor.execute("SELECT * FROM events.calls_20160101;")

    @pytest.mark.skip_usrs(["analyst"])
    def test_cannot_select_events(self, cursor):
        """Role cannot do SELECT on events.calls."""
        with pytest.raises(pg.ProgrammingError):
            cursor.execute("SELECT * FROM events.calls")

    def test_cannot_create_events(self, cursor):
        """Role cannot create tables in the events.* schema."""
        with pytest.raises(pg.ProgrammingError):
            cursor.execute(
                """
                CREATE TABLE events.calls_20160102 () INHERITS (events.calls)
            """
            )

    def test_create_public(self, cursor):
        """Role can CREATE TABLE in public."""
        cursor.execute("CREATE TABLE foo(id TEXT)")

    def test_drop_public(self, cursor):
        """Role can DROP TABLE in public."""
        cursor.execute("CREATE TABLE foo(id TEXT)")
        cursor.execute("DROP TABLE foo")

    @pytest.mark.skip_usrs(["analyst"])
    def test_cannot_drop_geo(self, cursor):
        """Reporter cannot drop geography tables."""
        with pytest.raises(pg.ProgrammingError):
            cursor.execute("DROP TABLE geography.admin0")

    @pytest.mark.skip_usrs(["analyst"])
    def test_cannot_drop_cache_tables(self, cursor):
        """Reporter cannot drop cache tables"""
        with pytest.raises(pg.ProgrammingError):
            cursor.execute("DROP TABLE cache.blah")

    def test_cannot_drop_cache_metadata_table(self, cursor):
        """Role cannot drop cache.cached"""
        with pytest.raises(pg.ProgrammingError):
            cursor.execute("DROP TABLE cache.cached")

    @pytest.mark.skip_usrs(["analyst"])
    def test_cannot_select_cache_metadata_table(self, cursor):
        """Reporter cannot read cache metadata"""
        with pytest.raises(pg.ProgrammingError):
            cursor.execute("SELECT * FROM cache.cached")

    def test_cannot_drop_cache_dependencies_table(self, cursor):
        """Role cannot drop cache metadata"""
        with pytest.raises(pg.ProgrammingError):
            cursor.execute("DROP TABLE cache.dependencies")

    @pytest.mark.skip_usrs(["reporter"])
    def test_can_drop_cache_tables(self, cursor):
        """Analyst can drop cache tables"""
        cursor.execute("DROP TABLE cache.blah")

    def test_select_cache(self, cursor):
        """Role can do SELECT on cache tables."""
        cursor.execute("SELECT * FROM cache.blah")

    def test_select_geo(self, cursor):
        """Everybody can SELECT from the geography tables."""
        cursor.execute("SELECT * FROM geography.admin0")

    @pytest.mark.skip_usrs(["reporter"])
    def test_select_cache_metadata_table(self, cursor):
        """Role can do SELECT on cache tables."""
        cursor.execute("SELECT * FROM cache.cached")

    @pytest.mark.skip_usrs(["reporter"])
    def test_select_cache_dependencies_table(self, cursor):
        """Analyst can do SELECT on cache dependencies table."""
        cursor.execute("SELECT * FROM cache.dependencies")
