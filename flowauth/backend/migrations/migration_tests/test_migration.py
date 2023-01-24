import functools
import logging
import os
import sys
from pathlib import Path

import flask_migrate
import git
from git.util import stream_copy
import pytest
import sqlite3

# from alembic import


@functools.singledispatch
def unblob_tree(tree, root: Path):
    yield from ()


@unblob_tree.register
def _(tree: git.Tree, root: Path):
    root.mkdir(tree.name)
    for object in tree.traverse():
        unblob_tree(object, root / tree.name)


@unblob_tree.register
def _(tree: git.Blob, root: Path):
    with open(root / tree.name, "wb") as fp:
        stream_copy(tree.data_stream, fp)


class MockCurrentApp:
    config = {"DB_IS_SET_UP": False}
    logger = logging.getLogger()


@pytest.fixture
def project_tmpdir(tmpdir_factory):
    fn = tmpdir_factory.mktemp("old_app")
    return fn


@pytest.fixture
def repo_root():
    return Path(__file__).parent.parent.parent.parent.parent


@pytest.fixture
def v1_17_0_models(project_tmpdir, monkeypatch, repo_root):
    """
    Monkeypatches Flowauth's models.py with version 1.17.0
    """
    repo = git.Repo(repo_root)
    src_tree = repo.tag("1.17.0").commit.tree / "flowauth" / "backend" / "flowauth"
    unblob_tree(src_tree, project_tmpdir)
    jwt_tree = repo.tag("1.17.0").commit.tree / "flowkit_jwt_generator"
    unblob_tree(jwt_tree, project_tmpdir)
    # Hack to replace jwt symlink, as it gets unblobbed as a flat file.
    jwt_path = project_tmpdir / "flowauth" / "jwt.py"
    os.remove(jwt_path)
    os.symlink(project_tmpdir / "flowkit_jwt_generator" / "jwt.py", jwt_path)


@pytest.fixture
def db_path(project_tmpdir):
    return project_tmpdir / "db.db"


class MockDbSetupWatcher:
    def wait(self):
        pass

    def is_set(self):
        return True


@pytest.fixture
def current_app_old_db(v1_17_0_models, db_path, project_tmpdir, repo_root):
    from importlib import invalidate_caches, import_module, reload

    sys.path = [str(project_tmpdir), *sys.path]
    invalidate_caches()

    import flowauth

    print(f"DB path: {db_path}")
    old_app = flowauth.create_app(
        {
            "TESTING": True,
            "SQLALCHEMY_DATABASE_URI": f"sqlite:///{db_path}",
            "FLOWAUTH_ADMIN_USERNAME": "TEST_ADMIN",
            "FLOWAUTH_ADMIN_PASSWORD": "DUMMY_PASSWORD",
            "DEMO_MODE": True,
        }
    )
    with old_app.app_context():
        old_app.test_client().get("/")
    del sys.path[0]
    del flowauth
    flowauth_module_keys = [n for n in sys.modules.keys() if n.startswith("flowauth")]
    for k in flowauth_module_keys:
        del sys.modules[k]

    invalidate_caches()
    import flowauth

    # assert flowauth_new.
    new_app = flowauth.create_app(
        {
            "TESTING": False,
            "SQLALCHEMY_DATABASE_URI": f"sqlite:///{db_path}",
            "FLOWAUTH_ADMIN_USERNAME": "TEST_ADMIN",
            "FLOWAUTH_ADMIN_PASSWORD": "DUMMY_PASSWORD",
            "DEMO_MODE": False,
            "DB_IS_SET_UP": MockDbSetupWatcher(),
        }
    )
    yield new_app


@pytest.fixture
def alembic_test_config(project_tmpdir):
    cfg = flask_migrate.Config()
    cfg.set_main_option("script_location", str(Path(__file__).parent.parent))

    # This should probably be fixturised later
    cfg.set_main_option("sqlalchemy.url", str(project_tmpdir / "db.db"))
    return cfg


def test_17_18_migration(current_app_old_db, monkeypatch, alembic_test_config, db_path):
    monkeypatch.syspath_prepend(Path(__file__).parent.parent / "versions")

    with current_app_old_db.app_context() as current_app, sqlite3.connect(
        db_path
    ) as conn:
        table_names = [
            row[0]
            for row in conn.cursor()
            .execute("SELECT name FROM sqlite_master WHERE type='table'")
            .fetchall()
        ]
        assert "group" in table_names
        assert "role" not in table_names
        flask_migrate.upgrade(
            str(Path(__file__).parent.parent), revision="66dee292d147"
        )

        table_names = [
            row[0]
            for row in conn.cursor()
            .execute("SELECT name FROM sqlite_master WHERE type='table'")
            .fetchall()
        ]
        assert "role" in table_names
        assert "group" not in table_names
        assert "group" not in table_names

