import functools
import logging
import os
from pathlib import Path

import alembic
import git
from git.util import stream_copy
import pytest

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
def v1_17_0_models(project_tmpdir, monkeypatch):
    """
    Monkeypatches Flowauth's models.py with version 1.17.0
    """
    repo = git.Repo(Path(__file__).parent.parent.parent.parent)
    src_tree = repo.tag("1.17.0").commit.tree / "flowauth" / "backend" / "flowauth"
    unblob_tree(src_tree, project_tmpdir)
    jwt_tree = repo.tag("1.17.0").commit.tree / "flowkit_jwt_generator"
    unblob_tree(jwt_tree, project_tmpdir)
    # Hack to replace jwt symlink, as it gets unblobbed as a flat file.
    jwt_path = project_tmpdir / "flowauth" / "jwt.py"
    os.remove(jwt_path)
    os.symlink(project_tmpdir / "flowkit_jwt_generator" / "jwt.py", jwt_path)
    monkeypatch.syspath_prepend(project_tmpdir)
    import flowauth

    return flowauth


@pytest.fixture
def v1_17_0_app(v1_17_0_models, project_tmpdir):
    db_path = project_tmpdir / "db.db"
    print(f"DB path: {db_path}")
    app = v1_17_0_models.create_app(
        {
            "TESTING": True,
            "SQLALCHEMY_DATABASE_URI": f"sqlite:///{db_path}",
            "FLOWAUTH_ADMIN_USERNAME": "TEST_ADMIN",
            "FLOWAUTH_ADMIN_PASSWORD": "DUMMY_PASSWORD",
            "DEMO_MODE": False,
        }
    )
    with app.app_context():
        app.test_client().get("/")
    yield app


@pytest.fixture
def alembic_test_config(project_tmpdir):
    cfg = alembic.config.Config()
    cfg.set_main_option("script_location", "flowauth:migrations")
    # This should probably be fixturised later
    cfg.set_main_option("sqlalchemy.url", str(project_tmpdir / "db.db"))
    MigrationContext.configure(cfg)
    return cfg


def test_17_18_migration(v1_17_0_app, monkeypatch, test_servers):
    monkeypatch.syspath_prepend(Path(__file__).parent.parent / "versions")
    from flowauth.models import db

    with v1_17_0_app.app_context():
        assert "group_memberships" in db.metadata.tables.keys()
        command.upgrade()
        assert "roles" in db.metadata.tables.keys()
