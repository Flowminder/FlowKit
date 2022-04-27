# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from click.testing import CliRunner


def test_copies_to_dag_folder(tmpdir):
    runner = CliRunner()
    from flowetl.cli import main

    dags_folder = tmpdir / "dags"
    dags_folder.mkdir()
    result = runner.invoke(
        main,
        [str(dags_folder)],
    )
    assert (dags_folder / "flowetl").exists()
