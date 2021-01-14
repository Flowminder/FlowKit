# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import sys

import pytest


@pytest.fixture(autouse=True)
def airflow_home(tmpdir, monkeypatch):
    monkeypatch.setenv("AIRFLOW_HOME", str(tmpdir))
    yield tmpdir


@pytest.fixture(autouse=True)
def unload_airflow():
    try:
        yield
    finally:
        for module in list(sys.modules.keys()):
            if "airflow" in module or "flowetl" in module:
                del sys.modules[module]
