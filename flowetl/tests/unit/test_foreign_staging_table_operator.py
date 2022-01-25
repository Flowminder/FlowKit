# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest


@pytest.mark.parametrize("header, expected", [(True, "TRUE"), (False, "FALSE")])
def test_foreign_staging_table_header_param(header, expected):
    from flowetl.operators.create_foreign_staging_table_operator import (
        CreateForeignStagingTableOperator,
    )

    op = CreateForeignStagingTableOperator(
        task_id="DUMMY_ID", filename="DUMMY_FILE", fields=dict(), header=header
    )
    assert op.params["header"] == expected


@pytest.mark.parametrize("program, expected", [(None, False), ("foo", "foo")])
def test_foreign_staging_table_program_param(program, expected):
    from flowetl.operators.create_foreign_staging_table_operator import (
        CreateForeignStagingTableOperator,
    )

    op = CreateForeignStagingTableOperator(
        task_id="DUMMY_ID", filename="DUMMY_FILE", fields=dict(), program=program
    )
    out = op.params.get("program", False)
    if out:
        assert out.resolve() == expected
    else:
        assert out == expected


@pytest.mark.parametrize("encoding, expected", [(None, False), ("foo", "foo")])
def test_foreign_staging_table_encoding_param(encoding, expected):
    from flowetl.operators.create_foreign_staging_table_operator import (
        CreateForeignStagingTableOperator,
    )

    op = CreateForeignStagingTableOperator(
        task_id="DUMMY_ID", filename="DUMMY_FILE", fields=dict(), encoding=encoding
    )
    out = op.params.get("encoding", False)
    if out:
        assert out.resolve() == expected
    else:
        assert out == expected
