# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest.mock import Mock

import pytest

from flowclient import ASyncConnection


@pytest.mark.asyncio
async def test_get_url(monkeypatch, token):
    monkeypatch.setattr(
        "flowclient.connection.Connection.get_url", Mock(return_value="DUMMY_RETURN")
    )
    con = ASyncConnection(url="DUMMY_URL", token=token)
    assert await con.get_url(route="DUMMY_ROUTE", data="DUMMY_DATA")


@pytest.mark.asyncio
async def test_post_json(monkeypatch, token):
    monkeypatch.setattr(
        "flowclient.connection.Connection.post_json", Mock(return_value="DUMMY_RETURN")
    )
    con = ASyncConnection(url="DUMMY_URL", token=token)
    assert await con.post_json(route="DUMMY_ROUTE", data="DUMMY_DATA")


def test_make_query_object(monkeypatch, token):
    con = ASyncConnection(url="DUMMY_URL", token=token)
    dummy_params = dict(dummy_params="DUMMY_PARAMS")
    assert con.make_api_query(dummy_params)._connection == con
    assert con.make_api_query(dummy_params).parameters == dummy_params
