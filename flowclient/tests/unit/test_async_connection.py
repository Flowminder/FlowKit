# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import pytest
from httpx import Response

from flowclient import ASyncConnection


@pytest.mark.asyncio
async def test_get_url(session_mock, dummy_route, async_flowclient_connection):
    session_mock.get(dummy_route).respond(200, content="DUMMY_RETURN")
    assert (
        b"DUMMY_RETURN"
        == (
            await (await async_flowclient_connection).get_url(
                route="DUMMY_ROUTE", data="DUMMY_DATA"
            )
        ).content
    )


@pytest.mark.asyncio
async def test_post_json(session_mock, dummy_route, async_flowclient_connection):
    session_mock.post(dummy_route).respond(content="DUMMY_RETURN", status_code=202)
    assert (
        b"DUMMY_RETURN"
        == (
            await (await async_flowclient_connection).post_json(
                route="DUMMY_ROUTE", data="DUMMY_DATA"
            )
        ).content
    )


def test_make_query_object(token):
    con = ASyncConnection(url="DUMMY_URL", token=token)
    dummy_params = dict(dummy_params="DUMMY_PARAMS")
    assert con.make_api_query(dummy_params)._connection == con
    assert con.make_api_query(dummy_params).parameters == dummy_params
