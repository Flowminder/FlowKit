import pytest
from asynctest import return_once
from tests.unit.zmq_helpers import ZMQReply


@pytest.mark.asyncio
async def test_get_qa_checks(app, access_token_builder, dummy_zmq_server):
    token = access_token_builder({"test_role": ["get_qa_checks"]})
    dummy_zmq_server.side_effect = return_once(
        ZMQReply(
            status="success",
            payload={
                "qa_checks": [
                    dict(
                        outcome="0",
                        type_of_query_or_check="dummy_query",
                        cdr_date="2016-01-01",
                    )
                ]
            },
        )
    )
    response = await app.client.get(
        "/api/0/qa/dummy_type/dummy_query/2016-01-01",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    value = await response.data
    assert value == b"0"


@pytest.mark.asyncio
async def test_qa_test_bad_date(app, access_token_builder, dummy_zmq_server):
    token = access_token_builder({"test_role": ["get_qa_checks"]})
    response = await app.client.get(
        "/api/0/qa/dummy_type/dummy_query/notadate",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_get_qa_checks_date_range(app, access_token_builder, dummy_zmq_server):
    token = access_token_builder({"test_role": ["get_qa_checks"]})
    dummy_zmq_server.side_effect = return_once(
        ZMQReply(
            status="success",
            payload={
                "qa_checks": [
                    dict(
                        outcome="0",
                        type_of_query_or_check="dummy_query",
                        cdr_date="2016-01-01",
                    )
                ]
            },
        )
    )
    response = await app.client.get(
        "/api/0/qa/dummy_type/dummy_query?start_date=2016-01-01&end_date=2016-01-02",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
