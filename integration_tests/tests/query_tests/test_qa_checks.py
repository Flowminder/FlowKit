# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

import flowclient


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connection, module",
    [
        (flowclient.Connection, flowclient),
        (flowclient.ASyncConnection, flowclient.async_client),
    ],
)
async def test_get_available_qa_checks(
    connection, module, access_token_builder, flowapi_url
):
    """
    Test that we can get a per event type listing of qa checks.
    """
    con = connection(
        url=flowapi_url,
        token=access_token_builder({"date_role": ["get_qa_checks"]}),
    )
    try:
        result = await module.get_available_qa_checks_df(
            connection=con, event_types=None
        )
    except TypeError:
        result = module.get_available_qa_checks_df(connection=con, event_types=None)
    assert len(result) - len(result.drop_duplicates()) == 0
    assert result.cdr_type.unique().tolist() == [
        "calls",
        "mds",
        "sms",
    ]
    assert len(result) > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connection, module",
    [
        (flowclient.Connection, flowclient),
        (flowclient.ASyncConnection, flowclient.async_client),
    ],
)
async def test_get_available_qa_checks(
    connection, module, access_token_builder, flowapi_url
):
    """
    Test that we can get a per event type listing of qa checks.
    """
    con = connection(
        url=flowapi_url,
        token=access_token_builder({"date_role": ["get_qa_checks"]}),
    )
    try:
        result = await module.get_available_qa_checks_df(
            connection=con, event_types=["sms", "forwards"]
        )
    except TypeError:
        result = module.get_available_qa_checks_df(
            connection=con, event_types=["sms", "forwards"]
        )
    assert len(result) - len(result.drop_duplicates()) == 0
    assert result.cdr_type.unique().tolist() == ["sms"]  # No checks for forwards
    assert len(result) > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connection, module",
    [
        (flowclient.Connection, flowclient),
        (flowclient.ASyncConnection, flowclient.async_client),
    ],
)
async def test_get_qa_checks(connection, module, access_token_builder, flowapi_url):
    """
    Test that we can get checks over a date range.
    """
    con = connection(
        url=flowapi_url,
        token=access_token_builder({"date_role": ["get_qa_checks"]}),
    )
    try:
        result = await module.get_qa_check_outcomes_df(
            connection=con,
            event_type="calls",
            start_date="2016-01-01",
            end_date="2016-01-02",
            check_type="count_null_imeis",
        )
    except TypeError:
        result = module.get_qa_check_outcomes_df(
            connection=con,
            event_type="calls",
            start_date="2016-01-01",
            end_date="2016-01-02",
            check_type="count_null_imeis",
        )
    assert len(result) == 2
    assert all(result.type_of_query_or_check == "count_null_imeis")
    assert result.cdr_date.tolist() == ["2016-01-01", "2016-01-02"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connection, module",
    [
        (flowclient.Connection, flowclient),
        (flowclient.ASyncConnection, flowclient.async_client),
    ],
)
async def test_get_missing_qa_checks_is_404(
    connection, module, access_token_builder, flowapi_url
):
    """
    Test that we get a 404 through the date range endpoint if the check isn't there
    """
    con = connection(
        url=flowapi_url,
        token=access_token_builder({"date_role": ["get_qa_checks"]}),
    )
    with pytest.raises(FileNotFoundError):
        try:
            result = await module.get_qa_check_outcomes_df(
                connection=con,
                event_type="calls",
                start_date="2016-01-01",
                end_date="2016-01-02",
                check_type="NOT A CHECK",
            )
        except TypeError:
            result = module.get_qa_check_outcomes_df(
                connection=con,
                event_type="calls",
                start_date="2016-01-01",
                end_date="2016-01-02",
                check_type="NOT A CHECK",
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connection, module",
    [
        (flowclient.Connection, flowclient),
        (flowclient.ASyncConnection, flowclient.async_client),
    ],
)
async def test_get_one_qa_check(connection, module, access_token_builder, flowapi_url):
    """
    Test that we can get a single check.
    """
    con = connection(
        url=flowapi_url,
        token=access_token_builder({"date_role": ["get_qa_checks"]}),
    )
    try:
        result = await module.get_qa_check_outcome(
            connection=con,
            event_type="calls",
            event_date="2016-01-01",
            check_type="count_null_imeis",
        )
    except TypeError:
        result = module.get_qa_check_outcome(
            connection=con,
            event_type="calls",
            event_date="2016-01-01",
            check_type="count_null_imeis",
        )
    assert "1254" == result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connection, module",
    [
        (flowclient.Connection, flowclient),
        (flowclient.ASyncConnection, flowclient.async_client),
    ],
)
async def test_missing_qa_check_is_404(
    connection, module, access_token_builder, flowapi_url
):
    """
    Test that we get a 404 for a check which isn't there.
    """
    con = connection(
        url=flowapi_url,
        token=access_token_builder({"date_role": ["get_qa_checks"]}),
    )
    with pytest.raises(FileNotFoundError):
        try:
            result = await module.get_qa_check_outcome(
                connection=con,
                event_type="calls",
                event_date="2016-01-01",
                check_type="NOT_A_CHECK",
            )
        except TypeError:
            result = module.get_qa_check_outcome(
                connection=con,
                event_type="calls",
                event_date="2016-01-01",
                check_type="NOT_A_CHECK",
            )
