import pytest
from flowmachine.core.server.query_proxy import QueryProxy, MissingQueryError


def test_query_proxy_from_missing_query_id():
    """
    Trying to construct a QueryProxy from a non-existent query id raises QueryProxyError.
    """
    with pytest.raises(MissingQueryError):
        QueryProxy.from_query_id("FOOBAR")
