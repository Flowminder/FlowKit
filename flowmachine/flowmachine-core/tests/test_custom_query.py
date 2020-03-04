from flowmachine_core.utility_queries.custom_query import CustomQuery


def test_custom_query_hash():
    """Test that CustomQuery hashing is same with case & space differences."""
    base = CustomQuery("SELECT * from foo", [])
    case_mismatch = CustomQuery("select * FROM foo", [])
    space_mismatch = CustomQuery(
        """select    *
                                 FROM foo""",
        [],
    )
    assert base.query_id == case_mismatch.query_id
    assert base.query_id == space_mismatch.query_id
