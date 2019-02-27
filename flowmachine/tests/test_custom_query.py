from flowmachine.core import CustomQuery


def test_custom_query_hash():
    """Test that CustomQuery hashing is same with case & space differences."""
    base = CustomQuery("SELECT * from foo", [])
    case_mismatch = CustomQuery("select * FROM foo", [])
    space_mismatch = CustomQuery(
        """select    *
                                 FROM foo""",
        [],
    )
    assert base.md5 == case_mismatch.md5
    assert base.md5 == space_mismatch.md5
