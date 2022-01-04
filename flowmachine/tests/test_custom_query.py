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
    assert base.query_id == case_mismatch.query_id
    assert base.query_id == space_mismatch.query_id


def test_custom_query_hash_with_dependents():
    base = CustomQuery("SELECT 1 as col from foo", ["col"])
    outer = CustomQuery("SELECT col FROM ({base}) as _", ["col"], {"base": base})
