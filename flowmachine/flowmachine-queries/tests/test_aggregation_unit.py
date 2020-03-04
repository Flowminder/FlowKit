import pytest
from flowmachine_queries.query_schemas.aggregation_unit import get_spatial_unit_obj


# Note: most of the code in aggregation_unit.py is tested
# via integration tests. This file only tests the "bad path"
# in the helper function get_spatial_unit_obj() because it
# is not hit by any of the integration tests.


def test_get_spatial_unit_obj():
    expected_error_msg = "The helper function `get_spatial_unit_obj` does not support aggregation units of type 'invalid_spatial_unit'"
    with pytest.raises(NotImplementedError, match=expected_error_msg):
        get_spatial_unit_obj("invalid_spatial_unit")
