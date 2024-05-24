import pytest

from flowmachine.core import GeoTable


def test_geotable_bad_params():
    """Test that geotable raises errors correctly."""
    with pytest.raises(ValueError):
        t = GeoTable("geography.admin3", geom_column="bad_column", columns=["geom"])
    with pytest.raises(ValueError):
        t = GeoTable(
            "geography.admin3", gid_column="bad_column", columns=["geom", "gid"]
        )


def test_geotable():
    """Test that geotable will work with an obviously geographic table."""
    t = GeoTable("geography.admin3", columns=["geom", "admin3pcod", "admin0name"])
    feature = t.to_geojson()["features"][0]
    assert feature["properties"]["admin0name"] == "Nepal"


def test_geotable_uses_row_number_without_gid():
    """Test that geotable infers a gid if one not present."""

    t = GeoTable("geography.admin3", columns=["geom", "admin3name"])
    feature = t.to_geojson()["features"][0]
    assert feature["id"] == 1


def test_geotable_uses_supplied_gid():
    """Test that geotable uses specified gid columns."""

    t = GeoTable(
        "geography.admin3", columns=["geom", "admin3name"], gid_column="admin3name"
    )
    feature = t.to_geojson()["features"][0]
    assert feature["id"] == "Sindhupalchok"


def test_geotable_column_names():
    """Test that column_names property matches head(0) for geotables"""
    t = GeoTable("geography.admin3", columns=["gid", "geom"])
    assert t.head(0).columns.tolist() == t.column_names
