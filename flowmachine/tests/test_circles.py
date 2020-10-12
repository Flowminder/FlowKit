# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pytest

from flowmachine.features import CircleGeometries, Circle

lons = [85.3240, 83.9956]
lats = [27.7172, 28.2380]
names = ["Kathmandu", "Pokhara"]
radii = [4000, 11000]
geojson = {
    "features": [
        {
            "geometry": {
                "coordinates": [
                    [
                        [85.364_560_077, 27.717_685_285],
                        [85.363_885_83, 27.710_634_111],
                        [85.361_679_032, 27.703_835_467],
                        [85.358_024_745, 27.697_550_564],
                        [85.353_063_586, 27.692_020_826],
                        [85.346_986_288, 27.687_458_637],
                        [85.340_026_362, 27.684_039_187],
                        [85.332_451_129, 27.681_893_768],
                        [85.324_551_468, 27.681_104_742],
                        [85.316_630_673, 27.681_702_388],
                        [85.308_992_84, 27.683_663_749],
                        [85.301_931_23, 27.686_913_509],
                        [85.295_717_032, 27.691_326_879],
                        [85.290_588_974, 27.696_734_38],
                        [85.286_744_16, 27.702_928_333],
                        [85.284_330_489, 27.709_670_828],
                        [85.283_440_949, 27.716_702_838],
                        [85.284_110_011, 27.723_754_17],
                        [85.286_312_258, 27.730_553_837],
                        [85.289_963_32, 27.736_840_473],
                        [85.294_923_071, 27.742_372_388],
                        [85.301_000_993, 27.746_936_869],
                        [85.307_963_478, 27.750_358_375],
                        [85.315_542_817, 27.752_505_303],
                        [85.323_447_507, 27.753_295_06],
                        [85.331_373_486, 27.752_697_255],
                        [85.339_015_87, 27.750_734_871],
                        [85.346_080_705, 27.747_483_379],
                        [85.352_296_311, 27.743_067_832],
                        [85.357_423_745, 27.737_658_04],
                        [85.361_265_999, 27.731_462_029],
                        [85.363_675_565, 27.724_718_027],
                        [85.364_560_077, 27.717_685_285],
                    ]
                ],
                "type": "Polygon",
            },
            "id": 1,
            "properties": {
                "name": "Kathmandu",
                "centroid": {
                    "coordinates": [85.324_000_327, 27.717_201_362],
                    "type": "Point",
                },
            },
            "type": "Feature",
        },
        {
            "geometry": {
                "coordinates": [
                    [
                        [84.107_563_07, 28.235_499_064],
                        [84.104_851_533, 28.216_204_653],
                        [84.097_944_528, 28.197_749_061],
                        [84.087_109_209, 28.180_840_781],
                        [84.072_762_968, 28.166_128_632],
                        [84.055_457_253, 28.154_176_978],
                        [84.035_856_353, 28.145_444_188],
                        [84.014_711_99, 28.140_265_162],
                        [83.992_834_661, 28.138_838_566],
                        [83.971_062_805, 28.141_219_253],
                        [83.950_230_929, 28.147_316_145],
                        [83.931_137_865, 28.156_895_677],
                        [83.914_516_336, 28.169_590_66],
                        [83.901_004_947, 28.184_914_246],
                        [83.891_123_674, 28.202_278_49],
                        [83.885_253_766, 28.221_016_8],
                        [83.883_622_869, 28.240_409_443],
                        [83.886_295_966, 28.259_711_155],
                        [83.893_172_522, 28.278_179_772],
                        [83.903_990_022, 28.295_104_819],
                        [83.918_333_784, 28.309_834_923],
                        [83.935_652_739, 28.321_802_988],
                        [83.955_280_582, 28.330_548_146],
                        [83.976_461_489, 28.335_733_614],
                        [83.998_379_4, 28.337_159_745],
                        [84.020_189_697, 28.334_771_758],
                        [84.041_052_021, 28.328_661_84],
                        [84.060_162_905, 28.319_065_541],
                        [84.076_786_912, 28.306_352_603],
                        [84.090_285_061, 28.291_012_605],
                        [84.100_139_391, 28.273_635_993],
                        [84.105_972_755, 28.254_891_242],
                        [84.107_563_07, 28.235_499_064],
                    ]
                ],
                "type": "Polygon",
            },
            "id": 2,
            "properties": {
                "name": "Pokhara",
                "centroid": {
                    "coordinates": [83.995_595_590, 28.238_010_587],
                    "type": "Point",
                },
            },
            "type": "Feature",
        },
    ],
    "properties": {"crs": "+proj=longlat +datum=WGS84 +no_defs"},
    "type": "FeatureCollection",
}


def test_circle_column_names():
    """Test that CircleGeometries has correct column_names property"""
    cl = Circle(2, 3, 4, "bob")
    c = CircleGeometries([cl])
    assert c.head(0).columns.tolist() == c.column_names


def test_circle_string_rep():
    """Test that Circle objects have correct string representation."""
    lon, lat, radius, name = 2, 3, 4, "bob"
    cl = Circle(lon, lat, radius, name)
    assert f"Circle(lon={lon},lat={lat},radius={radius},name={name})" == str(cl)


def test_circle_loc_creation():
    """
    Test that point attribute gets made correctly. Not sure whether I need to test this
    class since it basicly just wraps up saome data...?
    """
    cl = Circle(lon=lons[0], lat=lats[0], radius=radii[0], name=names[0])
    assert cl.point_sql == "ST_GeomFromText('POINT (85.324 27.7172)',4326)::geography"


def test_circle_geometries():
    """
    Test that correct geometry objects are returned and GeoDataMixin is working correctly
    """
    c_locs = [Circle(*vals) for vals in zip(lons, lats, radii, names)]
    geoms = CircleGeometries(c_locs)
    result_geojson = geoms.to_geojson()
    assert result_geojson == geojson


def test_rastersum(get_dataframe):
    """
    Test that CircleRasterSum returns correct data
    """
    c_locs = [Circle(84.038, 28.309, 500, "test")]
    geoms = CircleGeometries(c_locs)
    crs = geoms.raster_sum("population.small_nepal_raster")
    crs = get_dataframe(crs).set_index("name")

    assert crs.loc["test"]["statistic"] == 4610.0
