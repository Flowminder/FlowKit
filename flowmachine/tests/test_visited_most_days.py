# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.visited_most_days import VisitedMostDays
from flowmachine.core import make_spatial_unit
import pytest

import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


def test_visited_most_days_column_names(get_dataframe):
    """Test that column_names property is accurate"""
    vmd = VisitedMostDays(
        start_date="2016-01-01",
        end_date="2016-01-02",
        spatial_unit=make_spatial_unit("admin", level=1),
    )
    assert get_dataframe(vmd).columns.tolist() == vmd.column_names


def test_visited_most_days_sql_correct():
    """Test that the correct SQL is generated"""
    query = VisitedMostDays(
        start_date="2016-01-01",
        end_date="2016-01-07",
        spatial_unit=make_spatial_unit("admin", level=1),
    )
    assert (
        query._make_sql("test")[0]
        == """EXPLAIN (ANALYZE TRUE, TIMING FALSE, FORMAT JSON) CREATE TABLE test AS 
        (SELECT subscriber, pcod FROM (
        SELECT 
            ranked.subscriber, 
            pcod
        FROM
            (
                SELECT
                    agg.subscriber, pcod,
                    row_number() OVER (
                        PARTITION BY agg.subscriber
                        ORDER BY
                            agg.num_days_visited DESC,
                            agg.num_events DESC,
                            RANDOM()
                    ) AS rank
                FROM (
                    SELECT
                        subscriber, pcod,
                        COUNT(date_visited) AS num_days_visited,
                        SUM(total) AS num_events
                    FROM (
        SELECT 
            subscriber_locs.subscriber, 
            pcod, 
            time::date AS date_visited, 
            count(*) AS total
        FROM (
                SELECT
                    subscriber, datetime as time, pcod
                FROM
                    (
        SELECT
            l.datetime, l.location_id, l.subscriber,
            sites.pcod
        FROM
            (SELECT events.calls.datetime, events.calls.location_id, events.calls.msisdn AS subscriber 
FROM events.calls 
WHERE events.calls.datetime >= '2016-01-01 00:00:00' AND events.calls.datetime < '2016-01-07 00:00:00'
UNION ALL
SELECT events.sms.datetime, events.sms.location_id, events.sms.msisdn AS subscriber 
FROM events.sms 
WHERE events.sms.datetime >= '2016-01-01 00:00:00' AND events.sms.datetime < '2016-01-07 00:00:00'
UNION ALL
SELECT events.mds.datetime, events.mds.location_id, events.mds.msisdn AS subscriber 
FROM events.mds 
WHERE events.mds.datetime >= '2016-01-01 00:00:00' AND events.mds.datetime < '2016-01-07 00:00:00') AS l
        INNER JOIN
            (
        SELECT
            loc_table.id AS location_id,
            loc_table.date_of_first_service,
            loc_table.date_of_last_service
            ,
            geom_table.admin1pcod AS pcod
        FROM infrastructure.cells AS loc_table
        
            INNER JOIN
                (SELECT gid,admin1pcod,admin1name,parent_pcod,geom FROM geography.admin1) AS geom_table
            ON ST_within(
                loc_table.geom_point::geometry,
                ST_SetSRID(geom_table.geom, 4326)::geometry
            )
            
        ) AS sites
        ON
            l.location_id = sites.location_id
          AND
            l.datetime::date BETWEEN coalesce(sites.date_of_first_service,
                                                '-infinity'::timestamptz) AND
                                       coalesce(sites.date_of_last_service,
                                                'infinity'::timestamptz)
        ) AS foo
                WHERE location_id IS NOT NULL AND location_id !=''
                 ORDER BY time) AS subscriber_locs
        GROUP BY subscriber_locs.subscriber, pcod, time::date
        ) AS times_visited
                    GROUP BY subscriber, pcod
                ) AS agg
            ) AS ranked
        WHERE rank = 1
        ) _)"""
    )


def test_vsites(get_dataframe):
    """
    VisitedMostDays() returns the correct locations.
    """

    vmd = VisitedMostDays(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("versioned-site")
    )
    df = get_dataframe(vmd)
    df.set_index("subscriber", inplace=True)

    # assert "wzrXjw" == df.loc["yqQ6m96rp09dn510"].site_id
    # assert "qvkp6J" == df.loc["zvaOknzKbEVD2eME"].site_id
    assert (
        "dc0330125498e0386e15cb92a1036dd5"
        == df.loc["0000ee86786791f4d4895f6766562c1f"].site_id
    )
    assert (
        "39abc0df75a3181c56f83677fa1b3066"
        == df.loc["00034938ed8cfe60490b28a3ee16a8a6"].site_id
    )


def test_lon_lats(get_dataframe):
    """
    VisitedMostDays() has the correct values at the lon-lat spatial unit.
    """

    vmd = VisitedMostDays(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("lon-lat")
    )
    df = get_dataframe(vmd)
    df.set_index("subscriber", inplace=True)

    # assert pytest.approx(82.61895799084449) == float(df.loc["1QBlwRo4Kd5v3Ogz"].lon)
    # assert pytest.approx(28.941925079951545) == float(df.loc["1QBlwRo4Kd5v3Ogz"].lat)
    assert pytest.approx(85.72864450240787) == float(
        df.loc["0000ee86786791f4d4895f6766562c1f"].lon
    )
    assert pytest.approx(26.86887013312139) == float(
        df.loc["0000ee86786791f4d4895f6766562c1f"].lat
    )


def test_most_frequent_admin(get_dataframe):
    """
    Test that the most frequent admin3 is correctly calculated.
    """
    vmd = VisitedMostDays(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("admin", level=3)
    )
    df = get_dataframe(vmd)
    df.set_index("subscriber", inplace=True)

    # A few hand picked values
    df_set = df.set_index("subscriber")["pcod"]
    print(df_set)
    # assert "524 4 12 62" == df_set["0gmvwzMAYbz5We1E"]
    # assert "524 4 10 52" == df_set["1QBlwRo4Kd5v3Ogz"]
    # assert "524 3 09 50" == df_set["2Dq97XmPqvL6noGk"]
    assert "NPL.4.1.3_1" == df_set["0000ee86786791f4d4895f6766562c1f"]
    assert "NPL.4.1.3_1" == df_set["fffeeb8ee5f7f8a265fe16055d050619"]
    assert "NPL.2.3.4_1" == df_set["ffff32a2c03466a03b8d489d217ef371"]
