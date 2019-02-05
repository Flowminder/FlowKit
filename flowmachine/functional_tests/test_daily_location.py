# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.features import daily_location

daily_loc_sql_expected = """
        SELECT final_time.subscriber, pcod
        FROM
             (SELECT subscriber_locs.subscriber, time, pcod,
             row_number() OVER (PARTITION BY subscriber_locs.subscriber ORDER BY time DESC)
                 AS rank
             FROM (
        SELECT
            l.subscriber, l.time, l.location_id,
            sites.pcod
        FROM
            (
                SELECT
                    subscriber, datetime as time, location_id
                FROM
                    (
        SELECT datetime, location_id, msisdn AS subscriber
        FROM events.calls
        WHERE (datetime >= '2016-01-01'::timestamptz) AND (datetime <= '2016-01-02'::timestamptz)
        
UNION ALL

        SELECT datetime, location_id, msisdn AS subscriber
        FROM events.sms
        WHERE (datetime >= '2016-01-01'::timestamptz) AND (datetime <= '2016-01-02'::timestamptz)
        ) AS foo
                WHERE location_id IS NOT NULL AND location_id !=''
                ) AS l
        INNER JOIN
            (
        SELECT
            location_id, version, date_of_first_service, date_of_last_service,
            admin3pcod AS pcod
        FROM
            (
            SELECT
                locinfo.id AS location_id,
                locinfo.version,
                locinfo.date_of_first_service,
                locinfo.date_of_last_service,
                polygon.admin3pcod
            FROM
                infrastructure.cells AS locinfo
            INNER JOIN
                geography.admin3 AS polygon
            ON ST_within(
                locinfo.geom_point::geometry,
                ST_SetSRID(polygon.geom, 4326)::geometry
            )
            ) AS map
        ) AS sites
        ON
            l.location_id = sites.location_id
          AND
            l.time::date BETWEEN coalesce(sites.date_of_first_service,
                                                '-infinity'::timestamptz) AND
                                       coalesce(sites.date_of_last_service,
                                                'infinity'::timestamptz)
        ) AS subscriber_locs) AS final_time
        WHERE rank = 1
    """

def test_sql_daily_location(get_dataframe):
    """
    daily_location() is equivalent to the MostFrequentLocation().
    """
    dl = daily_location("2016-01-01", "2016-01-02")
    assert daily_loc_sql_expected.strip() == dl.get_query().strip()
