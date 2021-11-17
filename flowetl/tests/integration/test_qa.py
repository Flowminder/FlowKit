# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest


@pytest.mark.parametrize("cdr_type", ["calls", "sms", "mds", "topups"])
def test_added_rows(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, location_id) VALUES 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"B" * 64}'), 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template("count_added_rows.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )
    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 2


@pytest.mark.parametrize("cdr_type", ["calls", "sms", "mds", "topups"])
def test_count_duped(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, location_id) VALUES 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"B" * 64}'), 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"B" * 64}')"""
    check_sql = jinja_env.get_template("count_duplicated.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )

    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 1


@pytest.mark.parametrize("cdr_type", ["calls", "sms", "mds", "topups"])
def test_count_dupes(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, location_id) VALUES 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"B" * 64}'), 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"B" * 64}'),
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"B" * 64}')
            """
    check_sql = jinja_env.get_template("count_duplicates.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )

    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 2


@pytest.mark.parametrize("cdr_type", ["calls", "sms", "mds", "topups"])
def test_count_imeis(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, imei, location_id) VALUES 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"C" * 64}', '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"D" * 64}', '{"C" * 64}', '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template("count_imeis.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )

    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 1


@pytest.mark.parametrize("cdr_type", ["calls", "sms", "mds", "topups"])
def test_count_null_imeis(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, imei, location_id) VALUES 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"C" * 64}', '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', NULL, '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', NULL, '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template("count_null_imeis.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )

    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 2


@pytest.mark.parametrize("cdr_type", ["calls", "sms", "mds", "topups"])
def test_count_imsis(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, imsi, location_id) VALUES 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"C" * 64}', '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"D" * 64}', '{"C" * 64}', '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template("count_imsis.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )

    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 1


@pytest.mark.parametrize("cdr_type", ["calls", "sms", "mds", "topups"])
def test_count_null_imsis(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, imsi, location_id) VALUES 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"C" * 64}', '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', NULL, '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', NULL, '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template("count_null_imsis.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )

    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 2


@pytest.mark.parametrize("cdr_type", ["calls", "sms", "mds", "topups"])
def test_count_location_ids(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, location_id) VALUES 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template("count_location_ids.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )

    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 1


@pytest.mark.parametrize("cdr_type", ["calls", "sms", "mds", "topups"])
def test_count_null_location_ids(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, location_id) VALUES 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', NULL), 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', NULL)"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template("count_null_location_ids.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )

    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 2


@pytest.mark.parametrize("cdr_type", ["calls", "sms", "mds", "topups"])
def test_count_locatable_location_ids(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, location_id) VALUES 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"C" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"D" * 64}')"""
    cells_sql = f"""
    INSERT INTO
        infrastructure.cells (id, version, date_of_first_service, date_of_last_service, geom_point)
    VALUES
        ('{"B" * 64}', 0, NULL, NULL, 'POINT(0 0)'),
        ('{"C" * 64}', 0, NULL, '2016-01-02'::date, NULL),
        ('{"C" * 64}', 1, '2016-01-02'::date, NULL, 'POINT(0 0)')
    """
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    flowdb_transaction.execute(cells_sql)
    check_sql = jinja_env.get_template("count_locatable_location_ids.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )

    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 1


@pytest.mark.parametrize("cdr_type", ["calls", "sms", "mds", "topups"])
def test_count_msisdns(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, location_id) VALUES 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"B" * 64}'), 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template("count_msisdns.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )
    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 1


@pytest.mark.parametrize("cdr_type", ["calls", "sms"])
def test_count_msisdns_includes_counterparts(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, msisdn_counterpart, location_id) VALUES 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"C" * 64}', '{"B" * 64}'), 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"C" * 64}', '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template("count_msisdns.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )
    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 2


@pytest.mark.parametrize("cdr_type", ["calls", "sms", "mds", "topups"])
def test_earliest_timestamp(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, location_id) VALUES 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"B" * 64}'), 
            ('2016-01-01 00:02:00'::timestamptz, '{"A" * 64}', '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template("earliest_timestamp.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )
    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert str(check_result) == "2016-01-01 00:01:00+00:00"


@pytest.mark.parametrize("cdr_type", ["calls", "sms", "mds", "topups"])
def test_latest_timestamp(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, location_id) VALUES 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"B" * 64}'), 
            ('2016-01-01 00:02:00'::timestamptz, '{"A" * 64}', '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template("latest_timestamp.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )
    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert str(check_result) == "2016-01-01 00:02:00+00:00"


@pytest.mark.parametrize("cdr_type", ["calls", "sms", "mds", "topups"])
def test_max_msisdns_per_imei(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, imei, location_id) VALUES 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"C" * 64}', '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"D" * 64}', '{"F" * 64}', '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"E" * 64}', '{"F" * 64}', '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template("max_msisdns_per_imei.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )

    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 2


@pytest.mark.parametrize("cdr_type", ["calls", "sms", "mds", "topups"])
def test_max_msisdns_per_imsi(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, imsi, location_id) VALUES 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"C" * 64}', '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"D" * 64}', '{"F" * 64}', '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"E" * 64}', '{"F" * 64}', '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template("max_msisdns_per_imsi.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )

    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 2


@pytest.mark.parametrize("cdr_type", ["calls", "sms"])
def test_count_added_rows_outgoing(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, outgoing, msisdn, location_id) VALUES 
        ('2016-01-01 00:01:00'::timestamptz, TRUE, '{"A" * 64}', '{"B" * 64}'),
        ('2016-01-01 00:01:00'::timestamptz, TRUE, '{"A" * 64}', '{"B" * 64}'),
        ('2016-01-01 00:01:00'::timestamptz, FALSE, '{"A" * 64}', '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template(
        f"{cdr_type}/count_added_rows_outgoing.sql"
    ).render(cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101")

    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 2


@pytest.mark.parametrize("cdr_type", ["calls", "sms"])
def test_count_null_counterparts(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, msisdn_counterpart, location_id) VALUES 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"C" * 64}', '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', NULL, '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', NULL, '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template(
        f"{cdr_type}/count_null_counterparts.sql"
    ).render(cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101")

    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 2


@pytest.mark.parametrize("cdr_type", ["calls", "sms"])
def test_count_onnet_msisdns(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, msisdn_counterpart, location_id) VALUES 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"C" * 64}', '{"B" * 64}'), 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', '{"C" * 64}', '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template(f"{cdr_type}/count_onnet_msisdns.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )
    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 1


@pytest.mark.parametrize("cdr_type", ["calls", "sms"])
def test_count_onnet_msisdns_outgoing(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""
    INSERT INTO events.{cdr_type}_20160101(datetime, outgoing, msisdn, msisdn_counterpart, location_id)
    VALUES 
        ('2016-01-01 00:01:00'::timestamptz, TRUE, '{"A" * 64}', '{"C" * 64}', '{"B" * 64}'),
        ('2016-01-01 00:01:00'::timestamptz, TRUE, '{"D" * 64}', '{"C" * 64}', '{"B" * 64}'),
        ('2016-01-01 00:01:00'::timestamptz, FALSE, '{"D" * 64}', '{"C" * 64}', '{"B" * 64}'),
        ('2016-01-01 00:01:00'::timestamptz, FALSE, '{"E" * 64}', '{"C" * 64}', '{"B" * 64}'),
        ('2016-01-01 00:01:00'::timestamptz, FALSE, '{"F" * 64}', '{"C" * 64}', '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template(
        f"{cdr_type}/count_onnet_msisdns_outgoing.sql"
    ).render(cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101")
    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 2


@pytest.mark.parametrize("cdr_type", ["calls", "sms"])
def test_count_onnet_msisdns_incoming(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""
    INSERT INTO events.{cdr_type}_20160101(datetime, outgoing, msisdn, msisdn_counterpart, location_id)
    VALUES 
        ('2016-01-01 00:01:00'::timestamptz, TRUE, '{"A" * 64}', '{"C" * 64}', '{"B" * 64}'),
        ('2016-01-01 00:01:00'::timestamptz, TRUE, '{"D" * 64}', '{"C" * 64}', '{"B" * 64}'),
        ('2016-01-01 00:01:00'::timestamptz, FALSE, '{"D" * 64}', '{"C" * 64}', '{"B" * 64}'),
        ('2016-01-01 00:01:00'::timestamptz, FALSE, '{"E" * 64}', '{"C" * 64}', '{"B" * 64}'),
        ('2016-01-01 00:01:00'::timestamptz, FALSE, '{"F" * 64}', '{"C" * 64}', '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template(
        f"{cdr_type}/count_onnet_msisdns_outgoing.sql"
    ).render(cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101")
    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 3


@pytest.mark.parametrize("cdr_type", ["calls"])
def test_max_duration(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, duration, location_id) VALUES 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', 10 '{"B" * 64}'), 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', 20 '{"B" * 64}'), 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', 50 '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template(f"{cdr_type}/max_duration.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )
    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 50


@pytest.mark.parametrize("cdr_type", ["calls"])
def test_median_duration(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, duration, location_id) VALUES 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', 10 '{"B" * 64}'), 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', 20 '{"B" * 64}'), 
            ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', 50 '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template(f"{cdr_type}/median_duration.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )
    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 20


@pytest.mark.parametrize("cdr_type", ["calls"])
def test_count_null_durations(cdr_type, flowdb_transaction, jinja_env):
    create_sql = f"""CREATE TABLE IF NOT EXISTS events.{cdr_type}_20160101 (LIKE events.{cdr_type});"""
    insert_sql = f"""INSERT INTO events.{cdr_type}_20160101(datetime, msisdn, duration, location_id) VALUES 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', 10, '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', NULL, '{"B" * 64}'), 
        ('2016-01-01 00:01:00'::timestamptz, '{"A" * 64}', NULL, '{"B" * 64}')"""
    flowdb_transaction.execute(create_sql)
    flowdb_transaction.execute(insert_sql)
    check_sql = jinja_env.get_template(f"{cdr_type}/count_null_durations.sql").render(
        cdr_type=cdr_type, final_table=f"events.{cdr_type}_20160101"
    )

    check_result, *_ = list(flowdb_transaction.execute(check_sql))[0]

    assert check_result == 2
