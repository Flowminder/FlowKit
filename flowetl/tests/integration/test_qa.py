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

    assert check_result == "2016-01-01 00:01:00"


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

    assert check_result == "2016-01-01 00:02:00"
