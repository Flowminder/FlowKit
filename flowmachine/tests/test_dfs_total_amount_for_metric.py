# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import textwrap

from flowmachine.features.dfs.total_amount_for_metric import DFSTotalMetricAmount


def test_column_names():
    """
    Test that column_names property contains expected column names
    """
    q = DFSTotalMetricAmount(
        metric="amount",
        start_date="2016-01-01",
        end_date="2016-01-04",
        aggregation_unit="admin2",
    )
    assert q.column_names == ["date", "pcod", "value"]
    assert q.head(0).columns.tolist() == q.column_names

    q = DFSTotalMetricAmount(
        metric="fee",
        start_date="2016-01-02",
        end_date="2016-01-05",
        aggregation_unit="admin3",
    )
    assert q.column_names == ["date", "pcod", "value"]
    assert q.head(0).columns.tolist() == q.column_names


def test_dfs_total_transaction_amount(get_dataframe):
    """
    Total transaction amount returns expected result.
    """
    q = DFSTotalMetricAmount(
        metric="amount",
        start_date="2016-01-02",
        end_date="2016-01-04",
        aggregation_unit="admin1",
    )
    df = get_dataframe(q)
    result = df.to_csv(index=False).strip()
    result_expected = textwrap.dedent(
        """
        date,pcod,value
        2016-01-02,524 1,1576489.0
        2016-01-02,524 2,1622235.0
        2016-01-02,524 3,9657733.0
        2016-01-02,524 4,13381945.0
        2016-01-02,524 5,3651002.0
        2016-01-03,524 1,1512856.0
        2016-01-03,524 2,1598228.0
        2016-01-03,524 3,9920124.0
        2016-01-03,524 4,12846075.0
        2016-01-03,524 5,3635405.0
        2016-01-04,524 1,1624737.0
        2016-01-04,524 2,1644502.0
        2016-01-04,524 3,9850371.0
        2016-01-04,524 4,13515314.0
        2016-01-04,524 5,3567440.0
        """
    ).strip()
    assert result_expected == result


def test_dfs_total_transaction_commission(get_dataframe):
    """
    Total transaction amount returns expected result.
    """
    q = DFSTotalMetricAmount(
        metric="commission",
        start_date="2016-01-05",
        end_date="2016-01-06",
        aggregation_unit="admin2",
    )
    df = get_dataframe(q)
    result = df.to_csv(index=False).strip()
    print(df)
    result_expected = textwrap.dedent(
        """
        date,pcod,value
        2016-01-05,524 1 01,129914.0
        2016-01-05,524 1 02,88893.0
        2016-01-05,524 1 03,117333.0
        2016-01-05,524 2 04,103040.0
        2016-01-05,524 2 05,234969.0
        2016-01-05,524 3 07,446278.0
        2016-01-05,524 3 08,840320.0
        2016-01-05,524 3 09,633194.0
        2016-01-05,524 4 10,640180.0
        2016-01-05,524 4 11,224463.0
        2016-01-05,524 4 12,1743673.0
        2016-01-05,524 5 13,375005.0
        2016-01-05,524 5 14,439312.0
        2016-01-06,524 1 01,100116.0
        2016-01-06,524 1 02,126280.0
        2016-01-06,524 1 03,96396.0
        2016-01-06,524 2 04,105540.0
        2016-01-06,524 2 05,227195.0
        2016-01-06,524 3 07,473434.0
        2016-01-06,524 3 08,818439.0
        2016-01-06,524 3 09,631098.0
        2016-01-06,524 4 10,643176.0
        2016-01-06,524 4 11,238472.0
        2016-01-06,524 4 12,1794329.0
        2016-01-06,524 5 13,331421.0
        2016-01-06,524 5 14,428548.0
        """
    ).strip()
    assert result_expected == result


def test_dfs_total_transaction_fee(get_dataframe):
    """
    Total transaction amount returns expected result.
    """
    q = DFSTotalMetricAmount(
        metric="commission",
        start_date="2016-01-02",
        end_date="2016-01-02",
        aggregation_unit="admin2",
    )
    df = get_dataframe(q)
    result = df.to_csv(index=False).strip()
    result_expected = textwrap.dedent(
        """
        date,pcod,value
        2016-01-02,524 1 01,103164.0
        2016-01-02,524 1 02,106154.0
        2016-01-02,524 1 03,93262.0
        2016-01-02,524 2 04,112061.0
        2016-01-02,524 2 05,201846.0
        2016-01-02,524 3 07,433490.0
        2016-01-02,524 3 08,850995.0
        2016-01-02,524 3 09,671531.0
        2016-01-02,524 4 10,650900.0
        2016-01-02,524 4 11,212018.0
        2016-01-02,524 4 12,1850318.0
        2016-01-02,524 5 13,346616.0
        2016-01-02,524 5 14,395683.0
        """
    ).strip()
    assert result_expected == result
