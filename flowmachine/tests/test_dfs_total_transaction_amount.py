# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import textwrap

from flowmachine.features.dfs.total_transaction_amount import DFSTotalTransactionAmount


def test_column_names():
    """
    Test that column_names property contains expected column names
    """
    q = DFSTotalTransactionAmount(
        start_date="2016-01-01", end_date="2016-01-04", aggregation_unit="admin2"
    )
    assert q.column_names == ["date", "pcod", "value"]
    assert q.head(0).columns.tolist() == q.column_names
    q = DFSTotalTransactionAmount(
        start_date="2016-01-02", end_date="2016-01-05", aggregation_unit="admin3"
    )
    assert q.column_names == ["date", "pcod", "value"]
    assert q.head(0).columns.tolist() == q.column_names


def test_dfs_total_transaction_amount_values(get_dataframe):
    """
    DFSTotalTransactionAmount returns expected result.
    """
    q = DFSTotalTransactionAmount(
        start_date="2016-01-02", end_date="2016-01-04", aggregation_unit="admin2"
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
