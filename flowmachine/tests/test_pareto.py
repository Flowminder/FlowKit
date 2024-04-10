# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import math

import pandas as pd

from flowmachine.features import ContactBalance, ParetoInteractions


def percent_pareto_interactions(subscriber_count, percentage=0.8):
    """
    The percentage of subscriber's contacts that account for 80% of its interactions.
    """
    if len(subscriber_count) == 0:
        return None

    target = int(math.ceil(sum(subscriber_count.values()) * percentage))
    subscriber_sort = sorted(subscriber_count.keys(), key=lambda x: subscriber_count[x])

    while target > 0 and len(subscriber_sort) > 0:
        subscriber_id = subscriber_sort.pop()
        target -= subscriber_count[subscriber_id]

    return (len(subscriber_count) - len(subscriber_sort)) / len(subscriber_count)


def paretos(df):
    subscribers = list(set(df.subscriber.values))
    df = df.set_index("subscriber")
    ps = []
    for u in subscribers:
        counts = df.loc[u][["msisdn_counterpart", "events"]].values
        if counts.shape == (2,):
            ps.append(1)
        else:
            subscriber_count = dict(zip(*zip(*counts)))
            ps.append(percent_pareto_interactions(subscriber_count))
    return pd.DataFrame({"subscriber": subscribers, "value": ps})


def test_pareto(get_dataframe):
    """Test pareto proportion is correct for some hand picked subscribers."""
    p = ParetoInteractions("2016-01-01", "2016-01-02")
    assert all(get_dataframe(p).set_index("subscriber").loc["VkzMxYjv7mYn53oK"] == 0.75)

    p = ParetoInteractions("2016-01-03", "2016-01-04", direction="in")
    assert all(get_dataframe(p).set_index("subscriber").loc["ZM3zYAPqx95Rw15J"] == 1)

    p = ParetoInteractions("2016-01-03", "2016-01-04", direction="out")
    assert all(get_dataframe(p).set_index("subscriber").loc["YK6z2lXzg7w57Vap"] == 1)


def test_pareto_nepal(get_dataframe):
    """Test flowmachine's method against the nepal code."""
    cb = ContactBalance("2016-01-01", "2016-01-07", exclude_self_calls=False)
    p = ParetoInteractions("2016-01-01", "2016-01-07")
    df = get_dataframe(p)
    df2 = paretos(get_dataframe(cb))
    assert all(
        df.set_index("subscriber").sort_index()
        == df2.set_index("subscriber").sort_index()
    )


def test_pareto__call(get_dataframe):
    """
    Test that subscribers who only call themselves get pareto 1.
    """

    pi = get_dataframe(
        ParetoInteractions(
            "2016-01-01 00:00:00",
            "2016-01-01 00:00:50",
            subscriber_subset="self_caller",
        )
    )
    assert 1.0 == pi.value[0]


def test_pareto__call_exclusion(get_length):
    """
    Test that subscribers who only call themselves can be excluded from pareto.
    """

    pi = ParetoInteractions(
        "2016-01-01 00:00:00",
        "2016-01-01 00:00:50",
        exclude_self_calls=True,
        subscriber_subset="self_caller",
    )
    assert 0 == get_length(pi)
