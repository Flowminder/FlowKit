# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Classes that deal with location clustering using different methods.
These classes attempt to reduce the number of locations for each subscriber,
by clustering a set of locations according to a specified methodology.

These methods are great options for reducing the dimensionality of the
problem in hand.
"""

from typing import List, Union

from ..utilities import SubscriberLocations
from ...core import make_spatial_unit
from ...core.query import Query
from ...core.mixins import GeoDataMixin
from .call_days import CallDays


class BaseCluster(GeoDataMixin, Query):
    """Base query for cluster methods, providing a geo augmented query method."""

    def _geo_augmented_query(self):
        cols = [c for c in self.column_names if c != "cluster"]
        return (
            f"SELECT {', '.join(cols)}, cluster as geom, row_number() over() as gid FROM ({self.get_query()}) q",
            cols + ["geom", "gid"],
        )


class HartiganCluster(BaseCluster):
    """
    Implements the Hartigan Clustering algorithm.

    The algorithm clusters locations based on a ranked listed of call days.
    [1]_

    The Hartigan clustering algorithm will pick up the site associated with
    the highest call days and form a cluster on that site. It will descend
    the ranked list of call days from the top. The second site associated
    with the highest call days will be incorporated into the the first
    cluster if it is within a certain radius of it. In such case, a new
    cluster centroid is calculated by taking a weighted average by the call
    days of all the sites in the cluster. Eventually, when the algorithm
    lands on a site which is not within the radius of all the available
    clusters, it will create a new cluster on that particular site.

    The class will produce a table where each row lists a cluster for a
    given subscriber, the rank of that cluster, the total number of call days in
    that cluster (which is the sum of all call days constituting that
    cluster and not the actual call days of the cluster), and all the sites
    which constitute that cluster.

    Parameters
    ----------
    calldays : flowmachine.core.Query
        The calls day table which contains the call day data per subscriber. This
        table should follow the same format as the table produced with the
        `CallDays` class.
    radius : float or str
        The threshold value in km to be used for clustering towers. If a
        string is passed, it is assumed that it is the name of the column in
        the call day table.
    buffer : float
        The buffer radius size in km to be used for buffering the cluster
        centroid. If the cluster is formed by only one site, then the
        buffer radius size has no effect and the cluster centroid is
        buffered to the polygon representing the given site. If buffer is
        0, only the cluster centroids are returned.
    call_threshold : float
        The minimum number of calls that a cluster must have. Any cluster
        with less than that amount of calls will be eliminated.

    Examples
    --------
    >>> cd = CallDays( '2016-01-01', '2016-01-04', spatial_unit=make_spatial_unit('versioned-site'))

    >>> har = HartiganCluster(cd, 2.5)

    >>> har.head()
                subscriber                                cluster  rank  calldays
    038OVABN11Ak4W5P        POINT (82.60170958 29.81591927)     1         2
    038OVABN11Ak4W5P    POINT (82.91428457000001 29.358975)     2         2
    038OVABN11Ak4W5P  POINT (81.63916106000001 28.21192983)     3         1
    038OVABN11Ak4W5P        POINT (87.26522455 27.58509554)     4         1
    038OVABN11Ak4W5P  POINT (80.86633861999999 28.70767038)     5         1
    ...
    site_id version
       [m9jL23]     [0]
       [QeBRM8]     [0]
       [nWM8R3]     [0]
       [zdNQx2]     [0]
       [pqg7ZE]     [0]
    ...

    References
    ----------
    .. [1] S. Isaacman et al., "Identifying Important Places in People's Lives from Cellular Network Data", International Conference on Pervasive Computing (2011), pp 133-151.

    """

    def __init__(
        self,
        *,
        calldays: CallDays,
        radius: Union[float, str],
        buffer: float = 0,
        call_threshold: int = 0,
    ):
        """"""

        self.calldays = calldays
        try:
            if (
                "site_id" not in calldays.column_names
                or "version" not in calldays.column_names
            ):
                raise ValueError(
                    "calldays must include 'site_id' and 'version' columns."
                )
        except AttributeError:
            raise TypeError(
                "calldays must be a subclass of Query (e.g. CallDays, Table, CustomQuery"
            )
        self.radius = float(radius)
        self.call_threshold = int(call_threshold)
        self.buffer = float(buffer)
        super().__init__()

    def _make_query(self):

        calldays = "({}) AS calldays".format(self.calldays.get_query())

        sql = f"""
        SELECT clusters.subscriber AS subscriber,
               unnest(clusters) AS cluster,
               unnest(rank) AS rank,
               unnest(weights) AS calldays,
               regexp_split_to_array(unnest(site_ids), '::') AS site_id,
               regexp_split_to_array(unnest(versions), '::')::integer[] AS version
        FROM (
            SELECT calldays.subscriber, (hartigan(calldays.site_id, calldays.version, calldays.value::integer, {self.radius},
                {self.buffer}, {self.call_threshold})).*
            FROM {calldays}
            GROUP BY calldays.subscriber
        ) clusters
        """

        return sql

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "cluster", "rank", "calldays", "site_id", "version"]

    def join_to_cluster_components(self, query):
        """
        Join the versioned-sites composing the Hartigan cluster table with
        another table containing versioned-sites.

        Parameters
        ----------
        query : flowmachine.Query
            A flowmachine.Query object.
            This represents a table that can be joined to the versioned-sites
            composing the Hartigan cluster table. This must have a column
            called 'site_id', another column called 'version' and another
            called 'subscriber'. The remaining columns will be averaged over.

        Examples
        --------

        >>> es = EventScore(start='2016-01-01', stop='2016-01-05',
        spatial_unit=make_spatial_unit('versioned-site'))

        >>> cd = CallDays(start='2016-01-01', stop='2016-01-04',
        spatial_unit=make_spatial_unit('versioned-site'))

        >>> har = HartiganCluster(cd, 50, call_threshold=1)

        >>> har.join_to_cluster_components(es).head(geom=['cluster'])
                    subscriber                                      cluster  rank
        038OVABN11Ak4W5P              POINT (87.26522455 27.58509554)     4
        038OVABN11Ak4W5P               POINT (86.00007467 27.2713931)     7
        038OVABN11Ak4W5P              POINT (83.51373348 28.14524211)     8
        038OVABN11Ak4W5P  POINT (82.97508908333334 29.28452965333333)     2
        038OVABN11Ak4W5P        POINT (83.02805528499999 28.42765618)     6
        ...
        calldays  score_hour  score_dow
               1   -1.000000   0.000000
               1    1.000000   0.000000
               1   -1.000000  -1.000000
               3   -0.666667  -0.666667
               2    0.000000  -0.500000
        ...
        """
        return _JoinedHartiganCluster(self, query)


class _JoinedHartiganCluster(BaseCluster):
    """
    Join the versioned-sites composing the Hartigan cluster table with other
    another table containing versioned-sites.

    This is a helper class, wrapping the whole joining logic of the HartiganCluster class.
    See `HartiganCluster.join` for example use.

    Parameters
    ----------
    hartigan : flowmachine.HartiganCluster
        A flowmachine.HartiganCluster object on which we wish to join
        the second argument table.
    query : flowmachine.Query
        A flowmachine.Query object.
        This represents a table that can be joined to the versioned-sites
        composing the Hartigan cluster table. This must have a column
        called 'site_id', another column called 'version' and another
        called 'user'. The remaining columns will be averaged over.
    """

    def __init__(self, hartigan, query):
        self.hartigan = hartigan
        self.query = query
        if not isinstance(query, Query):
            raise TypeError(
                f"{query} is not a valid type of object. Should be a Query type object."
            )
        if "site_id" not in query.column_names or "version" not in query.column_names:
            raise ValueError(
                "scores query must include 'site_id' and 'version' columns."
            )
        super().__init__()

    def _make_query(self):

        table = f"({self.query.get_query()}) AS t"

        value_cols = ", ".join(
            [
                f"SUM(t.{c})::float/COUNT(*) AS {c}"
                for c in self.query.column_names
                if c not in ["subscriber", "site_id", "version"]
            ]
        )

        hartigan = f"({self.hartigan.get_query()}) AS h"

        sql = f"""
        SELECT hartigan.subscriber,
               hartigan.cluster,
               hartigan.rank,
               hartigan.calldays,
               {value_cols}
        FROM (
            SELECT h.subscriber, h.cluster, h.rank, h.calldays,
            UNNEST(site_id) AS site_id, UNNEST(version) AS version
        FROM {hartigan}) AS hartigan
        LEFT JOIN
        {table}
        ON
        hartigan.site_id = t.site_id AND
        hartigan.version = t.version AND
        hartigan.subscriber = t.subscriber
        GROUP BY hartigan.subscriber,
                hartigan.cluster,
                hartigan.rank,
                hartigan.calldays
        """

        return sql

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "cluster", "rank", "calldays"] + [
            c
            for c in self.query.column_names
            if c not in ("subscriber", "site_id", "version")
        ]
