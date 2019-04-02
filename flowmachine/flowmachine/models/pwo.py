# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
The population-weighted opportunities model uses
population distributions for predicting the
probability of movement between populated
areas. The model computes the attraction
between populated centers relative to the
distance between each origin and destination
locations and also to all other equidistant
locations from the destination. The model
works similarly to the Radiation Model.

The original publication suggests that its
ideal usage is in cities.



References
----------
    Yan X-Y, Zhao C, Fan Y, Di Z, Wang W-X. 2014 "Universal predictability of mobility patterns in cities". J. R. Soc. Interface 11: 20140834. http://dx.doi.org/10.1098/rsif.2014.0834

"""

import warnings
from typing import List

import pandas as pd

from flowmachine.features import daily_location
from flowmachine.utils import get_columns_for_level, list_of_dates
from ..features import ModalLocation
from ..core.query import Query
from ..core.model import Model, model_result
from ..features.spatial.distance_matrix import DistanceMatrix

import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class _populationBuffer(Query):
    """
    Private class for calculating a population
    vector based on buffers generated from
    the distance radii between point locations.

    Parameters
    ----------
    level : str
        Levels can be one of:
            'cell':
                The identifier as it is found in the CDR itself
            'versioned-cell':
                The identifier as found in the CDR combined with the version from
                the cells table.
            'versioned-site':
                The ID found in the sites table, coupled with the version
                number.
            'polygon':
                A custom set of polygons that live in the database. In which
                case you can pass the parameters column_name, which is the column
                you want to return after the join, and table_name, the table where
                the polygons reside (with the schema), and additionally geom_col
                which is the column with the geometry information (will default to
                'geom')
            'admin*':
                An admin region of interest, such as admin3. Must live in the
                database in the standard location.
            'grid':
                A square in a regular grid, in addition pass size to
                determine the size of the polygon.
    population_object : flowmachine.features.utilities.spatial_aggregates.SpatialAggregate
        An aggregated subscriber locating object
    distance_matrix : flowmachine.features.spatial.distance_matrix.DistanceMatrix
        A distance matrix
    """

    def __init__(self, level, population_object, distance_matrix):

        self.level = level
        self.population_object = population_object
        self.distance_matrix = distance_matrix

        super().__init__()

    def __get_location_buffer(self):
        """
        Protected method for generating SQL
        for the buffer areas between a location
        (i..e an origin) and all its possible
        counterparts (i.e. destinations).
        """
        cols = get_columns_for_level(self.level)

        from_cols = ", ".join("{c}_from".format(c=c) for c in cols)
        to_cols = ", ".join("{c}_to".format(c=c) for c in cols)
        sql = """

            SELECT
                {froms},
                {tos},
                A.distance AS distance,
                A.geom_origin AS geom_origin,
                A.geom_destination AS geom_destination,
                ST_Buffer(A.geom_destination::geography, A.distance * 1000) AS geom_buffer
            FROM ({distance_matrix_table}) AS A

        """.format(
            distance_matrix_table=self.distance_matrix.get_query(),
            froms=from_cols,
            tos=to_cols,
        )

        return sql

    @property
    def column_names(self) -> List[str]:
        cols = get_columns_for_level(self.level)

        return (
            ["id"]
            + [f"{c}_from" for c in cols]
            + [f"{c}_to" for c in cols]
            + ["distance", "geom_buffer", "buffer_population", "n_sites"]
        )

    def _make_query(self):
        """
        Protected method that generates SQL
        that calculates the population that is
        covered by a buffer.
        """
        cols = get_columns_for_level(self.level)

        from_cols = ", ".join("B.{c}_from".format(c=c) for c in cols)
        outer_from_cols = ", ".join("C.{c}_from".format(c=c) for c in cols)
        to_cols = ", ".join("B.{c}_to".format(c=c) for c in cols)
        outer_to_cols = ", ".join("C.{c}_to".format(c=c) for c in cols)
        pop_join = " AND ".join("A.{c} = B.{c}_to".format(c=c) for c in cols)
        sql = """
            SELECT row_number() OVER(ORDER BY C.geom_buffer) AS id,
                {froms_outer},
                {tos_outer},
                C.distance,
                C.geom_buffer,
                sum(C.destination_population) AS buffer_population,
                count(*) AS n_sites
                FROM
                (SELECT
                    {froms},
                    {tos},
                    B.distance,
                    B.geom_buffer,
                    A.destination_population
                FROM (
                    SELECT DISTINCT ON ({tos})
                        {tos},
                        B.geom_destination,
                        A.total AS destination_population
                    FROM ({population_table}) AS A
                    JOIN ({location_buffer_table}) AS B
                        ON {pop_join}
                ) AS A
                INNER JOIN ({location_buffer_table}) AS B
                    ON ST_Intersects(B.geom_buffer::geography,          
                                    A.geom_destination::geography)) AS C
            GROUP BY {froms_outer},
                    {tos_outer},
                    C.distance,
                    C.geom_buffer

        """.format(
            population_table=self.population_object.get_query(),
            location_buffer_table=self.__get_location_buffer(),
            pop_join=pop_join,
            froms=from_cols,
            tos=to_cols,
            froms_outer=outer_from_cols,
            tos_outer=outer_to_cols,
        )

        return sql


class PopulationWeightedOpportunities(Model):
    """
    Population-weighted opportunities model [1]_.



    The model predicts the mobility between populated
    areas in cities based only on the population densities
    of those areas, their spatial distribution, and
    the number of people that depart a certain area. This
    model is useful for studying mobility pattern in cities.

    Parameters
    ----------
    start : str
        Start of time period to analyse.

    stop : str
        Stop of time period to analyse.

    method : str
        Method used to calculate population using the
        Population() recipe. 'home-location' is the
        default method used. Refer to the Population()
        documentation for other available methods.

    level : str
        {levels}

    **kwargs : arguments
        Used to pass custom arguments to the DistanceMatrix()
        and ModalLocation() objects.

    Examples
    --------
    Much like other `flowmachine` classes, this model
    has to be instantiated:

    >>> p = PopulationWeightedOpportunities('2016-01-01', '2016-01-07')

    After instantiation, the model will run using the
    PopulationWeightedOpportunities().run() method
    as follows:

    >>> p.run(departure_rate_vector={'0xqNDj': 0.9}, ignore_missing=True)

    One can also run the model with uniform departure
    rates for all locations as follows:

    >>> p.run(uniform_departure_rate=0.5)
        origin  destination  prediction  probability
    0  0xqNDj        8wPojr    0.384117     0.010670
    1  0xqNDj        B8OaG5    0.344384     0.009566
    2  0xqNDj        DonxkP    0.715311     0.019870
    3  0xqNDj        zdNQx2    0.267854     0.007440

    Where prediction is the absolute number of people
    that move from one location to another. (This should
    be interpreted as a integer, but floats are provided
    for evaluating results in a continuous scale.) And
    probability is the predicted value over the total
    population leaving the origin (T_i). That is, how
    likely it is that a person leaving the origin will
    be found in a given destination.

    References
    ----------
    .. [1] Yan X-Y, Zhao C, Fan Y, Di Z, Wang W-X. 2014 "Universal predictability of mobility patterns in cities". J. R. Soc. Interface 11: 20140834. http://dx.doi.org/10.1098/rsif.2014.0834

    """

    def __init__(
        self, start, stop, method="home-location", level="versioned-site", **kwargs
    ):

        warnings.warn(
            "The PWO model is currently **experimental**. "
            + "Please review Yan X-Y et al. "
            + "(http://dx.doi.org/10.1098/rsif.2014.0834) "
            + "before using this model in production."
        )

        self.start = start
        self.stop = stop
        self.method = method
        self.level = level
        self.distance_matrix = DistanceMatrix(
            date=self.stop, level=level, return_geometry=True, **kwargs
        )

        if self.method == "home-location":
            self.population_object = ModalLocation(
                *[
                    daily_location(d, level=self.level, **kwargs)
                    for d in list_of_dates(self.start, self.stop)
                ]
            ).aggregate()

        self.population_buffer_object = _populationBuffer(
            level=self.level,
            population_object=self.population_object,
            distance_matrix=self.distance_matrix,
        )

    def __get_population(self, df, i):
        """
        Protected getter method for getting
        the location value of the self.population_df
        DataFrame.

        Parameters
        ----------
        i : object
            The index in the self.population_df DataFrame
            containing the location identifier.

        Returns
        -------
        A float value with the population value
        for location i.

        """
        return df.loc[tuple(i), "total"]

    def __get_buffer_population(self, df, i, j):
        """
        Protected getter method for getting
        the location value of the self.population_buffer
        DataFrame.

        Parameters
        ----------
        i : str
            Location identifier of the origin location
            in the self.population_buffer DataFrame.
        j : str
            Location identifier of the destination location
            in the self.population_buffer DataFrame.

        Returns
        -------
        A float value with the population value for
        a pair of origin (i) and destination (j)
        locations.

        """
        filtered = df.loc[tuple(i + j)]

        return filtered["buffer_population"]

    @model_result
    def run(
        self,
        uniform_departure_rate=0.1,
        departure_rate_vector=None,
        ignore_missing=False,
    ):
        """
        Runs model.

        Parameters
        ----------
        uniform_departure_rate : float
            Proportion of population from location i
            that will be departing in observed time period.
            This proportion applies to all locations
            uniformly.

        departure_rate_vector : dict
            A dictionary that contains the proportion
            of the population from locations i that have
            departed those locations. The keys of the
            dictionaries must be the location identifier
            and the values the departure rate.
            If passed, this will be used over the
            `uniform_departure_rate` parameter.

        ignore_missing : bool
            If True, existing locations that are not
            found in the departure_rate_vector dictionary
            will be computed using zero departures.

        Returns
        -------
        A pandas dataframe with a mobility matrix.

        """

        if "population_buffer" not in self.__dict__.keys():
            logger.warning(
                " Computing Population() and DistanceMatrix() "
                + "objects. This can take a few minutes."
            )

            population_df = self.population_object.get_dataframe()
            population_buffer = self.population_buffer_object.get_dataframe()
            ix = get_columns_for_level(self.level)
            ix = ["{}_{}".format(c, d) for d in ("from", "to") for c in ix]
            population_buffer.set_index(ix, inplace=True)

            M = population_df["total"].sum()
            N = len(population_df[get_columns_for_level(self.level)].drop_duplicates())
            beta = 1 / M

            locations = population_df[get_columns_for_level(self.level)].values.tolist()
            population_df.set_index(get_columns_for_level(self.level), inplace=True)

        if not departure_rate_vector:
            logger.warning(
                " Using an uniform departure "
                + "rate of {} for ".format(uniform_departure_rate)
                + "all locations."
            )
        elif not ignore_missing and len(departure_rate_vector) != len(locations):
            raise ValueError(
                "Locations missing from "
                + "`departure_rate_vector`. Use "
                + "ignore_missing=True if locations "
                + "without rates should be ignored."
            )

        results = []
        for i in locations:
            sigma = 0
            m_i = self.__get_population(population_df, i)

            if departure_rate_vector:
                try:
                    T_i = m_i * departure_rate_vector[i[0]]
                except KeyError:
                    try:
                        T_i = m_i * departure_rate_vector[tuple(i[:1])]
                    except KeyError:
                        T_i = 0
            else:
                T_i = m_i * uniform_departure_rate

            for k in [l for l in locations if l != i]:
                m_k = self.__get_population(population_df, k)
                S_ik = self.__get_buffer_population(population_buffer, i, k)

                sigma += m_k * ((1 / S_ik) - beta)

            for j in [l for l in locations if l != i]:
                m_j = self.__get_population(population_df, j)
                S_ij = self.__get_buffer_population(population_buffer, i, j)

                T_ij = (T_i * m_j * ((1 / S_ij) - beta)) / sigma

                if T_i != 0:
                    probability = T_ij / T_i
                else:
                    probability = 0

                results.append(i + j + [T_ij, probability])
        ix = get_columns_for_level(self.level)
        ix = ["{}_{}".format(c, d) for d in ("from", "to") for c in ix]
        ix += ["prediction", "probability"]
        res = pd.DataFrame(results, columns=ix)
        return res
