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
from typing import List, Optional, Union, Tuple

import pandas as pd

from flowmachine.core.query import Query
from flowmachine.features.subscriber import daily_location
from flowmachine.utils import list_of_dates
from flowmachine.features.subscriber import ModalLocation
from flowmachine.core import make_spatial_unit
from flowmachine.core.spatial_unit import LonLatSpatialUnit
from flowmachine.features.spatial.distance_matrix import DistanceMatrix

import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class _PopulationBuffer(Query):
    """
    Private class for calculating a population
    vector based on buffers generated from
    the distance radii between point locations.

    Parameters
    ----------
    population_object : flowmachine.features.utilities.spatial_aggregates.SpatialAggregate
        An aggregated subscriber locating object
    distance_matrix : flowmachine.features.spatial.distance_matrix.DistanceMatrix
        A distance matrix
    """

    def __init__(self, population_object, distance_matrix):
        self.population_object = population_object
        self.distance_matrix = distance_matrix
        self.spatial_unit = self.distance_matrix.spatial_unit

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        cols = self.spatial_unit.location_id_columns

        return (
            [f"{c}_from" for c in cols]
            + [f"{c}_to" for c in cols]
            + ["buffer_population", "src_pop", "sink_pop"]
        )

    def _make_query(self):
        """
        Protected method that generates SQL
        that calculates the population that is
        covered by a buffer.
        """
        cols = self.spatial_unit.location_id_columns

        #
        # The summing over partition - this works because at each row you're summing over the pops of all
        # rows with a distance less than the current one with the partitions over the destination row.
        #

        sql = f"""
        SELECT   
        {", ".join(f"{c}_{direction}" for direction in ("from", "to") for c in cols)},
         1/(sum(src_pop) over (partition by {", ".join(f"{c}_to" for c in cols)} order by value)+sink_pop-src_pop) as buffer_population,
         src_pop, sink_pop 
         FROM (
            (SELECT * FROM 
            (SELECT {", ".join(f"hl_{direction}.{c} as {c}_{direction}" for c in cols for direction in ("to", "from"))}, hl_from.total as src_pop, hl_to.total as sink_pop
            FROM
            ({self.population_object.get_query()}) as hl_from
            LEFT JOIN 
            ({self.population_object.get_query()}) as hl_to
            ON 
                {" OR ".join(f"hl_from.{c} != hl_to.{c}" for c in cols)}
            ) pops
            LEFT JOIN
            ({self.distance_matrix.get_query()}) as dm
            USING ({", ".join(f"{c}_{direction}" for c in cols for direction in ("to", "from"))})
            )
         ) distance_pop_matrix
        
        """

        return sql


class PopulationWeightedOpportunities(Query):
    """

    Parameters
    ----------
    start
    stop
    spatial_unit
    departure_rate
    hours
    method
    table
    subscriber_identifier
    ignore_nulls
    subscriber_subset
    """

    def __init__(
        self,
        start: str,
        stop: str,
        *,
        spatial_unit: Optional[LonLatSpatialUnit] = None,
        departure_rate: Union[pd.DataFrame, float] = 0.1,
        hours: Union[str, Tuple[int, int]] = "all",
        method: str = "last",
        table: Union[str, List[str]] = "all",
        subscriber_identifier: str = "msisdn",
        ignore_nulls: bool = True,
        subscriber_subset: Optional[Query] = None,
    ):

        warnings.warn(
            "The PopulationWeightedOpportunities model is currently **experimental**. "
            + "Please review Yan X-Y et al. "
            + "(http://dx.doi.org/10.1098/rsif.2014.0834) "
            + "before using this model in production."
        )

        if isinstance(departure_rate, pd.DataFrame):
            self.departure_rate = departure_rate.rename(
                columns=lambda x: x if x == "rate" else f"{x}_from"
            )
            self.departure_rate_values = departure_rate.values.tolist()
        elif isinstance(departure_rate, float):
            self.departure_rate = departure_rate
        else:
            raise TypeError(f"{departure_rate} must be a float or dataframe")
        self.start = start
        self.stop = stop
        if spatial_unit is None:
            self.spatial_unit = make_spatial_unit("versioned-site")
        else:
            self.spatial_unit = spatial_unit
        self.distance_matrix = DistanceMatrix(
            spatial_unit=self.spatial_unit, return_geometry=True
        )

        self.population_object = ModalLocation(
            *[
                daily_location(
                    d,
                    spatial_unit=self.spatial_unit,
                    hours=hours,
                    method=method,
                    table=table,
                    subscriber_identifier=subscriber_identifier,
                    ignore_nulls=ignore_nulls,
                    subscriber_subset=subscriber_subset,
                )
                for d in list_of_dates(self.start, self.stop)
            ]
        ).aggregate()

        self.population_buffer_object = _PopulationBuffer(
            population_object=self.population_object,
            distance_matrix=self.distance_matrix,
        )

    @property
    def column_names(self) -> List[str]:
        return [
            "{}_{}".format(c, d)
            for d in ("from", "to")
            for c in self.spatial_unit.location_id_columns
        ] + ["prediction", "probability"]

    def _make_query(self):
        if isinstance(self.departure_rate, float):
            scaled_buffer_query = (
                f"SELECT buffer.src_pop*{self.departure_rate} as T_i, * FROM buffer"
            )
        elif isinstance(self.departure_rate, pd.DataFrame):
            # select * from (VALUES ('a', 0, 1)) as t(name, version, value);
            scaled_buffer_query = f"""
            SELECT buffer.*, buffer.src_pop*rate as T_i FROM 
            (VALUES {", ".join([str(tuple(x)) for x in self.departure_rate.values])}) 
                AS t({", ".join(c for c in self.departure_rate.columns)})
                LEFT JOIN buffer
                USING ({", ".join(c for c in self.departure_rate.columns if c != 'rate')})
            """
        else:
            raise ValueError(
                "Unexpected departure rate type! Got {self.departure_rate}."
            )

        return f"""
        WITH buffer AS ({self.population_buffer_object.get_query()}),
        beta AS (SELECT 1.0/sum(total) as beta FROM ({self.population_object.get_query()}) pops),
        sigma AS (
            SELECT
             {", ".join(f"{c}_from" for c in self.spatial_unit.location_id_columns)},
             sum(sink_pop*(buffer_population-(SELECT beta FROM beta))) as sigma
             FROM buffer
             GROUP BY {", ".join(f"{c}_from" for c in self.spatial_unit.location_id_columns)}
        )
        SELECT {", ".join("{}_{}".format(c, d)
            for d in ("from", "to")
            for c in self.spatial_unit.location_id_columns)}, prediction, COALESCE(prediction / T_i, 0.) as probability
        FROM
        (SELECT {", ".join("{}_{}".format(c, d)
            for d in ("from", "to")
            for c in self.spatial_unit.location_id_columns)},
            NULLIF(T_i, 0) as T_i,
            (T_i*sink_pop*(buffer_population-(SELECT beta FROM beta)))/sigma as prediction
        
        FROM
        ({scaled_buffer_query}) scaled_buf
        LEFT JOIN
        sigma
        USING ({", ".join(f"{c}_from" for c in self.spatial_unit.location_id_columns)}) ) pwo_pred
            
        """
