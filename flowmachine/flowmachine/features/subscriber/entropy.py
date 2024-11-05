# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Calculates various entropy metrics for subscribers with a specified time
period.
"""

from abc import ABCMeta, abstractmethod
from typing import List, Union, Optional, Tuple

from flowmachine.core import make_spatial_unit
from flowmachine.core.spatial_unit import AnySpatialUnit
from flowmachine.features.utilities.events_tables_union import EventsTablesUnion
from flowmachine.features.utilities.subscriber_locations import SubscriberLocations
from flowmachine.features.subscriber.contact_balance import ContactBalance
from flowmachine.features.subscriber.metaclasses import SubscriberFeature
from flowmachine.features.utilities.direction_enum import Direction
from flowmachine.utils import make_where, standardise_date


class BaseEntropy(SubscriberFeature, metaclass=ABCMeta):
    """Base query for calculating entropy of subscriber features."""

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "entropy"]

    def _make_query(self):
        return f"""
        SELECT
            subscriber,
            -1 * SUM( relative_freq * LN( relative_freq ) ) AS entropy
        FROM ({self._relative_freq_query}) u
        GROUP BY subscriber
        """

    @property
    @abstractmethod
    def _absolute_freq_query(self):
        raise NotImplementedError

    @property
    def _relative_freq_query(self):
        return f"""
        SELECT
            subscriber,
            absolute_freq::float / ( SUM( absolute_freq ) OVER ( PARTITION BY subscriber ) ) AS relative_freq
        FROM ({self._absolute_freq_query}) u
        """


class PeriodicEntropy(BaseEntropy):
    """
    Calculates the recurrence period entropy for events, that is the entropy
    associated with the period in which events take place. For instance, if
    events regularly occur at a certain time of day, say at 9:00 and 18:00 then
    this user will have a low period entropy.

    Entropy is calculated as:

        -1 * SUM( relative_freq * LN( relative_freq ) )

    where `relative_freq` is the relative frequency of events occurring at a
    certain period (eg. hour of the day, day of the week, month of the year).

    This formula represents a consistent estimate of the true entropy only
    under certain conditions. Among them, that the relative frequency is a good
    approximation to the probability that a certain event occurs within certain
    periodic phases. In case of strong autocorrelation, this might not be true.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    phase : {"century", "day", "decade", "dow", "doy", "epoch", "hour",
            "isodow", "isoyear", "microseconds", "millennium", "milliseconds",
            "minute", "month", "quarter", "second", "week", "year"}, default 'hour'
        The phase of recurrence for which one wishes to calculate the entropy
        for. See [Postgres
        manual](https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT)
        for further info on the allowed phases.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    direction : {'in', 'out', 'both'} or Direction, default Direction.BOTH
        Whether to consider calls made, received, or both. Defaults to 'both'.
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    tables : str or list of strings, default 'all'
        Can be a string of a single table (with the schema)
        or a list of these. The keyword all is to select all
        subscriber tables

    Examples
    --------

    >>> s = PeriodicEntropy("2016-01-01", "2016-01-07")
    >>> s.get_dataframe()

             subscriber   entropy
       038OVABN11Ak4W5P  2.805374
       09NrjaNNvDanD8pk  2.730881
       0ayZGYEQrqYlKw6g  2.802434
       0DB8zw67E9mZAPK2  2.476354
       0Gl95NRLjW2aw8pW  2.788854
                    ...       ...
    """

    def __init__(
        self,
        start,
        stop,
        phase="hour",
        *,
        subscriber_identifier="msisdn",
        direction: Union[str, Direction] = Direction.BOTH,
        hours: Optional[Tuple[int, int]] = None,
        subscriber_subset=None,
        tables="all",
    ):
        self.tables = tables
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.subscriber_identifier = subscriber_identifier
        self.direction = Direction(direction)
        self.hours = hours

        column_list = [
            self.subscriber_identifier,
            "datetime",
            *self.direction.required_columns,
        ]

        # extracted from the POSTGRES manual
        allowed_phases = (
            "century",
            "day",
            "decade",
            "dow",
            "doy",
            "epoch",
            "hour",
            "isodow",
            "isoyear",
            "microseconds",
            "millennium",
            "milliseconds",
            "minute",
            "month",
            "quarter",
            "second",
            "week",
            "year",
        )

        if phase not in allowed_phases:
            raise ValueError(
                f"{phase} is not a valid phase. Choose one of {allowed_phases}"
            )

        self.phase = phase

        self.unioned_query = EventsTablesUnion(
            self.start,
            self.stop,
            tables=self.tables,
            columns=column_list,
            hours=hours,
            subscriber_identifier=subscriber_identifier,
            subscriber_subset=subscriber_subset,
        )
        super().__init__()

    @property
    def _absolute_freq_query(self):
        return f"""
        SELECT subscriber, COUNT(*) AS absolute_freq FROM
        ({self.unioned_query.get_query()}) u
        {make_where(self.direction.get_filter_clause())}
        GROUP BY subscriber, EXTRACT( {self.phase} FROM datetime )
        HAVING COUNT(*) > 0
        """


class LocationEntropy(BaseEntropy):
    """
    Calculates the entropy of locations visited. For instance, if an individual
    regularly makes her/his calls from certain location then this user will
    have a low location entropy.

    Entropy is calculated as:

        -1 * SUM( relative_freq * LN( relative_freq ) )

    where `relative_freq` is the relative frequency of events occurring at a
    certain location (eg. cell, site, admnistrative region, etc.).

    This formula represents a consistent estimate of the true entropy only
    under certain conditions. Among them, that the relative frequency is a good
    approximation to the probability that a certain event occurs in a given
    location. In case of strong spatial autocorrelation, this might not be
    true.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default cell
        Spatial unit to which subscriber locations will be mapped. See the
        docstring of make_spatial_unit for more information.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    tables : str or list of strings, default 'all'
        Can be a string of a single table (with the schema)
        or a list of these. The keyword all is to select all
        subscriber tables

    Examples
    --------

    >>> s = LocationEntropy("2016-01-01", "2016-01-07")
    >>> s.get_dataframe()

              subscriber   entropy
        038OVABN11Ak4W5P  2.832747
        09NrjaNNvDanD8pk  3.184784
        0ayZGYEQrqYlKw6g  3.072458
        0DB8zw67E9mZAPK2  2.838989
        0Gl95NRLjW2aw8pW  2.997069
                     ...       ...
    """

    def __init__(
        self,
        start,
        stop,
        *,
        spatial_unit: AnySpatialUnit = make_spatial_unit("cell"),
        subscriber_identifier="msisdn",
        hours: Optional[Tuple[int, int]] = None,
        subscriber_subset=None,
        tables="all",
        ignore_nulls=True,
    ):
        self.subscriber_locations = SubscriberLocations(
            start=start,
            stop=stop,
            spatial_unit=spatial_unit,
            table=tables,
            hours=hours,
            subscriber_identifier=subscriber_identifier,
            subscriber_subset=subscriber_subset,
            ignore_nulls=ignore_nulls,
        )

        super().__init__()

    @property
    def _absolute_freq_query(self):
        location_cols = ", ".join(
            self.subscriber_locations.spatial_unit.location_id_columns
        )

        return f"""
        SELECT subscriber, COUNT(*) AS absolute_freq FROM
        ({self.subscriber_locations.get_query()}) u
        GROUP BY subscriber, {location_cols}
        HAVING COUNT(*) > 0
        """


class ContactEntropy(BaseEntropy):
    """
    Calculates the entropy of counterparts contacted. For instance, if an
    individual regularly interacts with a few determined counterparts on a
    predictable way then this user will have a low contact entropy.

    Entropy is calculated as:

        -1 * SUM( relative_freq * LN( relative_freq ) )

    where `relative_freq` is the relative frequency of events with a given
    counterpart.

    This formula represents a consistent estimate of the true entropy only
    under certain conditions. Among them, that the relative frequency is a good
    approximation to the probability that a certain event will occur with a
    given counterpart. In case of strong correlation between counterparts, this
    might not be true.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    direction : {'in', 'out', 'both'} or Direction, default Direction.BOTH
        Whether to consider calls made, received, or both. Defaults to 'both'.
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    tables : str or list of strings, default 'all'
        Can be a string of a single table (with the schema)
        or a list of these. The keyword all is to select all
        subscriber tables
    exclude_self_calls : bool, default True
        Set to false to *include* calls a subscriber made to themself

    Examples
    --------

    >>> s = ContactEntropy("2016-01-01", "2016-01-07")
    >>> s.get_dataframe()

          subscriber   entropy
    2ZdMowMXoyMByY07  0.692461
    MobnrVMDK24wPRzB  0.691761
    0Ze1l70j0LNgyY4w  0.693147
    Nnlqka1oevEMvVrm  0.607693
    gPZ7jbqlnAXR3JG5  0.686211
                 ...       ...
    """

    def __init__(
        self,
        start,
        stop,
        *,
        subscriber_identifier="msisdn",
        direction: Union[str, Direction] = Direction.BOTH,
        hours: Optional[Tuple[int, int]] = None,
        subscriber_subset=None,
        tables="all",
        exclude_self_calls=True,
    ):
        self.contact_balance = ContactBalance(
            start=start,
            stop=stop,
            hours=hours,
            tables=tables,
            subscriber_identifier=subscriber_identifier,
            direction=direction,
            exclude_self_calls=exclude_self_calls,
            subscriber_subset=subscriber_subset,
        )

    @property
    def _absolute_freq_query(self):
        return f"""
        SELECT subscriber, events AS absolute_freq FROM
        ({self.contact_balance.get_query()}) u
        """

    @property
    def _relative_freq_query(self):
        return f"""
        SELECT subscriber, proportion AS relative_freq FROM
        ({self.contact_balance.get_query()}) u
        """
