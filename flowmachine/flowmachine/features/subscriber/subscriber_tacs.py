# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Features that represent TACs (unique phone model identifiers), and handsets associated with
a subscriber.



"""
import warnings
from typing import List

from ..utilities import EventsTablesUnion
from .metaclasses import SubscriberFeature
from ...core import Table
from flowmachine.utils import standardise_date

valid_characteristics = {
    "brand",
    "depth",
    "display_colors",
    "display_height",
    "display_type",
    "display_width",
    "hardware_bluetooth",
    "hardware_edge",
    "hardware_gprs",
    "hardware_gps",
    "hardware_umts",
    "hardware_wifi",
    "height",
    "hnd_type",
    "j2me_cldc_10",
    "j2me_cldc_11",
    "j2me_cldc_20",
    "j2me_midp_10",
    "j2me_midp_20",
    "j2me_midp_21",
    "mms_built_in_camera",
    "mms_receiver",
    "model",
    "software_os_name",
    "software_os_vendor",
    "software_os_version",
    "syncml_dm_acc_gprs",
    "syncml_dm_app_bookmark",
    "syncml_dm_app_browser",
    "syncml_dm_app_internet",
    "syncml_dm_app_java",
    "syncml_dm_app_mms",
    "syncml_dm_settings",
    "tac",
    "wap_1_2_1",
    "wap_2_0",
    "wap_push_oma_app_browser",
    "wap_push_oma_app_ims",
    "wap_push_oma_app_internet",
    "wap_push_oma_app_mms",
    "wap_push_oma_app_poc",
    "wap_push_oma_cp_bookmarks",
    "wap_push_oma_settings",
    "wap_push_ota_app_browser",
    "wap_push_ota_app_internet",
    "wap_push_ota_app_mms",
    "wap_push_ota_bookmarks",
    "wap_push_ota_multi_shot",
    "wap_push_ota_settings",
    "wap_push_ota_single_shot",
    "wap_push_ota_support",
    "weight",
    "width",
}


class SubscriberTACs(SubscriberFeature):
    """
    Class representing all the TACs for which a subscriber has been associated.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    table : str, default 'all'
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.


    Examples
    -------------

    >>> subscriber_tacs = SubscriberTACs('2016-01-01 13:30:30',
                               '2016-01-02 16:25:00')
    >>> subscriber_tacs.head()
                subscriber                      time         tac
    0     1vGR8kp342yxEpwY 2016-01-01 13:31:06+00:00  85151913.0
    1     QPdr2B94VaEzZgoW 2016-01-01 13:31:06+00:00  15314569.0
    2     LjDxeZEREElG7m0r 2016-01-01 13:32:21+00:00  92380772.0
                            .
                            .
                            .
    """

    def __init__(
        self,
        start,
        stop,
        *,
        hours="all",
        table="all",
        subscriber_subset=None,
        subscriber_identifier="msisdn",
    ):
        """

        """

        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.hours = hours
        self.table = table
        self.subscriber_identifier = subscriber_identifier
        self.tbl = EventsTablesUnion(
            start,
            stop,
            columns=[subscriber_identifier, "tac", "datetime"],
            tables=table,
            hours=hours,
            subscriber_subset=subscriber_subset,
            subscriber_identifier=self.subscriber_identifier,
        )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "time", "tac"]

    def _make_query(self):
        return f"""
                SELECT subscriber, datetime AS time, tac
                FROM ({self.tbl.get_query()}) e
                WHERE tac IS NOT NULL ORDER BY datetime"""


class SubscriberTAC(SubscriberFeature):
    """
    Class representing a single TAC associated to the subscriber.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    table : str, default 'all'
    subscriber_identifier : str, default 'msisdn'
        The focus of the analysis, usually either
        'msisdn', 'imei'
    method : {'most-common', 'last'}
        Method for choosing a TAC to associate.


    Examples
    --------

    >>> subscriber_tacs = SubscriberTAC('2016-01-01', '2016-01-07')
    >>> subscriber_tacs.head()
             subscriber         tac
    0  038OVABN11Ak4W5P  18867440.0
    1  09NrjaNNvDanD8pk  21572046.0
    2  0ayZGYEQrqYlKw6g  81963365.0
    3  0DB8zw67E9mZAPK2  92380772.0
    4  0Gl95NRLjW2aw8pW  77510543.0
                            .
                            .
                            .

    Notes
    -----

    Be aware that when using a imei as a subscriber identifier, than one imei
    is always associated to a _single_ TAC.
    """

    def __init__(
        self,
        start,
        stop,
        *,
        hours="all",
        table="all",
        subscriber_subset=None,
        subscriber_identifier="msisdn",
        method="most-common",
    ):
        """

        """

        if subscriber_identifier == "imei":
            warnings.warn("IMEI has a one to one mapping to TAC number.")

        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.hours = hours
        self.table = table
        self.subscriber_identifier = subscriber_identifier
        self.subscriber_tacs = SubscriberTACs(
            start,
            stop,
            hours=hours,
            table=table,
            subscriber_identifier=subscriber_identifier,
            subscriber_subset=subscriber_subset,
        )
        self.method = method
        if self.method not in ("most-common", "last"):
            raise ValueError("{} is not a valid method.".format(method))
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "tac"]

    def _make_query(self):
        if self.method == "most-common":
            query = """
                    SELECT t.subscriber as subscriber, pg_catalog.mode() WITHIN GROUP(ORDER BY tac) as tac
                    FROM ({}) t
                    GROUP BY t.subscriber""".format(
                self.subscriber_tacs.get_query()
            )
        elif self.method == "last":
            query = """
                    SELECT DISTINCT ON(t.subscriber) t.subscriber as subscriber, tac
                    FROM ({}) t 
                    ORDER BY t.subscriber, time DESC
            """.format(
                self.subscriber_tacs.get_query()
            )
        else:
            raise ValueError(
                f"Unsupported method. Valid values are: 'last', 'most-common'"
            )
        return query


class SubscriberHandsets(SubscriberFeature):
    """
    Class representing all the handsets for which a subscriber has been associated.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    table : str, default 'all'
    subscriber_identifier : str, default 'msisdn'
        The focus of the analysis, usually either
        'msisdn', 'imei'


    Examples
    -------------

    >>> subscriber_handsets = SubscriberHandsets('2016-01-01 13:30:30',
                               '2016-01-02 16:25:00')
    >>> subscriber_handsets.get_dataframe()
              tac        subscriber                      time    brand  model width   ...    j2me_midp_20 j2me_midp_21 j2me_cldc_10 j2me_cldc_11 j2me_cldc_20 hnd_type
    0  85151913.0  1vGR8kp342yxEpwY 2016-01-01 13:31:06+00:00     Sony  KQ-99  None   ...            None         None         None         None         None  Feature
    1  15314569.0  QPdr2B94VaEzZgoW 2016-01-01 13:31:06+00:00     Oppo  UT-18  None   ...            None         None         None         None         None    Smart
    2  92380772.0  LjDxeZEREElG7m0r 2016-01-01 13:32:21+00:00     Oppo  GK-00  None   ...            None         None         None         None         None  Feature
    3  99503609.0  Kabe7EppYbMEj6O3 2016-01-01 13:32:21+00:00  Samsung  NN-71  None   ...            None         None         None         None         None    Smart
    4  16875116.0  gWPl7QBD8VnEyX8K 2016-01-01 13:34:41+00:00     Sony  JH-73  None   ...            None         None         None         None         None    Smart

                            .
                            .
                            .
    """

    def __init__(
        self,
        start,
        stop,
        *,
        hours="all",
        table="all",
        subscriber_identifier="msisdn",
        subscriber_subset=None,
    ):
        """

        """

        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.hours = hours
        self.table = table
        self.subscriber_identifier = subscriber_identifier
        self.subscriber_tacs = SubscriberTACs(
            start,
            stop,
            hours=hours,
            table=table,
            subscriber_identifier=subscriber_identifier,
            subscriber_subset=subscriber_subset,
        )
        self.tacs = Table("infrastructure.tacs")
        self.joined = self.subscriber_tacs.join(self.tacs, "tac", "id", how="left")
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.joined.column_names

    def _make_query(self):
        return self.joined.get_query()


class SubscriberHandset(SubscriberFeature):
    """
    Class representing a single handset associated to the subscriber.

    Notes
    -----

    Be aware that when using a imei as a subscriber identifier, than one imei
    is always associated to a _single_ handset.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    table : str, default 'all'
    subscriber_identifier : str, default 'msisdn'
        The focus of the analysis, usually either
        'msisdn', 'imei'
    method : {'most-common', 'last'}
        Method for choosing a handset to associate.


    Examples
    -------------

    >>> subscriber_handsets = SubscriberHandset('2016-01-01', '2016-01-07')
    >>> subscriber_handsets.get_dataframe()
              tac        subscriber  brand  model width height depth   ...    j2me_midp_10 j2me_midp_20 j2me_midp_21 j2me_cldc_10 j2me_cldc_11 j2me_cldc_20 hnd_type
    0  18867440.0  038OVABN11Ak4W5P   Sony  TO-64  None   None  None   ...            None         None         None         None         None         None    Smart
    1  21572046.0  09NrjaNNvDanD8pk  Apple  WS-44  None   None  None   ...            None         None         None         None         None         None  Feature
    2  81963365.0  0ayZGYEQrqYlKw6g   Sony  YG-07  None   None  None   ...            None         None         None         None         None         None    Smart
    3  92380772.0  0DB8zw67E9mZAPK2   Oppo  GK-00  None   None  None   ...            None         None         None         None         None         None  Feature
    4  77510543.0  0Gl95NRLjW2aw8pW   Sony  KZ-03  None   None  None   ...            None         None         None         None         None         None  Feature

                            .
                            .
                            .
    """

    def __init__(
        self,
        start,
        stop,
        *,
        hours="all",
        table="all",
        subscriber_identifier="msisdn",
        method="most-common",
        subscriber_subset=None,
    ):
        """

        """

        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.hours = hours
        self.table = table
        self.subscriber_identifier = subscriber_identifier
        self.subscriber_tac = SubscriberTAC(
            start,
            stop,
            hours=hours,
            table=table,
            subscriber_identifier=subscriber_identifier,
            method=method,
            subscriber_subset=subscriber_subset,
        )
        self.method = method
        self.tacs = Table("infrastructure.tacs")
        self.joined = self.subscriber_tac.join(self.tacs, "tac", "id", how="left")
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.joined.column_names

    def _make_query(self):
        return self.joined.get_query()


class SubscriberHandsetCharacteristic(SubscriberFeature):
    """
    Class extracting a single characteristic from the handset.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    characteristic: {
        "brand",
        "depth",
        "display_colors",
        "display_height",
        "display_type",
        "display_width",
        "hardware_bluetooth",
        "hardware_edge",
        "hardware_gprs",
        "hardware_gps",
        "hardware_umts",
        "hardware_wifi",
        "height",
        "hnd_type",
        "j2me_cldc_10",
        "j2me_cldc_11",
        "j2me_cldc_20",
        "j2me_midp_10",
        "j2me_midp_20",
        "j2me_midp_21",
        "mms_built_in_camera",
        "mms_receiver",
        "model",
        "software_os_name",
        "software_os_vendor",
        "software_os_version",
        "syncml_dm_acc_gprs",
        "syncml_dm_app_bookmark",
        "syncml_dm_app_browser",
        "syncml_dm_app_internet",
        "syncml_dm_app_java",
        "syncml_dm_app_mms",
        "syncml_dm_settings",
        "tac",
        "wap_1_2_1",
        "wap_2_0",
        "wap_push_oma_app_browser",
        "wap_push_oma_app_ims",
        "wap_push_oma_app_internet",
        "wap_push_oma_app_mms",
        "wap_push_oma_app_poc",
        "wap_push_oma_cp_bookmarks",
        "wap_push_oma_settings",
        "wap_push_ota_app_browser",
        "wap_push_ota_app_internet",
        "wap_push_ota_app_mms",
        "wap_push_ota_bookmarks",
        "wap_push_ota_multi_shot",
        "wap_push_ota_settings",
        "wap_push_ota_single_shot",
        "wap_push_ota_support",
        "weight",
        "width",
    }
        The required handset characteristic.
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    table : str, default 'all'
    subscriber_identifier : str, default 'msisdn'
        The focus of the analysis, usually either
        'msisdn', 'imei'
    method : {'most-common', 'last'}
        Method for choosing a handset to associate.


    Examples
    -------------

    >>> subscriber_smart = SubscriberHandsetCharacteristic('2016-01-01', '2016-01-07',
        'hnd_type')
    >>> subscriber_smart.get_dataframe()
             subscriber        value
    0  038OVABN11Ak4W5P        Smart
    1  09NrjaNNvDanD8pk      Feature
    2  0ayZGYEQrqYlKw6g      Feature
    3  0DB8zw67E9mZAPK2      Feature
    4  0Gl95NRLjW2aw8pW      Feature
                            .
                            .
                            .
    Notes
    ----

    For most-common, this query checks whether the subscriber most commonly uses
    a particular type of phone, rather than the type of their most commonly
    used phone.
    """

    def __init__(
        self,
        start,
        stop,
        characteristic,
        hours="all",
        table="all",
        subscriber_identifier="msisdn",
        method="most-common",
        subscriber_subset=None,
    ):
        """

        """

        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.hours = hours
        self.table = table
        self.subscriber_identifier = subscriber_identifier
        self.characteristic = characteristic
        if self.characteristic not in valid_characteristics:
            raise ValueError("{} is not a valid characteristic.".format(characteristic))
        if method == "most-common":
            self.subscriber_handsets = SubscriberHandsets(
                start,
                stop,
                hours=hours,
                table=table,
                subscriber_identifier=subscriber_identifier,
                subscriber_subset=subscriber_subset,
            )
        else:
            self.subscriber_handsets = SubscriberHandset(
                start,
                stop,
                hours=hours,
                table=table,
                subscriber_identifier=subscriber_identifier,
                subscriber_subset=subscriber_subset,
                method=method,
            )
        self.method = method
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):
        if self.method == "most-common":
            query = f"""
                    SELECT t.subscriber as subscriber, pg_catalog.mode() WITHIN GROUP(ORDER BY t.{self.characteristic}) as value
                    FROM ({self.subscriber_handsets.get_query()}) t
                    GROUP BY t.subscriber"""
        elif self.method == "last":
            query = f"""
            SELECT t.subscriber as subscriber, t.{self.characteristic} as value
            FROM ({self.subscriber_handsets.get_query()}) t
            """
        return query
