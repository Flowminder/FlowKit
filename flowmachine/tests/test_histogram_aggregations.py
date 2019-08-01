# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core import make_spatial_unit
from flowmachine.features import (
    ModalLocation,
    daily_location,
    SubscriberHandsetCharacteristic,
)
import numpy as np

from flowmachine.features import Displacement, RadiusOfGyration, daily_location
from flowmachine.features.utilities.histogram_aggregations import HistogramAggregation
from flowmachine.features.subscriber.daily_location import locate_subscribers
from flowmachine.utils import list_of_dates


def test_can_be_aggregated_admin3_distribution  (get_dataframe):
    """
    
    """
    RoG = RadiusOfGyration("2016-01-01", "2016-01-02")
    
    agg = HistogramAggregation(locations=RoG, bins=[5, 20, 50, 70, 80, 100])
    df = get_dataframe(agg)
    de = np.histogram(get_dataframe(RoG).value, bins=[5,20,50,70,80,100])
    print("==================================")
    print(df)
    # for x in de:
    #   print(x)
    print("==================================")
    
