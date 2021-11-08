# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Features (i.e. co-variates, indicators, metrics) are 
used as measurements of a phenomenon of interest.
This section of `flowmachine` contains code that calculates
a series of features.

"""
from .location import *
from .network import *
from .subscriber import *

from .raster import *
from .spatial import *

from .utilities import *

loc = [
    "TotalLocationEvents",
    "Flows",
    "UniqueSubscriberCounts",
    "LocationIntroversion",
    "MeaningfulLocationsAggregate",
    "MeaningfulLocationsOD",
]
nw = ["TotalNetworkObjects", "AggregateNetworkObjects"]
subs = [
    "RadiusOfGyration",
    "NocturnalEvents",
    "FirstLocation",
    "CallDays",
    "ModalLocation",
    "daily_location",
    "DayTrajectories",
    "LocationVisits",
    "NewSubscribers",
    "subscriber_location_cluster",
    "HartiganCluster",
    "UniqueLocationCounts",
    "SubscriberDegree",
    "TotalActivePeriodsSubscriber",
    "ContactBalance",
    "EventScore",
    "LabelEventScore",
    "SubscriberTACs",
    "SubscriberTAC",
    "SubscriberHandsets",
    "SubscriberHandset",
    "SubscriberHandsetCharacteristic",
    "ParetoInteractions",
    "SubscriberCallDurations",
    "PairedSubscriberCallDurations",
    "PerLocationSubscriberCallDurations",
    "PairedPerLocationSubscriberCallDurations",
    "MostFrequentLocation",
    "LastLocation",
    "PeriodicEntropy",
    "LocationEntropy",
    "ContactEntropy",
    "EventCount",
    "PerLocationEventStats",
    "PerContactEventStats",
    "MeaningfulLocations",
    "ProportionEventType",
    "PeriodicEntropy",
    "LocationEntropy",
    "ContactEntropy",
    "DistanceCounterparts",
    "ContactReferenceLocationStats",
    "IntereventPeriod",
    "TopUpAmount",
    "TopUpBalance",
    "MDSVolume",
    "ContactReciprocal",
    "ProportionContactReciprocal",
    "ProportionEventReciprocal",
    "HandsetStats",
    "VisitedMostDays",
]

rast = ["RasterStatistics"]
spat = [
    "LocationArea",
    "LocationCluster",
    "DistanceMatrix",
    "Geography",
    "VersionedInfrastructure",
    "Grid",
    "Circle",
    "CircleGeometries",
]

ut = [
    "GroupValues",
    "feature_collection",
    "SubscriberLocations",
    "EventTableSubset",
    "UniqueSubscribers",
    "EventsTablesUnion",
    "EventTableSubset",
]

sub_modules = ["location", "subscriber", "network", "utilities", "raster", "spatial"]

__all__ = loc + nw + subs + rast + ut + spat + sub_modules
