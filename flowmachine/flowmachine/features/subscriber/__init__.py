# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
This is the appropriate place for any query that computes something about
subscribers, other than a location. For example percentage nocturnal calls,
total events or radius of gyration.

"""
from .call_days import CallDays
from .new_subscribers import NewSubscribers
from .modal_location import ModalLocation
from .first_location import FirstLocation
from .entropy import PeriodicEntropy, LocationEntropy, ContactEntropy
from .daily_location import daily_location
from .nocturnal_calls import NocturnalCalls
from .location_visits import LocationVisits
from .day_trajectories import DayTrajectories
from .radius_of_gyration import RadiusOfGyration
from .displacement import Displacement
from .total_subscriber_events import TotalSubscriberEvents
from .subscriber_degree import SubscriberDegree, SubscriberInDegree, SubscriberOutDegree
from .subscriber_location_cluster import subscriber_location_cluster, HartiganCluster
from .most_frequent_location import MostFrequentLocation
from .last_location import LastLocation

from .proportion_outgoing import ProportionOutgoing
from .unique_location_counts import UniqueLocationCounts
from .total_active_periods import TotalActivePeriodsSubscriber
from .contact_balance import ContactBalance
from .scores import EventScore, LabelEventScore

from .pareto_interactions import ParetoInteractions

from .subscriber_tacs import (
    SubscriberTAC,
    SubscriberTACs,
    SubscriberHandsets,
    SubscriberHandset,
    SubscriberPhoneType,
)

from .subscriber_call_durations import (
    SubscriberCallDurations,
    PairedSubscriberCallDurations,
    PerLocationSubscriberCallDurations,
    PairedPerLocationSubscriberCallDurations,
)


__all__ = [
    "RadiusOfGyration",
    "NocturnalCalls",
    "TotalSubscriberEvents",
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
    "SubscriberInDegree",
    "SubscriberOutDegree",
    "ProportionOutgoing",
    "TotalActivePeriodsSubscriber",
    "ContactBalance",
    "EventScore",
    "LabelEventScore",
    "SubscriberTACs",
    "SubscriberTAC",
    "SubscriberHandsets",
    "SubscriberHandset",
    "SubscriberPhoneType",
    "ParetoInteractions",
    "SubscriberCallDurations",
    "PairedSubscriberCallDurations",
    "PerLocationSubscriberCallDurations",
    "PairedPerLocationSubscriberCallDurations",
    "Displacement",
    "MostFrequentLocation",
    "LastLocation",
    "PeriodicEntropy",
    "LocationEntropy",
    "ContactEntropy",
]
