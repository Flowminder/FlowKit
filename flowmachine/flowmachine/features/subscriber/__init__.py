# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
This is the appropriate place for any query that computes something about
subscribers, other than a location. For example percentage nocturnal calls,
total events or radius of gyration.

"""
from .active_at_reference_location import ActiveAtReferenceLocation
from .call_days import CallDays
from .contact_balance import ContactBalance
from .contact_reciprocal import (
    ContactReciprocal,
    ProportionContactReciprocal,
    ProportionEventReciprocal,
)
from .contact_reference_locations_stats import ContactReferenceLocationStats
from .daily_location import daily_location
from .day_trajectories import DayTrajectories
from .displacement import Displacement
from .distance_counterparts import DistanceCounterparts
from .distance_series import DistanceSeries
from .entropy import ContactEntropy, LocationEntropy, PeriodicEntropy
from .event_count import EventCount
from .event_type_proportion import ProportionEventType
from .first_location import FirstLocation
from .handset_stats import HandsetStats
from .hartigan_cluster import HartiganCluster
from .imputed_distance_series import ImputedDistanceSeries
from .interevent_interval import IntereventInterval
from .interevent_period import IntereventPeriod
from .iterative_median_filter import IterativeMedianFilter
from .label_event_score import LabelEventScore
from .last_location import LastLocation
from .location_visits import LocationVisits
from .mds_volume import MDSVolume
from .meaningful_locations import MeaningfulLocations
from .modal_location import ModalLocation
from .most_frequent_location import MostFrequentLocation
from .new_subscribers import NewSubscribers
from .nocturnal_events import NocturnalEvents
from .pareto_interactions import ParetoInteractions
from .per_contact_event_stats import PerContactEventStats
from .per_location_event_stats import PerLocationEventStats
from .radius_of_gyration import RadiusOfGyration
from .scores import EventScore
from .subscriber_call_durations import (
    PairedPerLocationSubscriberCallDurations,
    PairedSubscriberCallDurations,
    PerLocationSubscriberCallDurations,
    SubscriberCallDurations,
)
from .subscriber_degree import SubscriberDegree
from .subscriber_tacs import (
    SubscriberHandset,
    SubscriberHandsetCharacteristic,
    SubscriberHandsets,
    SubscriberTAC,
    SubscriberTACs,
)
from .topup_amount import TopUpAmount
from .topup_balance import TopUpBalance
from .total_active_periods import TotalActivePeriodsSubscriber
from .unique_location_counts import UniqueLocationCounts
from .unique_locations import UniqueLocations
from .visited_most_days import VisitedMostDays
