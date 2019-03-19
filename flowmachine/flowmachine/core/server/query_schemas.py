from copy import deepcopy
from abc import ABCMeta, abstractmethod
from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf, Length, Range
from marshmallow_oneofschema import OneOfSchema

from flowmachine.core import Query
from flowmachine.core.dummy_query import DummyQuery
from flowmachine.core.query_info_lookup import QueryInfoLookup


###############################
#                             #
#  Flowmachine query schemas  #
#                             #
###############################


class DummyQuerySchema(Schema):
    """
    Dummy query useful for testing.
    """

    dummy_param = fields.String(required=True)

    @post_load
    def make_query_object(self, params):
        return DummyQueryExposed(**params)


class DailyLocationSchema(Schema):
    date = fields.Date(required=True)
    method = fields.String(required=True, validate=OneOf(["last", "most-common"]))
    aggregation_unit = fields.String(
        required=True, validate=OneOf(["admin0", "admin1", "admin2", "admin3"])
    )
    subscriber_subset = fields.String(
        required=False, allow_none=True, validate=OneOf([None])
    )

    @post_load
    def make_query_object(self, params):
        return DailyLocationExposed(**params)


class InputToModalLocationSchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {"daily_location": DailyLocationSchema}

    def get_obj_type(self, obj):
        if isinstance(obj, DailyLocationExposed):
            return "daily_location"
        else:
            raise Exception("Unknown object type: {obj.__class__.__name__}")


class ModalLocationSchema(Schema):
    locations = fields.Nested(
        InputToModalLocationSchema, many=True, validate=Length(min=1)
    )
    aggregation_unit = fields.String(
        validate=OneOf(["admin0", "admin1", "admin2", "admin3"])
    )
    subscriber_subset = fields.String(allow_none=True, validate=OneOf([None]))

    @post_load
    def make_query_object(self, data):
        return ModalLocationExposed(**data)


class TowerDayOfWeekScore(fields.Dict):
    """
    Field that serializes to a title case string and deserializes
    to a lower case string.
    """

    def _deserialize(self, value, attr, data, **kwargs):
        ttt = super()._deserialize(value, attr, data, **kwargs)
        breakpoint()
        pass

    def _serialize(self, value, attr, obj, **kwargs):
        if value is None:
            return ""
        return value.title()

    def _deserialize(self, value, attr, data, **kwargs):
        return value.lower()


class MeaningfulLocationsSchema(Schema):
    start_date = fields.Date(required=True)
    stop_date = fields.Date(required=True)
    aggregation_unit = fields.String(
        validate=OneOf(["admin0", "admin1", "admin2", "admin3"])
    )
    label = fields.String(required=True)
    labels = fields.Dict(
        keys=fields.String(), values=fields.Dict()
    )  # TODO: use custom field here for stricter validation!
    tower_hour_of_day_scores = fields.List(
        fields.Float(validate=Range(min=-1.0, max=1.0))
    )
    tower_day_of_week_scores = fields.Dict(
        keys=fields.String(
            validate=OneOf(
                [
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                    "sunday",
                ]
            )
        ),
        values=fields.Float(validate=Range(min=-1.0, max=1.0)),
    )
    tower_cluster_radius = fields.Float()


class MeaningfulLocationsAggregateSchema(Schema):
    meaningful_locations = fields.Nested(MeaningfulLocationsSchema)
    aggregation_unit = fields.String(
        validate=OneOf(["admin0", "admin1", "admin2", "admin3"])
    )


class FlowmachineQuerySchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {
        "dummy_query": DummyQuerySchema,
        "daily_location": DailyLocationSchema,
        "modal_location": ModalLocationSchema,
        "meaningful_locations_aggregate": MeaningfulLocationsAggregateSchema,
    }

    def get_obj_type(self, obj):
        if isinstance(obj, DummyQueryExposed):
            return "dummy_query"
        elif isinstance(obj, DailyLocationExposed):
            return "daily_location"
        elif isinstance(obj, ModalLocationExposed):
            return "modal_location"
        elif isinstance(obj, MeaningfulLocationsAggregateExposed):
            return "meaningful_locations_aggregate"
        else:
            raise ValueError(
                f"Object type '{obj.__class__.__name__}' not registered in FlowmachineQuerySchema."
            )


####################################################
#                                                  #
#  Flowmachine exposed query objects               #
#                                                  #
#  These are constructed using the schemas above.  #
#                                                  #
####################################################


class BaseExposedQuery(metaclass=ABCMeta):
    """
    Base class for exposed flowmachine queries.

    Note: these classes are not meant to be instantiated directly!
    Instead, they are instantiated automatically through the class
    FlowmachineQuerySchema above. Example:

        FlowmachineQuerySchema().load({"query_kind": "dummy_query", "dummy_param": "foobar"})
    """

    def __init__(self):
        # TODO: we rely on the fact that subclasses call super().__init__() at
        # the end of their own __init__() methods. This is potentially error-prone
        # if users forget to do this when adding their own query classes. We should
        # add a simple metaclass for BaseExposedQuery which ensures that this is done
        # automatically.
        self._create_query_info_lookup()

    @property
    @abstractmethod
    def __schema__(self):
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not have the __schema__ property set."
        )

    @property
    @abstractmethod
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine query object which this class exposes.

        Returns
        -------
        Query
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not have the fm_query_obj property set."
        )

    def store_async(self):
        """
        Store this query using a background thread.

        Returns
        -------
        Future
            Future object representing the calculation.

        """
        q = self._flowmachine_query_obj
        q.store()

        # FIXME: currently we also aggregate the query here, but this is not
        # the place to do this. Instead, there should be a separate query_kind
        # to perform the spatial aggregation. Once we have this, the following
        # code block should be removed and simply `q.md5` be returned as the
        # query_id.
        try:
            # In addition to the actual query, also set an aggregated version running.
            # This is the one which we return via the API so that we don't expose any
            # individual-level data.
            q_agg = q.aggregate()
            q_agg.store()
            query_id = q_agg.md5
            # FIXME: this is a big hack to register the query info lookup also
            # for the aggregated query. This should not be necessary any more once
            # this whole code block is removed (see fixme comment above). To make
            # it obvious that it is the lookup of an aggregated query we artificially
            # insert two additional keys 'is_aggregate' and 'non_aggregated_query_id'.
            q_agg_info_lookup = QueryInfoLookup(Query.redis)
            query_params_agg = deepcopy(self.query_params)
            query_params_agg["is_aggregate"] = True
            query_params_agg["non_aggregated_query_id"] = self.query_id
            q_agg_info_lookup.register_query(q_agg.md5, self.query_params)
        except AttributeError:
            # This can happen for flows, which doesn't support aggregation
            query_id = q.md5

        return query_id

    @property
    def query_id(self):
        # TODO: Ideally we'd like to return the md5 hash of the query parameters
        # as known to the marshmallow schema:
        #    return md5(json.dumps(self.query_params, sort_keys=True).encode()).hexdigest()
        #
        # However, the resulting md5 hash is different from the one produced internally
        # by flowmachine.core.Query.md5, and the latter is currently being used by
        # the QueryStateMachine, so we need to use it to check the query state.
        return self._flowmachine_query_obj.md5

    @property
    def query_params(self):
        """
        Return the parameters from which the query is constructed. Note that this
        includes the the parameters of any subqueries of which it is composed.

        Returns
        -------
        dict
            JSON representation of the query parameters, including those of subqueries.
        """
        # marshmallow_schema = self.__schema__()
        # return marshmallow_schema.dump(self)
        # return FlowmachineQuerySchema().load({"query_kind": "dummy_query", "dummy_param": "foobar"})
        return FlowmachineQuerySchema().dump(self)

    def _create_query_info_lookup(self):
        """
        Create the query info lookup between query_id and query parameters, so that
        we can get information about the query later using only its query_id.
        """
        # TODO: is this the right place for this or should this happen somewhere else?!
        q_info_lookup = QueryInfoLookup(Query.redis)
        q_info_lookup.register_query(self.query_id, self.query_params)


class DummyQueryExposed(BaseExposedQuery):

    __schema__ = DummyQuerySchema

    def __init__(self, dummy_param):
        self.dummy_param = dummy_param
        super().__init__()  # NOTE: this *must* be called at the end of the __init__() method of any subclass of BaseExposedQuery

    @property
    def _flowmachine_query_obj(self):
        return DummyQuery(dummy_param=self.dummy_param)


class DailyLocationExposed(BaseExposedQuery):

    __schema__ = DailyLocationSchema

    def __init__(self, date, *, method, aggregation_unit, subscriber_subset=None):
        self.date = date
        self.method = method
        self.aggregation_unit = aggregation_unit
        self.subscriber_subset = subscriber_subset
        super().__init__()  # NOTE: this *must* be called at the end of the __init__() method of any subclass of BaseExposedQuery

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine daily_location object.

        Returns
        -------
        Query
        """
        from flowmachine.features import daily_location

        return daily_location(
            date=self.date,
            level=self.aggregation_unit,
            method=self.method,
            subscriber_subset=self.subscriber_subset,
        )


class ModalLocationExposed(BaseExposedQuery):

    __schema__ = ModalLocationSchema

    def __init__(self, locations, *, aggregation_unit, subscriber_subset=None):
        self.locations = locations
        self.aggregation_unit = aggregation_unit
        self.subscriber_subset = subscriber_subset
        super().__init__()  # NOTE: this *must* be called at the end of the __init__() method of any subclass of BaseExposedQuery

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine ModalLocation object.

        Returns
        -------
        ModalLocation
        """
        from flowmachine.features import ModalLocation

        locations = [loc._flowmachine_query_obj for loc in self.locations]
        return ModalLocation(*locations)


class MeaningfulLocationsAggregateExposed(BaseExposedQuery):

    __schema__ = MeaningfulLocationsAggregateSchema

    def __init__(self, *, meaningful_locations, aggregation_unit):
        self.meaningful_locations = meaningful_locations
        self.aggregation_unit = aggregation_unit
        super().__init__()  # NOTE: this *must* be called at the end of the __init__() method of any subclass of BaseExposedQuery

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine MeaningfulLocationsAggregate object.

        Returns
        -------
        ModalLocation
        """
        from flowmachine.features import MeaningfulLocationsAggregate

        meaningful_locations = self.meaningful_locations._flowmachine_query_obj
        return MeaningfulLocationsAggregate(
            meaningful_locations=meaningful_locations, level=self.aggregation_unit
        )
