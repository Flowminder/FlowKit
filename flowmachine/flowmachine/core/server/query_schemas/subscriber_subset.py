# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# This file contains a custom definition for the subscriber_subset field for use
# by the flowmachine query schemas.
import typing

from typing import Union

from marshmallow import ValidationError, fields
from marshmallow.validate import Validator
from marshmallow.utils import missing

from flowmachine.core.cache import cache_table_exists, get_query_object_by_id
from flowmachine.core.context import get_redis, get_db
from flowmachine.core.query_info_lookup import QueryInfoLookup, UnkownQueryIdError
from flowmachine.core.table import Table


class NoneOrQuery(Validator):
    """Validator which fails if ``value`` is not the id of a real query, or is not None`."""

    def __call__(self, value) -> Union[None, str]:
        from flowmachine.core.server.query_schemas import FlowmachineQuerySchema

        if (value is not None) and (value is not missing):
            try:
                (
                    FlowmachineQuerySchema()
                    .load(QueryInfoLookup(get_redis()).get_query_params(value))
                    ._flowmachine_query_obj
                )
            except UnkownQueryIdError:
                if not cache_table_exists(get_db(), value):
                    raise ValidationError("Must be None or a valid query id.")

        return value


class SubscriberSubset(fields.String):
    """
    Represents a subscriber subset. This can either be a string representing
    a query_id or `None`, meaning "all subscribers".
    """

    def __init__(self, required=False, allow_none=True, validate=None, **kwargs):
        if validate is not None:
            raise ValueError(
                "The SubscriberSubset field provides its own validation "
                "and thus does not accept a the 'validate' argument."
            )

        super().__init__(
            required=required, allow_none=allow_none, validate=NoneOrQuery(), **kwargs
        )

    def deserialize(
        self,
        value: typing.Any,
        attr: str = None,
        data: typing.Mapping[str, typing.Any] = None,
        **kwargs,
    ) -> Union[None, Table]:
        from flowmachine.core.server.query_schemas import FlowmachineQuerySchema

        table_name = super().deserialize(value, attr, data, **kwargs)
        if (table_name is missing) or (table_name is None):
            return table_name
        else:
            try:
                return (
                    FlowmachineQuerySchema()
                    .load(QueryInfoLookup(get_redis()).get_query_params(value))
                    ._flowmachine_query_obj
                )
            except UnkownQueryIdError:
                return get_query_object_by_id(get_db(), value)
