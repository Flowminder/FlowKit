# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf
from collections import namedtuple

from flowmachine.core.server.action_handlers import ACTION_HANDLERS


class ActionRequest(Schema):
    """
    Marshmallow schema for incoming ZMQ requests.

    Deserialises a ZeroMQ JSON message into a namedtuple
    """

    action = fields.String(required=True, validate=OneOf(ACTION_HANDLERS.keys()))
    request_id = fields.String(required=True)
    params = fields.Dict(required=False, missing={})

    @post_load
    def make_action(self, data, **kwargs):
        return namedtuple("Action", self.fields.keys())(**data)
