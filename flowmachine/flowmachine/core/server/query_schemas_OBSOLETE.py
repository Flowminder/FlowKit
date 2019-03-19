from copy import deepcopy
from abc import ABCMeta, abstractmethod
from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf, Length, Range
from marshmallow_oneofschema import OneOfSchema

from flowmachine.core import Query


###############################
#                             #
#  Flowmachine query schemas  #
#                             #
###############################


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


####################################################
#                                                  #
#  Flowmachine exposed query objects               #
#                                                  #
#  These are constructed using the schemas above.  #
#                                                  #
####################################################
