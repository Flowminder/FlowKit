# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from abc import ABCMeta, abstractmethod
from marshmallow import fields

from .base_exposed_query import BaseExposedQuery
from .base_schema import BaseSchema
from .random_sample import RandomSampleSchema


class BaseQueryWithSamplingSchema(BaseSchema):
    sampling = fields.Nested(RandomSampleSchema, allow_none=True)


class BaseExposedQueryWithSampling(BaseExposedQuery, metaclass=ABCMeta):
    @property
    @abstractmethod
    def _unsampled_query_obj(self):
        """
        Return the flowmachine query object to be sampled.

        Returns
        -------
        Query
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not have the _unsampled_query_obj property set."
        )

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine query object which this class exposes.

        Returns
        -------
        Query
        """
        query = self._unsampled_query_obj
        if self.sampling is None:
            return query
        else:
            return self.sampling.make_random_sample_object(query)
