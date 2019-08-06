# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, validates_schema, ValidationError, post_load
from marshmallow.validate import OneOf, Range
from marshmallow_oneofschema import OneOfSchema

from flowmachine.core.random import random_factory


class BaseRandomSampleSchema(Schema):
    size = fields.Integer(validate=Range(min=1))
    fraction = fields.Float(
        validate=Range(0.0, 1.0, min_inclusive=False, max_inclusive=False)
    )
    estimate_count = fields.Boolean()

    @validates_schema
    def validate_size_or_fraction(self, data, **kwargs):
        if ("size" in data) == ("fraction" in data):
            raise ValidationError(
                "Must provide exactly one of 'size' or 'fraction' for a random sample"
            )


class SystemRowsRandomSampleSchema(BaseRandomSampleSchema):
    @post_load
    def make_random_sampler(self, params, **kwargs):
        return RandomSampler(method="system_rows", **params)


class SystemRandomSampleSchema(BaseRandomSampleSchema):
    seed = fields.Integer()

    @post_load
    def make_random_sampler(self, params, **kwargs):
        return RandomSampler(method="system", **params)


class BernoulliRandomSampleSchema(BaseRandomSampleSchema):
    seed = fields.Integer()

    @post_load
    def make_random_sampler(self, params, **kwargs):
        return RandomSampler(method="bernoulli", **params)


class RandomIDsRandomSampleSchema(BaseRandomSampleSchema):
    seed = fields.Float(validate=Range(0.0, 1.0))

    @post_load
    def make_random_sampler(self, params, **kwargs):
        return RandomSampler(method="random_ids", **params)


class RandomSampler:
    def __init__(self, *, method, size, fraction, estimate_count, seed=None):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.method = method
        self.size = size
        self.fraction = fraction
        self.estimate_count = estimate_count
        self.seed = seed

    def make_random_sample_object(self, query):
        Random = random_factory(type(query))
        return Random(
            query,
            method=self.method,
            size=self.size,
            fraction=self.fraction,
            estimate_count=self.estimate_count,
            seed=self.seed,
        )


class RandomSampleSchema(OneOfSchema):
    type_field = "method"
    type_schemas = {
        "system_rows": SystemRowsRandomSampleSchema,
        "system": SystemRandomSampleSchema,
        "bernoulli": BernoulliRandomSampleSchema,
        "random_ids": RandomIDsRandomSampleSchema,
    }
