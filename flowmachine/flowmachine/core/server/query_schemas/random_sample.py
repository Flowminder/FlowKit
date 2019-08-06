# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, validates_schema, ValidationError, post_load
from marshmallow.validate import OneOf, Range

from flowmachine.core.random import random_factory


class RandomSampleSchema(Schema):
    size = fields.Integer(validate=Range(min=1))
    fraction = fields.Float(
        validate=Range(0.0, 1.0, min_inclusive=False, max_inclusive=False)
    )
    method = fields.String(
        validate=OneOf(["system_rows", "system", "bernoulli", "random_ids"]),
        required=True,
    )
    estimate_count = fields.Boolean()
    seed = fields.Float()

    @validates_schema
    def validate_size_or_fraction(self, data, **kwargs):
        if ("size" in data) == ("fraction" in data):
            raise ValidationError(
                "Must provide exactly one of 'size' or 'fraction' for a random sample"
            )

    @validates_schema
    def validate_seed(self, data, **kwargs):
        if data["method"] == "system_rows" and "seed" in data:
            raise ValidationError(
                "'system_rows' sampling method does not support seeding"
            )
        elif data["method"] == "random_ids" and not 0 <= data["seed"] <= 1:
            raise ValidationError(
                "Seed must be between 0 and 1 for 'random_ids' sampling method"
            )

    @post_load
    def make_random_sample_factory(self, params, **kwargs):
        return RandomSampleFactory(**params)


class RandomSampleFactory:
    def __init__(self, *, size, fraction, method, estimate_count, seed):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.size = size
        self.fraction = fraction
        self.method = method
        self.estimate_count = estimate_count
        self.seed = seed

    def make_random_sample_object(self, query):
        Random = random_factory(type(query))
        return Random(
            query,
            size=self.size,
            fraction=self.fraction,
            method=self.method,
            estimate_count=self.estimate_count,
            seed=self.seed,
        )
