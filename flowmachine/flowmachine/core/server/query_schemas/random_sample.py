# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, validates_schema, ValidationError, post_load
from marshmallow.validate import OneOf, Range
from marshmallow_oneofschema import OneOfSchema

__all__ = ["RandomSampleSchema", "RandomSampler"]


class BaseRandomSampleSchema(Schema):
    size = fields.Integer(validate=Range(min=1))
    fraction = fields.Float(
        validate=Range(0.0, 1.0, min_inclusive=False, max_inclusive=False)
    )
    estimate_count = fields.Boolean(missing=True)

    @validates_schema
    def validate_size_or_fraction(self, data, **kwargs):
        if ("size" in data) == ("fraction" in data):
            raise ValidationError(
                "Must provide exactly one of 'size' or 'fraction' for a random sample."
            )


class SystemRowsRandomSampleSchema(BaseRandomSampleSchema):
    # We must define the sampling_method field here for it to appear in the API spec.
    # This field is removed by RandomSampleSchema before passing on to this schema,
    # so the sampling_method parameter is never received here and is not included in the
    # params passed to make_random_sampler.
    sampling_method = fields.String(validate=OneOf(["system_rows"]))

    @post_load
    def make_random_sampler(self, params, **kwargs):
        return RandomSampler(sampling_method="system_rows", **params)


class SystemRandomSampleSchema(BaseRandomSampleSchema):
    # We must define the sampling_method field here for it to appear in the API spec.
    # This field is removed by RandomSampleSchema before passing on to this schema,
    # so the sampling_method parameter is never received here and is not included in the
    # params passed to make_random_sampler.
    sampling_method = fields.String(validate=OneOf(["system"]))
    seed = fields.Float()

    @post_load
    def make_random_sampler(self, params, **kwargs):
        return RandomSampler(sampling_method="system", **params)


class BernoulliRandomSampleSchema(BaseRandomSampleSchema):
    # We must define the sampling_method field here for it to appear in the API spec.
    # This field is removed by RandomSampleSchema before passing on to this schema,
    # so the sampling_method parameter is never received here and is not included in the
    # params passed to make_random_sampler.
    sampling_method = fields.String(validate=OneOf(["bernoulli"]))
    seed = fields.Float()

    @post_load
    def make_random_sampler(self, params, **kwargs):
        return RandomSampler(sampling_method="bernoulli", **params)


class RandomIDsRandomSampleSchema(BaseRandomSampleSchema):
    # We must define the sampling_method field here for it to appear in the API spec.
    # This field is removed by RandomSampleSchema before passing on to this schema,
    # so the sampling_method parameter is never received here and is not included in the
    # params passed to make_random_sampler.
    sampling_method = fields.String(validate=OneOf(["random_ids"]))
    seed = fields.Float(validate=Range(-1.0, 1.0))

    @post_load
    def make_random_sampler(self, params, **kwargs):
        return RandomSampler(sampling_method="random_ids", **params)


class RandomSampler:
    def __init__(
        self, *, sampling_method, size=None, fraction=None, estimate_count, seed=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.sampling_method = sampling_method
        self.size = size
        self.fraction = fraction
        self.estimate_count = estimate_count
        if sampling_method != "system_rows":
            self.seed = seed

    def make_random_sample_object(self, query):
        sample_params = {
            "sampling_method": self.sampling_method,
            "size": self.size,
            "fraction": self.fraction,
            "estimate_count": self.estimate_count,
        }
        if self.sampling_method != "system_rows":
            sample_params["seed"] = self.seed
        return query.random_sample(**sample_params)


class RandomSampleSchema(OneOfSchema):
    type_field = "sampling_method"
    type_schemas = {
        "system_rows": SystemRowsRandomSampleSchema,
        "system": SystemRandomSampleSchema,
        "bernoulli": BernoulliRandomSampleSchema,
        "random_ids": RandomIDsRandomSampleSchema,
    }
