# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
from collections import Mapping
import functools

from hashlib import md5
import rapidjson as json


@functools.singledispatch
def to_hashable(val):
    return str(val)


@to_hashable.register(set)
def _(val: set):
    return str(sorted(to_hashable(v) for v in val))


@to_hashable.register
def _(val: Mapping):
    return json.dumps(
        val,
        sort_keys=True,
        default=to_hashable,
        iterable_mode=json.IM_ONLY_LISTS,
        mapping_mode=json.MM_COERCE_KEYS_TO_STRINGS,
    )


def hash_query(query: "Query"):
    dependencies = query.dependencies
    state = query.__getstate__()
    vals = [
        *[x.query_id for x in dependencies],
        *[to_hashable(val) for val in state.values()],
        query.__class__.__name__,
    ]
    return md5(str(sorted(vals)).encode()).hexdigest()


def gen_all_of_type(var, typ):
    if hasattr(var, "items"):
        for k, v in var.items():
            if isinstance(k, typ):
                yield k
            if isinstance(v, typ):
                yield v
            elif isinstance(v, dict):
                yield from gen_all_of_type(v, typ)
            elif isinstance(v, (set, list, tuple)):
                for d in v:
                    yield from gen_all_of_type(d, typ)

    elif isinstance(var, typ):
        yield var
    elif hasattr(var, "__dict__"):
        yield from gen_all_of_type(var.__dict__, typ)
