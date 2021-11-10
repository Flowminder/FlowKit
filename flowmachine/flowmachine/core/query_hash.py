# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
from collections import Iterable

from hashlib import md5
import rapidjson as json


def hash_query(query: "Query"):
    dependencies = query.dependencies
    state = query.__getstate__()
    hashes = sorted([x.query_id for x in dependencies])
    for key, item in sorted(state.items()):
        try:
            item_q_id = item.query_id
            if item not in dependencies:
                hashes.append(item_q_id)
        except AttributeError:
            if isinstance(item, (list, tuple, set)):
                item = sorted(
                    item,
                    key=lambda x: x.query_id if hasattr(x, "query_id") else str(x),
                )
            elif isinstance(item, dict):
                # Transform any queries to query ids
                item_dict = dict()
                for key, val in item.items():
                    try:
                        key = key.query_id
                    except AttributeError:
                        pass
                    try:
                        val = val.query_id
                    except AttributeError:
                        pass
                    item_dict[key] = val
                item = json.dumps(item_dict, sort_keys=True, default=str)
            else:
                # if it's not a list or a dict we leave the item as it is
                pass

            hashes.append(str(item))
    hashes.append(query.__class__.__name__)
    hashes.sort()
    return md5(str(hashes).encode()).hexdigest()


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
