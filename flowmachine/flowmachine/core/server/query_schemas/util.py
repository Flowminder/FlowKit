# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import Dict, Type
from collections import ChainMap

import pkg_resources


def get_type_schemas_from_entrypoint(entry_point_name: str) -> Dict[str, Type]:
    return dict(
        ChainMap(
            *[
                entry_point.load()
                for entry_point in pkg_resources.iter_entry_points("flowkit.queries")
                if entry_point.name == entry_point_name
            ]
        )
    )
