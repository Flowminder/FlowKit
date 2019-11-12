# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Prefect tasks used in workflows.
"""

from prefect import task
from typing import Any, Dict


@task
def mappable_dict(**kwargs) -> Dict[str, Any]:
    """
    Task that returns keyword arguments as a dict.
    Equivalent to passing dict(**kwargs) within a Flow context,
    except that this is a prefect task so it can be mapped.
    """
    return kwargs
