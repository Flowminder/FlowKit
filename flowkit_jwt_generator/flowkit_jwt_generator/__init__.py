# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from .jwt import (
    generate_token,
    get_all_claims_from_flowapi,
    generate_keypair,
    load_private_key,
    load_public_key,
)

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

from . import _version

__version__ = _version.get_versions()["version"]
