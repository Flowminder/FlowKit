# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Models implemented at Flowminder should be
added here. Models are mathematical representations
of data that make predictions from that data.

"""
from .louvain import Louvain

__all__ = ["Louvain"]
