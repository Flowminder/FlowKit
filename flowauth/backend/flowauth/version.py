# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flask import jsonify, Blueprint
from flowauth._version import get_versions

blueprint = Blueprint(__name__.split(".").pop(), __name__)


@blueprint.route("/version")
def get_version():
    return jsonify({"version": get_versions()["version"]})
