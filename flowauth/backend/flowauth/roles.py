# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flask import Blueprint, jsonify, request

from flask_login import login_required
from flask_principal import Permission, RoleNeed
