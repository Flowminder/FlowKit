# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flask import jsonify, Blueprint, request
from flask_login import login_required
from flask_principal import Permission, RoleNeed


from .models import *

blueprint = Blueprint(__name__, __name__)
admin_permission = Permission(RoleNeed("admin"))


@blueprint.route("")
@login_required
def list_all_spatial_aggregation():
    """
    Get a list of all the aggregation units.

    """
    return jsonify(
        [
            {"id": unit.id, "name": unit.name}
            for unit in SpatialAggregationUnit.query.all()
        ]
    )


@blueprint.route("", methods=["POST"])
@login_required
@admin_permission.require(http_exception=401)
def add_aggregation_unit():
    """
    Add a new spatial aggregation unit.

    Notes
    -----
    Expects json with a "name" field.
    Responds with {"name":<aggregation_unit_name>, "id":<aggregation_unit_id>}
    """
    agg_unit = request.get_json()
    agg_unit = SpatialAggregationUnit(**agg_unit)
    db.session.add(agg_unit)
    db.session.commit()
    return jsonify({"name": agg_unit.name, "id": agg_unit.id})


@blueprint.route("/<agg_unit_id>", methods=["DELETE"])
@login_required
@admin_permission.require(http_exception=401)
def rm_aggregation_unit(agg_unit_id):
    """
    Delete a unit of spatial aggregation.
    """
    agg_unit = SpatialAggregationUnit.query.filter(
        SpatialAggregationUnit.id == agg_unit_id
    ).first_or_404()
    db.session.delete(agg_unit)
    db.session.commit()
    return jsonify(), 200
