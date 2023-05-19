from flask import Blueprint, request

import ckanext.xloader.utils as utils


xloader = Blueprint("xloader", __name__)


def get_blueprints():
    return [xloader]


@xloader.route("/dataset/<id>/resource_data/<resource_id>", methods=("GET", "POST"))
def resource_data(id, resource_id):
    rows = request.args.get('rows')
    if rows:
        try:
            rows = int(rows)
        except ValueError:
            rows = None
    return utils.resource_data(id, resource_id, rows)
