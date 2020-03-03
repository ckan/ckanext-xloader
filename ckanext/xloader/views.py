from flask import Blueprint

import ckanext.xloader.utils as utils


xloader = Blueprint("xloader", __name__)


def get_blueprints():
    return [xloader]


@xloader.route("/dataset/<id>/resource_data/<resource_id>", methods=("GET", "POST"))
def resource_data(id, resource_id):
    return utils.resource_data(id, resource_id)
