import ckan.plugins as p
import ckanext.xloader.utils as utils


class ResourceDataController(p.toolkit.BaseController):
    def resource_data(self, id, resource_id):
        return utils.resource_data(id, resource_id)
