# -*- coding: utf-8 -*-

import ckan.plugins as p


class MixinPlugin(p.SingletonPlugin):
    p.implements(p.IRoutes, inherit=True)

    # IRoutes

    def before_map(self, m):
        m.connect(
            'resource_data_xloader', '/dataset/{id}/resource_data/{resource_id}',
            controller='ckanext.xloader.controllers:ResourceDataController',
            action='resource_data', ckan_icon='cloud-upload')
        return m
