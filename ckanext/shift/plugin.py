from ckan import model
import ckan.plugins as plugins

from ckanext.shift import action, auth
import ckanext.shift.helpers as shift_helpers
from ckanext.shift.loader import fulltext_function_exists, get_write_engine

log = __import__('logging').getLogger(__name__)
p = plugins


# resource.formats accepted by ckanext-shift. Must be lowercase here.
DEFAULT_FORMATS = [
    'csv', 'application/csv',
    # 'xls', 'xlsx', 'tsv',
    # 'application/vnd.ms-excel',
    # 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    # 'ods', 'application/vnd.oasis.opendocument.spreadsheet',
]


class ShiftPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IConfigurable)
    plugins.implements(plugins.IDomainObjectModification)
    plugins.implements(plugins.IResourceUrlChange)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.IRoutes, inherit=True)
    plugins.implements(plugins.ITemplateHelpers)

    # IConfigurer

    def update_config(self, config):
        templates_base = config.get('ckan.base_templates_folder',
                                    'templates-bs2')  # for ckan < 2.8
        p.toolkit.add_template_directory(config, templates_base)

    # IConfigurable

    def configure(self, config):
        self.config = config

        shift_formats = config.get('ckanext.shift.formats', '').lower()
        self.shift_formats = shift_formats.lower().split() or DEFAULT_FORMATS

        for config_option in ('ckan.site_url',): #'ckanext.shift.url',):
            if not config.get(config_option):
                raise Exception(
                    'Config option `{0}` must be set to use ckanext-shift.'
                    .format(config_option))

        connection = get_write_engine().connect()
        if not fulltext_function_exists(connection):
            raise Exception('populate_full_text_trigger is not defined. See '
                            'ckanext-shift\'s README.rst for more details.')

    # IDomainObjectModification
    # IResourceUrlChange

    def notify(self, entity, operation=None):
        if isinstance(entity, model.Resource):
            if (operation == model.domain_object.DomainObjectOperation.new or
                    not operation):
                # if operation is None, resource URL has been changed, as
                # the notify function in IResourceUrlChange only takes
                # 1 parameter
                context = {'model': model, 'ignore_auth': True,
                           'defer_commit': True}
                if (entity.format and
                        entity.format.lower() in self.shift_formats and
                        entity.url_type not in ('datapusher', 'shift')):

                    # try:
                    #     task = p.toolkit.get_action('task_status_show')(
                    #         context, {
                    #             'entity_id': entity.id,
                    #             'task_type': 'datapusher',
                    #             'key': 'datapusher'}
                    #     )
                    #     if task.get('state') == 'pending':
                    #         # There already is a pending DataPusher submission,
                    #         # skip this one ...
                    #         log.debug(
                    #             'Skipping DataPusher submission for '
                    #             'resource {0}'.format(entity.id))
                    #         return
                    # except p.toolkit.ObjectNotFound:
                    #     pass

                    try:
                        log.debug('Submitting resource {0} to be shifted'
                                  .format(entity.id))
                        p.toolkit.get_action('shift_submit')(context, {
                            'resource_id': entity.id
                        })
                    except p.toolkit.ValidationError, e:
                        # If shift is offline, we want to catch error instead
                        # of raising otherwise resource save will fail with 500
                        log.critical(e)
                        pass

    # IActions

    def get_actions(self):
        return {
            'shift_submit': action.shift_submit,
            'shift_hook': action.shift_hook,
            'shift_status': action.shift_status,
            }

    # IAuthFunctions

    def get_auth_functions(self):
        return {
            'shift_submit': auth.shift_submit,
            'shift_status': auth.shift_status,
            }

    # IRoutes

    def before_map(self, m):
        m.connect(
            'resource_data_shift', '/dataset/{id}/resource_data/{resource_id}',
            controller='ckanext.shift.controllers:ResourceDataController',
            action='resource_data', ckan_icon='cloud-upload')
        return m

    # ITemplateHelpers

    def get_helpers(self):
        return {
            'shift_status': shift_helpers.shift_status,
            'shift_status_description':
            shift_helpers.shift_status_description,
        }
