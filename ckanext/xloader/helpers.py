import ckan.plugins.toolkit as toolkit
from ckanext.xloader.utils import XLoaderFormats


def xloader_status(resource_id):
    try:
        return toolkit.get_action('xloader_status')(
            {}, {'resource_id': resource_id})
    except toolkit.ObjectNotFound:
        return {
            'status': 'unknown'
        }


def xloader_status_description(status):
    _ = toolkit._

    if status.get('status'):
        captions = {
            'complete': _('Complete'),
            'pending': _('Pending'),
            'submitting': _('Submitting'),
            'error': _('Error'),
        }

        return captions.get(status['status'], status['status'].capitalize())
    else:
        return _('Not Uploaded Yet')


def is_resource_supported_by_xloader(res_dict, check_access=True):
    is_supported_format = XLoaderFormats.is_it_an_xloader_format(res_dict.get('format'))
    is_datastore_active = res_dict.get('datastore_active', False)
    user_has_access = not check_access or toolkit.h.check_access(
        'package_update', {'id': res_dict.get('package_id')})
    url_type = res_dict.get('url_type')
    if url_type:
        try:
            is_supported_url_type = url_type not in toolkit.h.datastore_rw_resource_url_types()
        except AttributeError:
            is_supported_url_type = (url_type == 'upload')
    else:
        is_supported_url_type = True
    return (is_supported_format or is_datastore_active) and user_has_access and is_supported_url_type
