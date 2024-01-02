import os

import ckan.plugins.toolkit as toolkit


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


def get_rewrite_url():
    if os.getenv('CKANEXT__XLOADER__REWRITE_SITE_URL'):
        return os.getenv('CKANEXT__XLOADER__REWRITE_SITE_URL')
    return toolkit.config.get('ckanext.xloader.rewrite_site_url')