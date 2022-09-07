# encoding: utf-8

import json

from ckan import model
from ckan.lib import search
import ckan.plugins as p


def resource_data(id, resource_id):

    if p.toolkit.request.method == "POST":
        try:
            p.toolkit.get_action("xloader_submit")(
                None,
                {
                    "resource_id": resource_id,
                    "ignore_hash": True,  # user clicked the reload button
                },
            )
        except p.toolkit.ValidationError:
            pass

        return p.toolkit.redirect_to(
            "xloader.resource_data", id=id, resource_id=resource_id
        )

    try:
        pkg_dict = p.toolkit.get_action("package_show")(None, {"id": id})
        resource = p.toolkit.get_action("resource_show")(None, {"id": resource_id})
    except (p.toolkit.ObjectNotFound, p.toolkit.NotAuthorized):
        return p.toolkit.abort(404, p.toolkit._("Resource not found"))

    try:
        xloader_status = p.toolkit.get_action("xloader_status")(
            None, {"resource_id": resource_id}
        )
    except p.toolkit.ObjectNotFound:
        xloader_status = {}
    except p.toolkit.NotAuthorized:
        return p.toolkit.abort(403, p.toolkit._("Not authorized to see this page"))

    return p.toolkit.render(
        "xloader/resource_data.html",
        extra_vars={
            "status": xloader_status,
            "resource": resource,
            "pkg_dict": pkg_dict,
        },
    )


def get_xloader_user_apitoken():
    """ Returns the API Token for authentication.

    xloader actions require an authenticated user to perform the actions. This
    method returns the api_token set in the config file and defaults to the
    site_user.
    """
    api_token = p.toolkit.config.get('ckanext.xloader.api_token', None)
    if api_token:
        return api_token

    site_user = p.toolkit.get_action('get_site_user')({'ignore_auth': True}, {})
    return site_user["apikey"]


def set_resource_metadata(update_dict):
    '''
    Set appropriate datastore_active flag on CKAN resource.

    Called after creation or deletion of DataStore table.
    '''
    # We're modifying the resource extra directly here to avoid a
    # race condition, see issue #3245 for details and plan for a
    # better fix

    q = model.Session.query(model.Resource). \
        filter(model.Resource.id == update_dict['resource_id'])
    resource = q.one()

    # update extras in database for record
    extras = resource.extras
    extras.update(update_dict)
    q.update({'extras': extras}, synchronize_session=False)

    # TODO: Remove resource_revision_table when dropping support for 2.8
    if hasattr(model, 'resource_revision_table'):
        model.Session.query(model.resource_revision_table).filter(
            model.ResourceRevision.id == update_dict['resource_id'],
            model.ResourceRevision.current is True
        ).update({'extras': extras}, synchronize_session=False)
    model.Session.commit()

    # get package with updated resource from solr
    # find changed resource, patch it and reindex package
    psi = search.PackageSearchIndex()
    solr_query = search.PackageSearchQuery()
    q = {
        'q': 'id:"{0}"'.format(resource.package_id),
        'fl': 'data_dict',
        'wt': 'json',
        'fq': 'site_id:"%s"' % p.toolkit.config.get('ckan.site_id'),
        'rows': 1
    }
    for record in solr_query.run(q)['results']:
        solr_data_dict = json.loads(record['data_dict'])
        for resource in solr_data_dict['resources']:
            if resource['id'] == update_dict['resource_id']:
                resource.update(update_dict)
                psi.index_package(solr_data_dict)
                break
