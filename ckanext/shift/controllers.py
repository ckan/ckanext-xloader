import ckan.plugins as p

_ = p.toolkit._


class ResourceDataController(p.toolkit.BaseController):

    def resource_data(self, id, resource_id):

        if p.toolkit.request.method == 'POST':
            try:
                p.toolkit.c.pkg_dict = \
                    p.toolkit.get_action('shift_submit')(
                        None, {'resource_id': resource_id}
                    )
            except p.toolkit.ValidationError:
                pass

            p.toolkit.redirect_to(
                controller='ckanext.shift.controllers:ResourceDataController',
                action='resource_data',
                id=id,
                resource_id=resource_id
            )

        try:
            p.toolkit.c.pkg_dict = p.toolkit.get_action('package_show')(
                None, {'id': id}
            )
            p.toolkit.c.resource = p.toolkit.get_action('resource_show')(
                None, {'id': resource_id}
            )
        except (p.toolkit.ObjectNotFound, p.toolkit.NotAuthorized):
            p.toolkit.abort(404, _('Resource not found'))

        try:
            shift_status = p.toolkit.get_action('shift_status')(
                None, {'resource_id': resource_id}
            )
        except p.toolkit.ObjectNotFound:
            shift_status = {}
        except p.toolkit.NotAuthorized:
            p.toolkit.abort(403, _('Not authorized to see this page'))

        return p.toolkit.render('shift/resource_data.html',
                                extra_vars={'status': shift_status})
