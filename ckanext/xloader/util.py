import sys

import ckan.plugins as p


def printable_status(print_func):
    try:
        import ckan.lib.jobs as rq_jobs
    except ImportError:
        import ckanext.rq.jobs as rq_jobs
    jobs = rq_jobs.get_queue().jobs
    if not jobs:
        print_func('No jobs currently queued')
    for job in jobs:
        job_params = eval(job.description.replace(
            'ckanext.xloader.jobs.xloader_data_into_datastore', ''))
        job_metadata = job_params['metadata']
        print_func(
            '{id} Enqueued={enqueued:%Y-%m-%d %H:%M} res_id={res_id} url={url}'
            .format(
                id=job._id,
                enqueued=job.enqueued_at,
                res_id=job_metadata['resource_id'],
                url=job_metadata['original_url'],
                ))


def submit_all(print_func):
    '''Submit every package.

    For each package in the package list, submit each resource with
    submit_package()
    '''
    import ckan.model as model
    package_list = p.toolkit.get_action('package_list')(
        {'model': model, 'ignore_auth': True}, {})
    print_func('Processing %d datasets' % len(package_list))
    user = p.toolkit.get_action('get_site_user')(
        {'model': model, 'ignore_auth': True}, {})
    error_occurred = False
    for p_id in package_list:
        error_occurred |= submit_package(
            pkg_id=p_id, print_func=print_func, user=user, indent=2)
    if error_occurred:
        print_func('Finished but saw errors - see above for details')
        sys.exit(1)


def submit_all_existing(print_func, dry_run=False):
    '''Submit every resource that is already in DataStore.

    Submits each resource with submit_resource()
    '''
    import ckan.model as model
    from ckanext.datastore.backend import get_all_resources_ids_in_datastore
    resource_ids = get_all_resources_ids_in_datastore()
    print_func('Processing {} resources'.format(len(resource_ids)))
    user = p.toolkit.get_action('get_site_user')(
        {'model': model, 'ignore_auth': True}, {})
    error_occurred = False
    for resource_id in resource_ids:
        try:
            resource_dict = p.toolkit.get_action('resource_show')(
                {'model': model, 'ignore_auth': True}, {'id': resource_id})
        except p.toolkit.ObjectNotFound:
            print_func('  Skipping resource {} found in datastore but not in '
                    'metadata'.format(resource_id))
            continue
        error_occurred |= submit_resource(
            resource_dict, print_func, user=user, indent=2, dry_run=dry_run)
    if error_occurred:
        print_func('Finished but saw errors - see above for details')
        sys.exit(1)


def submit_package(pkg_name_or_id, print_func, user=None, indent=0):
    import ckan.model as model
    if not user:
        user = p.toolkit.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {})

    try:
        pkg = p.toolkit.get_action('package_show')(
            {'model': model, 'ignore_auth': True},
            {'id': pkg_name_or_id.strip()})
    except Exception as e:
        print_func(e)
        print_func(' ' * indent + 'Dataset "{}" was not found'.format(pkg_id))
        sys.exit(1)

    print_func(' ' * indent + 'Processing dataset {} with {} resources'.format(
            pkg['name'], len(pkg['resources'])))
    error_occurred = False
    for resource in pkg['resources']:
        try:
            resource['package_name'] = pkg['name']  # for debug output
            submit_resource(resource, print_func=print_func, user=user,
                            indent=indent + 2)
        except Exception as e:
            error_occurred = True
            print_func(e)
            print_func(' ' * indent + 'ERROR submitting resource "{}" '.format(
                resource['id']))
            continue
    return error_occurred


def submit_resource(resource, print_func, user, indent=0, dry_run=False):
    '''resource: resource dictionary

    return:  None, or True if an error occurred
    '''
    # import here, so that that loggers are setup
    from ckanext.xloader.plugin import XLoaderFormats

    if not XLoaderFormats.is_it_an_xloader_format(resource['format']):
        print_func(
            ' ' * indent +
            'Skipping resource {r[id]} because format "{r[format]}" is '
            'not configured to be xloadered'.format(r=resource))
        return
    if resource['url_type'] in ('datapusher', 'xloader'):
        print_func(
            ' ' * indent +
            'Skipping resource {r[id]} because url_type "{r[url_type]}" '
            'means resource.url points to the datastore '
            'already, so loading would be circular.'.format(
            r=resource))
        return
    dataset_ref = resource.get('package_name', resource['package_id'])
    print_func(
        '{indent}Submitting /dataset/{dataset}/resource/{r[id]}\n'
        '{indent}           url={r[url]}\n'
        '{indent}           format={r[format]}'
        .format(dataset=dataset_ref, r=resource, indent=' ' * indent))
    data_dict = {
        'resource_id': resource['id'],
        'ignore_hash': True,
    }
    if dry_run:
        print_func(' ' * indent + '(not submitted - dry-run)')
        return
    success = p.toolkit.get_action('xloader_submit')({'user': user['name']}, data_dict)
    if success:
        print_func(' ' * indent + '...ok')
    else:
        print_func(' ' * indent + 'ERROR submitting resource')
        return True  # i.e. error_occurred
