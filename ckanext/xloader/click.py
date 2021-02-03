# -*- coding: utf-8 -*-

import click
import ckan.plugins.toolkit as tk

# CKAN 2.9 Click commands

@click.group()
def xloader():
    """xloader commands
    """
    pass

def _print_status():
    try:
        import ckan.lib.jobs as rq_jobs
    except ImportError:
        import ckanext.rq.jobs as rq_jobs
    jobs = rq_jobs.get_queue().jobs
    if not jobs:
        print('No jobs currently queued')
    for job in jobs:
        job_params = eval(job.description.replace(
            'ckanext.xloader.jobs.xloader_data_into_datastore', ''))
        job_metadata = job_params['metadata']
        print('{id} Enqueued={enqueued:%Y-%m-%d %H:%M} res_id={res_id} '
                'url={url}'.format(
                    id=job._id,
                    enqueued=job.enqueued_at,
                    res_id=job_metadata['resource_id'],
                    url=job_metadata['original_url'],
                    ))

def _submit_all_existing(self):
    import ckan.model as model
    from ckanext.datastore.backend \
        import get_all_resources_ids_in_datastore
    resource_ids = get_all_resources_ids_in_datastore()
    print('Processing %d resources' % len(resource_ids))
    user = tk.get_action('get_site_user')(
        {'model': model, 'ignore_auth': True}, {})
    for resource_id in resource_ids:
        try:
            resource_dict = tk.get_action('resource_show')(
                {'model': model, 'ignore_auth': True}, {'id': resource_id})
        except tk.ObjectNotFound:
            print('  Skipping resource {} found in datastore but not in '
                    'metadata'.format(resource_id))
            continue
        self._submit_resource(resource_dict, user, indent=2)

def _submit_all():
    # submit every package
    # for each package in the package list,
    #   submit each resource w/ _submit_package
    import ckan.model as model
    package_list = tk.get_action('package_list')(
        {'model': model, 'ignore_auth': True}, {})
    print('Processing %d datasets' % len(package_list))
    user = tk.get_action('get_site_user')(
        {'model': model, 'ignore_auth': True}, {})
    for p_id in package_list:
        self._submit_package(p_id, user, indent=2)

def _submit_package(self, pkg_id, user=None, indent=0):
    import ckan.model as model
    if not user:
        user = tk.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {})

    try:
        pkg = tk.get_action('package_show')(
            {'model': model, 'ignore_auth': True},
            {'id': pkg_id.strip()})
    except Exception as e:
        print(e)
        print(' ' * indent + 'Dataset "{}" was not found'.format(pkg_id))
        sys.exit(1)

    print(' ' * indent + 'Processing dataset {} with {} resources'.format(
            pkg['name'], len(pkg['resources'])))
    for resource in pkg['resources']:
        try:
            resource['package_name'] = pkg['name']  # for debug output
            self._submit_resource(resource, user, indent=indent + 2)
        except Exception as e:
            self.error_occured = True
            print(e)
            print(' ' * indent + 'ERROR submitting resource "{}" '.format(
                resource['id']))
            continue

def _submit_resource(self, resource, user, indent=0):
    '''resource: resource dictionary
    '''
    # import here, so that that loggers are setup
    from ckanext.xloader.plugin import XLoaderFormats

    if not XLoaderFormats.is_it_an_xloader_format(resource['format']):
        print(' ' * indent +
                'Skipping resource {r[id]} because format "{r[format]}" is '
                'not configured to be xloadered'.format(r=resource))
        return
    if resource['url_type'] in ('datapusher', 'xloader'):
        print(' ' * indent +
                'Skipping resource {r[id]} because url_type "{r[url_type]}" '
                'means resource.url points to the datastore '
                'already, so loading would be circular.'.format(
                r=resource))
        return
    dataset_ref = resource.get('package_name', resource['package_id'])
    print('{indent}Submitting /dataset/{dataset}/resource/{r[id]}\n'
            '{indent}           url={r[url]}\n'
            '{indent}           format={r[format]}'
            .format(dataset=dataset_ref, r=resource, indent=' ' * indent))
    data_dict = {
        'resource_id': resource['id'],
        'ignore_hash': True,
    }
    if self.options.dry_run:
        print(' ' * indent + '(not submitted - dry-run)')
        return
    success = tk.get_action('xloader_submit')({'user': user['name']}, data_dict)
    if success:
        print(' ' * indent + '...ok')
    else:
        print(' ' * indent + 'ERROR submitting resource')
        self.error_occured = True

@xloader.command()
def status():
    """Shows status of jobs
    """
    _print_status()

@xloader.command()
@click.argument(u'dataset-spec')
@click.option('-y', default=False, help='Always answer yes to questions')
@click.option('--dry-run', default=False, help='Don\'t actually submit any resources')
@click.option('--ignore-format', default=False, help="""Submit resources even if they have a format
                not in the configured ckanext.xloader.formats""")
@click.pass_context
def submit(ctx, dataset_spec, y, dry_run, ignore_format):
    """
        xloader submit [options] <dataset-spec>
    """
    _submit_all()

def get_commands():
    return [xloader]