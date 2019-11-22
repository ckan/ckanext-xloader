import sys
import logging

import ckan.lib.cli as cli
import ckan.plugins as p


class xloaderCommand(cli.CkanCommand):
    '''xloader commands

    Usage:

        xloader submit [options] <dataset-spec>
            Submit the given datasets' resources to be xloaded into the
            DataStore. (They are added to the queue for CKAN's background task
            worker.)

            where <dataset-spec> is one of:

                <dataset-name> - Submit a particular dataset's resources

                <dataset-id> - Submit a particular dataset's resources

                all - Submit all datasets' resources to the DataStore

                all-existing - Re-submits all the resources already in the
                    DataStore. (Ignores any resources that have not been stored
                    in DataStore, for any reason.)

            options:

                --dry-run - doesn't actually submit any resources

                --ignore-format - submit resources even if they have a format
                not in the configured ckanext.xloader.formats

        xloader status
              Shows status of jobs
    '''

    summary = __doc__.split('\n')[0]
    usage = __doc__
    min_args = 1

    def __init__(self, name):
        super(xloaderCommand, self).__init__(name)
        self.error_occured = False

        self.parser.add_option('-y', dest='yes',
                               action='store_true', default=False,
                               help='Always answer yes to questions')
        self.parser.add_option('--ignore-format',
                               action='store_true', default=False,
                               help='Submit even if the resource.format is not'
                               ' in ckanext.xloader.formats')
        self.parser.add_option('--dry-run',
                               action='store_true', default=False,
                               help='Don\'t actually submit anything')

    def command(self):
        if not self.args:
            print self.usage
            sys.exit(1)
        if self.args[0] == 'submit':
            if len(self.args) < 2:
                self.parser.error('This command requires an argument')
            if self.args[1] == 'all':
                self._load_config()
                self._setup_xloader_logger()
                self._submit_all()
            elif self.args[1] == 'all-existing':
                self._confirm_or_abort()
                self._load_config()
                self._setup_xloader_logger()
                self._submit_all_existing()
            else:
                pkg_name_or_id = self.args[1]
                self._load_config()
                self._setup_xloader_logger()
                self._submit_package(pkg_name_or_id)
            self._handle_command_status()
        elif self.args[0] == 'status':
            self._load_config()
            self._print_status()
        else:
            self.parser.error('Unrecognized command')

    def _handle_command_status(self):
        if self.error_occured:
            print('Finished but saw errors - see above for details')
            sys.exit(1)

    def _setup_xloader_logger(self):
        # whilst the deveopment.ini's loggers are setup now, because this is
        # cli, let's ensure we xloader debug messages are printed for the user
        logger = logging.getLogger('ckanext.xloader')
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '      %(name)-12s %(levelname)-5s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False  # in case the config

    def _confirm_or_abort(self):
        if self.options.yes or self.options.dry_run:
            return
        question = (
            "Data in any datastore resource that isn't in their source files "
            "(e.g. data added using the datastore API) will be permanently "
            "lost. Are you sure you want to proceed?"
        )
        answer = cli.query_yes_no(question, default=None)
        if not answer == 'yes':
            print "Aborting..."
            sys.exit(0)

    def _submit_all_existing(self):
        import ckan.model as model
        from ckanext.datastore.backend \
            import get_all_resources_ids_in_datastore
        resource_ids = get_all_resources_ids_in_datastore()
        print('Processing %d resources' % len(resource_ids))
        user = p.toolkit.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {})
        for resource_id in resource_ids:
            try:
                resource_dict = p.toolkit.get_action('resource_show')(
                    {'model': model, 'ignore_auth': True}, {'id': resource_id})
            except p.toolkit.ObjectNotFound:
                print('  Skipping resource {} found in datastore but not in '
                      'metadata'.format(resource_id))
                continue
            self._submit_resource(resource_dict, user, indent=2)

    def _submit_all(self):
        # submit every package
        # for each package in the package list,
        #   submit each resource w/ _submit_package
        import ckan.model as model
        package_list = p.toolkit.get_action('package_list')(
            {'model': model, 'ignore_auth': True}, {})
        print('Processing %d datasets' % len(package_list))
        user = p.toolkit.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {})
        for p_id in package_list:
            self._submit_package(p_id, user, indent=2)

    def _submit_package(self, pkg_id, user=None, indent=0):
        import ckan.model as model
        if not user:
            user = p.toolkit.get_action('get_site_user')(
                {'model': model, 'ignore_auth': True}, {})

        try:
            pkg = p.toolkit.get_action('package_show')(
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
        success = p.toolkit.get_action('xloader_submit')({'user': user['name']}, data_dict)
        if success:
            print(' ' * indent + '...ok')
        else:
            print(' ' * indent + 'ERROR submitting resource')
            self.error_occured = True

    def _print_status(self):
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
