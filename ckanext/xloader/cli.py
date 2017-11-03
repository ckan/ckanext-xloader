import sys

import ckan.lib.cli as cli
import ckan.plugins as p
try:
    # as it was called up to ckan 2.7
    import ckanext.datastore.db as datastore_backend
except ImportError:
    import ckanext.datastore as datastore_backend


class xloaderCommand(cli.CkanCommand):
    '''xloader commands

    Usage:

        xloader submit <pkg-name-or-id>
              Submit the given dataset's resources to the DataStore.

        xloader submit all
              Submit all datasets' resources to the DataStore.

              Unless you specify `--force`, resources that are already in the
              DataStore are skipped (even if the resource URL has changed, or
              the content at the URL has changed. Override the prompt using
              `-y` or `--yes`.

        xloader submit all-existing
              Submit all datasets' resources that are already in the DataStore
              to the DataStore again.

        xloader status
              Shows status of jobs

    '''

    summary = __doc__.split('\n')[0]
    usage = __doc__
    min_args = 1

    def __init__(self, name):
        super(xloaderCommand, self).__init__(name)

        self.parser.add_option('-y', dest='yes',
                               action='store_true', default=False,
                               help='Always answer yes to questions')
        self.parser.add_option('--force',
                               action='store_true', default=False,
                               help='Submit even if the resource is unchanged')

    def command(self):
        if not self.args:
            print self.usage
            sys.exit(1)
        if self.args[0] == 'submit':
            if len(self.args) < 2:
                self.parser.error('This command requires an argument')
            if self.args[1] == 'all':
                if self.options.force:
                    self._confirm_or_abort()
                self._load_config()
                self._submit_all()
            elif self.args[1] == 'all-existing':
                self._confirm_or_abort()
                self._load_config()
                self._submit_all_existing()
            else:
                pkg_name_or_id = self.args[1]
                self._load_config()
                self._submit_package(pkg_name_or_id)
        elif self.args[0] == 'status':
            self._load_config()
            self._print_status()
        else:
            self.parser.error('Unrecognized command')

    def _confirm_or_abort(self):
        if self.options.yes:
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
        resource_ids = datastore_db.get_all_resources_ids_in_datastore()
        self._submit(resource_ids)

    def _submit_all(self):
        # submit every package
        # for each package in the package list,
        #   submit each resource w/ _submit_package
        import ckan.model as model
        package_list = p.toolkit.get_action('package_list')
        for p_id in package_list({'model': model, 'ignore_auth': True}, {}):
            self._submit_package(p_id)

    def _submit_package(self, pkg_id):
        import ckan.model as model

        package_show = p.toolkit.get_action('package_show')
        try:
            pkg = package_show({'model': model, 'ignore_auth': True},
                               {'id': pkg_id.strip()})
        except Exception, e:
            print e
            print "Dataset '{}' was not found".format(pkg_id)
            sys.exit(1)

        resource_ids = [r['id'] for r in pkg['resources']]
        self._submit(resource_ids)

    def _submit(self, resources):
        import ckan.model as model

        print 'Submitting %d datastore resources' % len(resources)
        user = p.toolkit.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {})
        xloader_submit = p.toolkit.get_action('xloader_submit')
        for resource_id in resources:
            print ('Submitting %s...' % resource_id),
            data_dict = {
                'resource_id': resource_id,
                'ignore_hash': True,
            }
            if xloader_submit({'user': user['name']}, data_dict):
                print 'OK'
            else:
                print 'Fail'

    def _print_status(self):
        try:
            import ckan.lib.jobs as rq_jobs
        except ImportError:
            import ckanext.rq.jobs as rq_jobs
        jobs = rq_jobs.get_queue().jobs
        if not jobs:
            print 'No jobs currently queued'
        for job in jobs:
            job_params = eval(job.description.replace(
                'ckanext.xloader.jobs.xloader_data_into_datastore', ''))
            job_metadata = job_params['metadata']
            print '{id} Enqueued={enqueued:%Y-%m-%d %H:%M} res_id={res_id} ' \
                'url={url}'.format(
                    id=job._id,
                    enqueued=job.enqueued_at,
                    res_id=job_metadata['resource_id'],
                    url=job_metadata['original_url'],
                    )
