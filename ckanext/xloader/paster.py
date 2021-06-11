from __future__ import print_function
import sys

import ckan.lib.cli as cli
import ckan.plugins as p
import ckan.model as model

import ckanext.datastore.helpers as h
from ckanext.xloader.command import XloaderCmd

# Paster command for CKAN 2.8 and below


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
                    in DataStore, e.g. because they are not tabular)

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
        cmd = XloaderCmd(self.options.dry_run)
        if not self.args:
            print(self.usage)
            sys.exit(1)
        if self.args[0] == 'submit':
            if len(self.args) < 2:
                self.parser.error('This command requires an argument')
            if self.args[1] == 'all':
                self._load_config()
                cmd._setup_xloader_logger()
                cmd._submit_all()
            elif self.args[1] == 'all-existing':
                self._confirm_or_abort()
                self._load_config()
                cmd._setup_xloader_logger()
                cmd._submit_all_existing()
            else:
                pkg_name_or_id = self.args[1]
                self._load_config()
                cmd._setup_xloader_logger()
                cmd._submit_package(pkg_name_or_id)
            self._handle_command_status(cmd.error_occured)
        elif self.args[0] == 'status':
            self._load_config()
            cmd.print_status()
        else:
            self.parser.error('Unrecognized command')

    def _handle_command_status(self, error_occured):
        if error_occured:
            print('Finished but saw errors - see above for details')
            sys.exit(1)

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
            print("Aborting...")
            sys.exit(0)


class MigrateTypesCommand(cli.CkanCommand):
    '''Migrate command

    Turn existing resource field types into Data Dictionary overrides.
    This is intended to simplify migration from DataPusher to XLoader,
    by allowing you to reuse the types that DataPusher has guessed.

    Usage:

        migrate_types [options] [resource-spec]
            Add the given resources' field types to the Data Dictionary.

            where resource-spec is one of:

                <resource-id> - Migrate a particular resource

                all - Migrate all resources (this is the default)

    '''
    summary = __doc__.split('\n')[0]
    usage = __doc__
    min_args = 0

    def __init__(self, name):
        super(MigrateTypesCommand, self).__init__(name)
        self.error_occured = False

        self.parser.add_option('-t', '--include-text',
                               action='store_true', default=False,
                               help='Add Data Dictionary overrides even for text fields')

        self.parser.add_option('--force',
                               action='store_true', default=False,
                               help='Overwrite existing data dictionary if it exists')

    def command(self):
        self._load_config()
        if not self.args or len(self.args) == 0 or self.args[0] == 'all':
            self._migrate_all()
        else:
            self._migrate_resource(self.args[0])
        self._handle_command_status()

    def _migrate_all(self):
        session = model.Session
        resource_count = session.query(model.Resource).filter_by(state='active').count()
        print("Updating {} resource(s)".format(resource_count))
        resources_done = 0
        for resource in session.query(model.Resource).filter_by(state='active'):
            resources_done += 1
            self._migrate_resource(resource.id,
                                   prefix='[{}/{}]: '.format(resources_done,
                                                             resource_count))
            if resources_done % 100 == 0:
                print("[{}/{}] done".format(resources_done, resource_count))
        print("[{}/{}] done".format(resources_done, resource_count))

    def _migrate_resource(self, resource_id, prefix=''):
        data_dict = h.datastore_dictionary(resource_id)

        def print_status(status):
            if self.options.verbose:
                print("{}{}: {}".format(prefix, resource_id, status))

        if not data_dict:
            print_status("not found")
            return

        fields = []
        for field in data_dict:
            if field['type'] == 'text' and not self.options.include_text:
                type_override = ''
            else:
                type_override = field['type']

            if 'info' not in field:
                field.update({'info': {'notes': '',
                                       'type_override': type_override,
                                       'label': ''}})
            elif self.options.force:
                field['info'].update({'type_override': type_override})
            else:
                print_status("skipped")
                return

            fields.append({
                'id': field['id'],
                'type': field['type'],
                'info': field['info']
            })

        try:
            p.toolkit.get_action('datastore_create')(None, {
                'resource_id': resource_id,
                'force': True,
                'fields': fields
            })
            print_status("updated")
        except Exception as e:
            self.error_occured = True
            print("{}: failed, {}".format(resource_id, e))

    def _handle_command_status(self):
        if self.error_occured:
            print('Finished but saw errors - see above for details')
            sys.exit(1)
