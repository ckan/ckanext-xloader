# -*- coding: utf-8 -*-

import click
from .command import XloaderCommand, Opts, print_status

# CKAN 2.9 Click commands

@click.group()
def xloader():
    """xloader commands
    """
    pass

@xloader.command()
def status():
    """Shows status of jobs
    """
    print_status()

@xloader.command()
@click.argument(u'dataset-spec')
@click.option('-y', default=False, help='Always answer yes to questions')
@click.option('--dry-run', default=False, help='Don\'t actually submit any resources')
@click.option('--ignore-format', default=False, help="""Submit resources even if they have a format
                not in the configured ckanext.xloader.formats""")
def submit(dataset_spec, y, dry_run, ignore_format):
    """
        xloader submit [options] <dataset-spec>
    """
    cmd = XloaderCommand(Opts(dry_run, ignore_format))

    if dataset_spec == 'all':
        cmd._setup_xloader_logger()
        cmd._submit_all()
    elif dataset_spec == 'all-existing':
        _confirm_or_abort(y, dry_run)
        cmd._setup_xloader_logger()
        cmd._submit_all_existing()
    else:
        pkg_name_or_id = dataset_spec
        cmd._setup_xloader_logger()
        cmd._submit_package(pkg_name_or_id)

    if cmd.error_occured:
        print('Finished but saw errors - see above for details')
        sys.exit(1)

def get_commands():
    return [xloader]

def _confirm_or_abort(yes, dry_run):
    if yes or dry_run:
        return
    question = (
        "Data in any datastore resource that isn't in their source files "
        "(e.g. data added using the datastore API) will be permanently "
        "lost. Are you sure you want to proceed?"
    )
    if not click.confirm(question):
        print("Aborting...")
        sys.exit(0)