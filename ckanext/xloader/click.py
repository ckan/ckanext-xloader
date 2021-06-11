# -*- coding: utf-8 -*-

import sys
import click
from ckanext.xloader.command import XloaderCmd

# Click commands for CKAN 2.9 and above


@click.group(short_help='Perform XLoader related actions')
def xloader():
    """xloader commands
    """
    pass


@xloader.command()
def status():
    """Shows status of jobs
    """
    cmd = XloaderCmd()
    cmd.print_status()


@xloader.command()
@click.argument(u'dataset-spec')
@click.option('-y', is_flag=True, default=False, help='Always answer yes to questions')
@click.option('--dry-run', is_flag=True, default=False, help='Don\'t actually submit any resources')
def submit(dataset_spec, y, dry_run):
    """
        xloader submit [options] <dataset-spec>
    """
    cmd = XloaderCmd(dry_run)

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
