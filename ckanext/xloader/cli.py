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
@click.option('--sync', is_flag=True, default=False,
              help='Execute immediately instead of enqueueing for asynchronous processing')
def submit(dataset_spec, y, dry_run, sync):
    """
        xloader submit [options] <dataset-spec>
    """
    cmd = XloaderCmd(dry_run)

    if dataset_spec == 'all':
        cmd._setup_xloader_logger()
        cmd._submit_all(sync=sync)
    elif dataset_spec == 'all-existing':
        _confirm_or_abort(y, dry_run)
        cmd._setup_xloader_logger()
        cmd._submit_all_existing(sync=sync)
    else:
        pkg_name_or_id = dataset_spec
        cmd._setup_xloader_logger()
        cmd._submit_package(pkg_name_or_id, sync=sync)

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
