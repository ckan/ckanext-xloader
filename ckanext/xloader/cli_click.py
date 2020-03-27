# -*- coding: utf-8 -*-

import click

import ckantoolkit as tk

from ckanext.xloader import util


def get_commands():
    return [xloader]


@click.group()
def xloader():
    """xloader commands
    """
    pass


@xloader.command()
def status():
    """Shows status of jobs
    """
    util.printable_status(print_func=click.echo)


@click.argument(u"dataset-name-or-id")
@xloader.command()
def submit(dataset_name_or_id):
    """Submit the given datasets' resources to be xloaded into the
    DataStore. (They are added to the queue for CKAN's background task
    worker.)

    where <dataset-spec> is a dataset name or id

    all - Submit all datasets' resources to the DataStore

    all-existing - Re-submits all the resources already in the
        DataStore. (Ignores any resources that have not been stored
        in DataStore, e.g. because they are not tabular)

    """
    util.submit_package(dataset_name_or_id, print_func=click.echo)


@xloader.command()
def submit_all():
    """Submit all datasets' resources to be xloaded into the
    DataStore. (They are added to the queue for CKAN's background task
    worker.)
    """
    util.submit_all(print_func=click.echo)


@click.confirmation_option(prompt="Data in any DataStore resource that isn't "
    "in their source files "
    "(e.g. data added using the DataStore API) will be permanently "
    "lost. Are you sure you want to proceed?"
)
@xloader.command()
def submit_all_existing():
    """Re-submits all the resources already in the DataStore. (Ignores any
    resources that have not been stored in DataStore, e.g. because they are not
    tabular.) (They are added to the queue for CKAN's background task
    worker.)
    """
    util.submit_all_existing(print_func=click.echo)
