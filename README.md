# XLoader - ckanext-xloader

[![Tests](https://github.com/ckan/ckanext-xloader/workflows/Tests/badge.svg?branch=master)](https://github.com/ckan/ckanext-xloader/actions)
[![Latest Version](https://img.shields.io/pypi/v/ckanext-xloader.svg)](https://pypi.org/project/ckanext-xloader/)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/ckanext-xloader.svg)](https://pypi.org/project/ckanext-xloader/)
[![Development Status](https://img.shields.io/pypi/status/ckanext-xloader.svg)](https://pypi.org/project/ckanext-xloader/)
[![License](https://img.shields.io/pypi/l/ckanext-xloader.svg)](https://pypi.org/project/ckanext-xloader/)


Loads CSV (and similar) data into CKAN's DataStore. Designed as a
replacement for DataPusher because it offers ten times the speed and
more robustness (hence the name, derived from "Express Loader")

**OpenGov Inc.** has sponsored this development, with the aim of
benefitting open data infrastructure worldwide.

* [Key differences from DataPusher](#key-differences-from-datapusher)
* [Requirements](#requirements)
* [Installation](#installation)
* [Config settings](#config-settings)
* [Developer installation](#developer-installation)
* [Upgrading from DataPusher](#upgrading-from-datapusher)
* [Command-line interface](#command-line-interface)
   * [Jobs and workers](#jobs-and-workers)
* [Troubleshooting](#troubleshooting)
* [Running the Tests](#running-the-tests)
* [Releasing a New Version of XLoader](#releasing-a-new-version-of-xloader)

## Key differences from DataPusher

### Speed of loading

DataPusher - parses CSV rows, converts to detected column types,
converts the data to a JSON string, calls datastore_create for each
batch of rows, which reformats the data into an INSERT statement string,
which is passed to PostgreSQL.

XLoader - pipes the CSV file directly into PostgreSQL using COPY.

In [tests](https://github.com/ckan/ckanext-xloader/issues/25), XLoader
is over ten times faster than DataPusher.

### Robustness

DataPusher - one cause of failure was when casting cells to a guessed
type. The type of a column was decided by looking at the values of only
the first few rows. So if a column is mainly numeric or dates, but a
string (like "N/A") comes later on, then this will cause the load to
error at that point, leaving it half-loaded into DataStore.

XLoader - loads all the cells as text, before allowing the admin to
convert columns to the types they want (using the Data Dictionary
feature). In future it could do automatic detection and conversion.

### Simpler queueing tech

DataPusher - job queue is done by ckan-service-provider which is
bespoke, complicated and stores jobs in its own database (sqlite by
default).

XLoader - job queue is done by RQ, which is simpler, is backed by Redis,
allows access to the CKAN model and is CKAN's default queue technology.
You can also debug jobs easily using pdb. Job results are stored in
Sqlite by default, and for production simply specify CKAN's database in
the config and it's held there - easy.

(The other obvious candidate is Celery, but we don't need its
heavyweight architecture and its jobs are not debuggable with pdb.)

### Separate web server

DataPusher - has the complication that the queue jobs are done by a
separate (Flask) web app, apart from CKAN. This was the design because
the job requires intensive processing to convert every line of the data
into JSON. However it means more complicated code as info needs to be
passed between the services in http requests, more for the user to
set-up and manage - another app config, another apache config, separate
log files.

XLoader - the job runs in a worker process, in the same app as CKAN, so
can access the CKAN config, db and logging directly and avoids many HTTP
calls. This simplification makes sense because the xloader job doesn't
need to do much processing - mainly it is streaming the CSV file from
disk into PostgreSQL.

It is still entirely possible to run the XLoader worker on a separate
server, if that is desired. The worker needs the following:

- A copy of CKAN installed in the same Python virtualenv (but not
  running).
- A copy of the CKAN config file.
- Access to the Redis instance that the running CKAN app uses to store
  jobs.
- Access to the database.

You can then run it via ckan jobs worker as below.

### Caveat - column types

Note: With XLoader, all columns are stored in DataStore's database as
'text' type (whereas DataPusher did some rudimentary type guessing -
see 'Robustness' above). However once a resource is xloaded, an admin
can use the resource's Data Dictionary tab to change these types to
numeric or datestamp and re-load the file. When migrating from
DataPusher to XLoader you can preserve the types of existing resources
by using the `migrate_types` command.

There is scope to add functionality for automatically guessing column
type -offers to contribute this are welcomed.

## Requirements

Compatibility with core CKAN versions:

  | CKAN version   | Compatibility                                         |
  | -------------- |-------------------------------------------------------|
  | 2.7            | no longer supported (last supported version: 0.12.2)  |
  | 2.8            | no longer supported (last supported version: 0.12.2)  |
  | 2.9            | no longer supported (last supported version: 1.2.x)   |
  | 2.10           | yes                                                   |
  | 2.11           | yes                                                   |

## Installation

To install XLoader:

1.  Activate your CKAN virtual environment, for example:

        . /usr/lib/ckan/default/bin/activate

2.  Install the ckanext-xloader Python package into your virtual
    environment:

        pip install ckanext-xloader

3.  Install dependencies:

        pip install -r https://raw.githubusercontent.com/ckan/ckanext-xloader/master/requirements.txt
        pip install -U requests[security]

4.  Add `xloader` to the `ckan.plugins` setting in your CKAN config file
    (by default the config file is located at
    `/etc/ckan/default/production.ini`).

    You should also remove `datapusher` if it is in the list, to avoid
    them both trying to load resources into the DataStore.

    Ensure `datastore` is also listed, to enable CKAN DataStore.

5.  Starting CKAN 2.10 you will need to set an API Token to be able to
    execute jobs against the server:

        ckanext.xloader.api_token = <your-CKAN-generated-API-Token>

6.  If it is a production server, you'll want to store jobs info in a
    more robust database than the default sqlite file. It can happily
    use the main CKAN postgres db by adding this line to the config, but
    with the same value as you have for `sqlalchemy.url`:

        ckanext.xloader.jobs_db.uri = postgresql://ckan_default:pass@localhost/ckan_default

    (This step can be skipped when just developing or testing.)

7.  Restart CKAN. For example if you've deployed CKAN with Apache on
    Ubuntu:

        sudo service apache2 reload

8.  Run the worker:

        ckan -c /etc/ckan/default/ckan.ini jobs worker

## Config settings

Configuration:


This plugin supports the [`ckan.download_proxy`](https://docs.ckan.org/en/latest/maintaining/configuration.html#ckan-download-proxy) setting,
to use a proxy server when downloading files. This setting is shared
with other plugins that download resource files, such as
ckanext-archiver. Eg:

    ckan.download_proxy = <http://my-proxy:1234/>

You may also wish to configure the database to use your preferred date
input style on COPY. For example, to make
[PostgreSQL](<https://www.postgresql.org/docs/current/runtime-config-client.html#RUNTIME-CONFIG-CLIENT-FORMAT>)
expect European (day-first) dates, you could add to `postgresql.conf`:

    datestyle=ISO,DMY

All configurations below are defined in the
[config_declaration.yaml](ckanext/xloader/config_declaration.yaml) file.


#### ckanext.xloader.jobs_db.uri

Example:

```
postgresql://ckan_default:pass@localhost/ckan_default

```


Default value: `sqlite:////tmp/xloader_jobs.db`

The connection string for the jobs database used by XLoader. The
default of an sqlite file is fine for development. For production use a
Postgresql database.


#### ckanext.xloader.api_token

Example:

```
ckanext.xloader.api_token = eyJ0eXAiOiJKV1QiLCJh.eyJqdGkiOiJ0M2VNUFlQWFg0VU.8QgV8em4RA
```

Default value: none

Uses a specific API token for the xloader_submit action instead of the
apikey of the site_user. It's mandatory starting from CKAN 2.10. You can get one
running the command `ckan user token add {USER_NAME} xloader -q`


#### ckanext.xloader.formats

Example:

```
ckanext.xloader.formats = csv application/csv xls application/vnd.ms-excel
```

Default value: none

The formats that are accepted. If the value of the resource.format is
anything else then it won't be 'xloadered' to DataStore (and will therefore
only be available to users in the form of the original download/link).
Case insensitive. Defaults are listed in utils.py.


#### ckanext.xloader.max_content_length

Example:

```
ckanext.xloader.max_content_length = 100000
```

Default value: `1000000000`

The maximum file size that XLoader will attempt to load.


#### ckanext.xloader.use_type_guessing

Default value: `False`

By default, xloader will first try to add tabular data to the DataStore
with a direct PostgreSQL COPY. This is relatively fast, but does not
guess column types. If this fails, xloader falls back to a method more
like DataPusher's behaviour. This has the advantage that the column types
are guessed. However it is more error prone and far slower.
To always skip the direct PostgreSQL COPY and use type guessing, set
this option to True.


#### ckanext.xloader.strict_type_guessing

Default value: `True`

Use with ckanext.xloader.use_type_guessing to set strict true or false
for type guessing. If set to False, the types will always fallback to string type.

Strict means that a type will not be guessed if parsing fails for a single cell in the column.


#### ckanext.xloader.max_type_guessing_length

Example:

```
ckanext.xloader.max_type_guessing_length = 100000
```

Default value: `0`

The maximum file size that will be passed to Tabulator if the
use_type_guessing flag is enabled. Larger files will use COPY even if
the flag is set. Defaults to 1/10 of the maximum content length.


#### ckanext.xloader.parse_dates_dayfirst

Default value: `False`

Whether ambiguous dates should be parsed day first. Defaults to False.
If set to True, dates like '01.02.2022' will be parsed as day = 01,
month = 02.
NB: isoformat dates like '2022-01-02' will be parsed as YYYY-MM-DD, and
this option will not override that.
See [dateutil docs](https://dateutil.readthedocs.io/en/stable/parser.html#dateutil.parser.parse)
for more details.


#### ckanext.xloader.parse_dates_yearfirst

Default value: `False`

Whether ambiguous dates should be parsed year first. Defaults to False.
If set to True, dates like '01.02.03' will be parsed as year = 2001,
month = 02, day = 03. See [dateutil docs](https://dateutil.readthedocs.io/en/stable/parser.html#dateutil.parser.parse)
for more details.


#### ckanext.xloader.job_timeout

Example:

```
ckanext.xloader.job_timeout = 3600
```

Default value: `3600`

The maximum time for the loading of a resource before it is aborted.
Give an amount in seconds. Default is 60 minutes


#### ckanext.xloader.ignore_hash

Default value: `False`

Ignore the file hash when submitting to the DataStore, if set to True
resources are always submitted (if their format matches), if set to
False (default), resources are only submitted if their hash has changed.


#### ckanext.xloader.max_excerpt_lines

Example:

```
ckanext.xloader.max_excerpt_lines = 100
```

Default value: `0`

When loading a file that is bigger than `max_content_length`, xloader can
still try and load some of the file, which is useful to display a
preview. Set this option to the desired number of lines/rows that it
loads in this case.
If the file-type is supported (CSV, TSV) an excerpt with the number of
`max_excerpt_lines` lines will be submitted while the `max_content_length`
is not exceeded.
If set to 0 (default) files that exceed the `max_content_length` will
not be loaded into the datastore.


#### ckanext.xloader.ssl_verify

Example:

```
ckanext.xloader.ssl_verify = True
```

Default value: `True`

Requests verifies SSL certificates for HTTPS requests. Setting verify to
False should only be enabled during local development or testing. Default
to True.


#### ckanext.xloader.clean_datastore_tables

Example:

```
ckanext.xloader.clean_datastore_tables = True
```

Default value: `False`

Enqueue jobs to remove Datastore tables from Resources that have a format
that is not in ckanext.xloader.formats after a Resource is updated.


#### ckanext.xloader.show_badges

Default value: `True`

Controls whether or not the status badges display in the front end.


#### ckanext.xloader.debug_badges

Example:

```
ckanext.xloader.debug_badges = True
```

Default value: `False`

Controls whether or not the status badges display all of the statuses. By default,
the badges will display "pending", "running", and "error". With debug_badges enabled,
they will also display "complete", "active", "inactive", and "unknown".

#### ckanext.xloader.validation.requires_successful_report

Supports: __ckanext-validation__

Example:

```
ckanext.xloader.validation.requires_successful_report = True
```

Default value: `False`

Controls whether or not a resource requires a successful validation report from the ckanext-validation plugin in order to be XLoadered.

#### ckanext.xloader.validation.enforce_schema

Supports: __ckanext-validation__

Example:

```
ckanext.xloader.validation.enforce_schema = False
```

Default value: `True`

Controls whether or not a resource requires a Validation Schema to be present from the ckanext-validation plugin to be XLoadered.

## Data Dictionary Fields

#### strip_extra_white

This plugin adds the `Strip Extra Leading and Trailing White Space` field to Data Dictionary fields. This controls whether or not to trim whitespace from data values prior to inserting into the database. Default for each field is `True` (it will trim whitespace).

## Developer installation

To install XLoader for development, activate your CKAN virtualenv and in
the directory up from your local ckan repo:

    git clone https://github.com/ckan/ckanext-xloader.git
    cd ckanext-xloader
    pip install -e .
    pip install -r requirements.txt
    pip install -r dev-requirements.txt

## Upgrading from DataPusher

To upgrade from DataPusher to XLoader:

1.  Install XLoader as above, including running the xloader worker.

2.  (Optional) For existing datasets that have been datapushed to
    datastore, freeze the column types (in the data dictionaries), so
    that XLoader doesn't change them back to string on next xload:

        ckan -c /etc/ckan/default/ckan.ini migrate_types

3.  If you've not already, change the enabled plugin in your config -
    on the `ckan.plugins` line replace `datapusher` with `xloader`.

4.  (Optional) If you wish, you can disable the direct loading and
    continue to just use tabulator - for more about this see the docs on
    config option: `ckanext.xloader.use_type_guessing`

5.  Stop the datapusher worker:

        sudo a2dissite datapusher

6.  Restart CKAN:

        sudo service apache2 reload
        sudo service nginx reload

## Command-line interface

You can submit single or multiple resources to be xloaded using the
command-line interface.

e.g. :

    ckan -c /etc/ckan/default/ckan.ini xloader submit <dataset-name>

For debugging you can try xloading it synchronously (which does the load
directly, rather than asking the worker to do it) with the `-s` option:

    ckan -c /etc/ckan/default/ckan.ini xloader submit <dataset-name> -s

See the status of jobs:

    ckan -c /etc/ckan/default/ckan.ini xloader status

Submit all datasets' resources to the DataStore:

    ckan -c /etc/ckan/default/ckan.ini xloader submit all

Re-submit all the resources already in the DataStore (Ignores any
resources that have not been stored in DataStore e.g. because they are
not tabular):

    ckan -c /etc/ckan/default/ckan.ini xloader submit all-existing

**Full list of XLoader CLI commands**:

    ckan -c /etc/ckan/default/ckan.ini xloader --help

### Jobs and workers

Main docs for managing jobs:

https://docs.ckan.org/en/latest/maintaining/background-tasks.html#managing-background-jobs

Main docs for running and managing workers are here:

https://docs.ckan.org/en/latest/maintaining/background-tasks.html#running-background-jobs

Useful commands:

Clear (delete) all outstanding jobs:

    ckan -c /etc/ckan/default/ckan.ini jobs clear [QUEUES]

If having trouble with the worker process, restarting it can help:

    sudo supervisorctl restart ckan-worker:*

## Troubleshooting

**KeyError: "Action 'datastore_search' not found"**

You need to enable the [datastore]{.title-ref} plugin in your CKAN
config. See 'Installation' section above to do this and restart the
worker.

**ProgrammingError: (ProgrammingError) relation "\_table_metadata"
does not exist**

Your DataStore permissions have not been set-up - see:
https://docs.ckan.org/en/latest/maintaining/datastore.html#set-permissions

## Running the Tests

The first time, your test datastore database needs the trigger applied:

    sudo -u postgres psql datastore_test -f full_text_function.sql

To run the tests, do:

    pytest ckan-ini=test.ini ckanext/xloader/tests

## Releasing a New Version of XLoader

XLoader is available on PyPI as
<https://pypi.org/project/ckanext-xloader>.

To publish a new version to PyPI follow these steps:

1.  Update the version number in the `setup.py` file. See [PEP
    440](http://legacy.python.org/dev/peps/pep-0440/#public-version-identifiers)
    for how to choose version numbers.

2.  Update the CHANGELOG.

3.  Make sure you have the latest version of necessary packages:

        pip install --upgrade setuptools wheel twine

4.  Create source and binary distributions of the new version:

        python setup.py sdist bdist_wheel && twine check dist/*

    Fix any errors you get.

5.  Upload the source distribution to PyPI:

        twine upload dist/*

6.  Commit any outstanding changes:

        git commit -a
        git push

7.  Tag the new release of the project on GitHub with the version number
    from the `setup.py` file. For example if the version number in
    `setup.py` is 0.0.1 then do:

        git tag 0.0.1
        git push --tags
