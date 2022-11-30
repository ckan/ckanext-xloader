.. You should enable this project on travis-ci.org and coveralls.io to make
   these badges work. The necessary Travis and Coverage config files have been
   generated for you.

.. image:: https://travis-ci.org/ckan/ckanext-xloader.svg?branch=master
    :target: https://travis-ci.org/ckan/ckanext-xloader

.. image:: https://img.shields.io/pypi/v/ckanext-xloader.svg
    :target: https://pypi.org/project/ckanext-xloader/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/pyversions/ckanext-xloader.svg
    :target: https://pypi.org/project/ckanext-xloader/
    :alt: Supported Python versions

.. image:: https://img.shields.io/pypi/status/ckanext-xloader.svg
    :target: https://pypi.org/project/ckanext-xloader/
    :alt: Development Status

.. image:: https://img.shields.io/pypi/l/ckanext-xloader.svg
    :target: https://pypi.org/project/ckanext-xloader/
    :alt: License

=========================
XLoader - ckanext-xloader
=========================

Loads CSV (and similar) data into CKAN's DataStore. Designed as a replacement
for DataPusher because it offers ten times the speed and more robustness
(hence the name, derived from "Express Loader")

**OpenGov Inc.** has sponsored this development, with the aim of benefitting
open data infrastructure worldwide.

-------------------------------
Key differences from DataPusher
-------------------------------

Speed of loading
----------------

DataPusher - parses CSV rows, converts to detected column types, converts the
data to a JSON string, calls datastore_create for each batch of rows, which
reformats the data into an INSERT statement string, which is passed to
PostgreSQL.

XLoader - pipes the CSV file directly into PostgreSQL using COPY.

In `tests <https://github.com/ckan/ckanext-xloader/issues/25>`_, XLoader
is over ten times faster than DataPusher.

Robustness
----------

DataPusher - one cause of failure was when casting cells to a guessed type. The
type of a column was decided by looking at the values of only the first few
rows. So if a column is mainly numeric or dates, but a string (like "N/A")
comes later on, then this will cause the load to error at that point, leaving
it half-loaded into DataStore.

XLoader - loads all the cells as text, before allowing the admin to
convert columns to the types they want (using the Data Dictionary feature). In
future it could do automatic detection and conversion.

Simpler queueing tech
---------------------

DataPusher - job queue is done by ckan-service-provider which is bespoke,
complicated and stores jobs in its own database (sqlite by default).

XLoader - job queue is done by RQ, which is simpler, is backed by Redis, allows
access to the CKAN model and is CKAN's default queue technology (since CKAN
2.7). You can also debug jobs easily using pdb. Job results are stored in
Sqlite by default, and for production simply specify CKAN's database in the
config and it's held there - easy.

(The other obvious candidate is Celery, but we don't need its heavyweight
architecture and its jobs are not debuggable with pdb.)

Separate web server
-------------------

DataPusher - has the complication that the queue jobs are done by a separate
(Flask) web app, apart from CKAN. This was the design because the job requires
intensive processing to convert every line of the data into JSON. However it
means more complicated code as info needs to be passed between the services in
http requests, more for the user to set-up and manage - another app config,
another apache config, separate log files.

XLoader - the job runs in a worker process, in the same app as CKAN, so
can access the CKAN config, db and logging directly and avoids many HTTP calls.
This simplification makes sense because the xloader job doesn't need to do much
processing - mainly it is streaming the CSV file from disk into PostgreSQL.

Caveat - column types
---------------------

Note: With XLoader, all columns are stored in DataStore's database as 'text'
type (whereas DataPusher did some rudimentary type guessing - see 'Robustness'
above). However once a resource is xloaded, an admin can use the resource's
Data Dictionary tab (CKAN 2.7 onwards) to change these types to numeric or
datestamp and re-load the file. When migrating from DataPusher to XLoader you
can preserve the types of existing resources by using the ``migrate_types``
command.

There is scope to add functionality for automatically guessing column type -
offers to contribute this are welcomed.


------------
Requirements
------------

Compatibility with core CKAN versions:

=============== =============
CKAN version    Compatibility
=============== =============
2.3             no longer tested and you must install ckanext-rq
2.4             no longer tested and you must install ckanext-rq
2.5             no longer tested and you must install ckanext-rq
2.6             no longer tested and you must install ckanext-rq
2.7             yes
2.8             yes
2.9             yes (both Python2 and Python3)
=============== =============

------------
Installation
------------

To install XLoader:

1. Activate your CKAN virtual environment, for example::

     . /usr/lib/ckan/default/bin/activate

2. Install the ckanext-xloader Python package into your virtual environment::

     pip install ckanext-xloader

3. Install dependencies::

     pip install -r https://raw.githubusercontent.com/ckan/ckanext-xloader/master/requirements.txt
     pip install -U requests[security]

4. If you are using CKAN version before 2.8.x you need to define the
   ``populate_full_text_trigger`` in your database
   ::

     sudo -u postgres psql datastore_default -f full_text_function.sql

   If successful it will print
   ::

     CREATE FUNCTION
     ALTER FUNCTION

   NB this assumes you used the defaults for the database name and username.
   If in doubt, check your config's ``ckan.datastore.write_url``. If you don't have
   database name ``datastore_default`` and username ``ckan_default`` then adjust
   the psql option and ``full_text_function.sql`` before running this.

5. Add ``xloader`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

   You should also remove ``datapusher`` if it is in the list, to avoid them
   both trying to load resources into the DataStore.

   Ensure ``datastore`` is also listed, to enable CKAN DataStore.

6. Starting CKAN 2.10 you will need to set an API Token to be able to
   execute jobs against the server::

     ckanext.xloader.api_token = <your-CKAN-generated-API-Token>

7. If it is a production server, you'll want to store jobs info in a more
   robust database than the default sqlite file. It can happily use the main
   CKAN postgres db by adding this line to the config, but with the same value
   as you have for ``sqlalchemy.url``::

     ckanext.xloader.jobs_db.uri = postgresql://ckan_default:pass@localhost/ckan_default

   (This step can be skipped when just developing or testing.)

8. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu::

     sudo service apache2 reload

9. Run the worker. First test it on the command-line. If you have CKAN version 2.9 or above::
   
    ckan -c /etc/ckan/default/ckan.ini jobs worker
    
   otherwise::

     paster --plugin=ckan jobs -c /etc/ckan/default/ckan.ini worker

   or if you have CKAN version 2.6.x or less (and are therefore using ckanext-rq)::

     paster --plugin=ckanext-rq jobs -c /etc/ckan/default/ckan.ini worker

   Test it will load a CSV ok by submitting a `CSV in the web interface <http://docs.ckan.org/projects/datapusher/en/latest/using.html#ckan-2-2-and-above>`_
   or in another shell::

     paster --plugin=ckanext-xloader xloader submit <dataset-name> -c /etc/ckan/default/ckan.ini

   Clearly, running the worker on the command-line is only for testing - for
   production services see:

       http://docs.ckan.org/en/ckan-2.7.0/maintaining/background-tasks.html#using-supervisor

   If you have CKAN version 2.6.x or less then you'll need to download
   `supervisor-ckan-worker.conf <https://raw.githubusercontent.com/ckan/ckan/master/ckan/config/supervisor-ckan-worker.conf>`_ and adjust the ``command`` to reference
   ckanext-rq.


---------------
Config settings
---------------

Configuration:

::

    # The connection string for the jobs database used by XLoader. The
    # default of an sqlite file is fine for development. For production use a
    # Postgresql database.
    ckanext.xloader.jobs_db.uri = sqlite:////tmp/xloader_jobs.db

    # The formats that are accepted. If the value of the resource.format is
    # anything else then it won't be 'xloadered' to DataStore (and will therefore
    # only be available to users in the form of the original download/link).
    # Case insensitive.
    # (optional, defaults are listed in plugin.py - DEFAULT_FORMATS).
    ckanext.xloader.formats = csv application/csv xls application/vnd.ms-excel

    # The maximum size of files to load into DataStore. In bytes. Default is 1 GB.
    ckanext.xloader.max_content_length = 1000000000

    # By default, xloader will first try to add tabular data to the DataStore
    # with a direct PostgreSQL COPY. This is relatively fast, but does not
    # guess column types. If this fails, xloader falls back to a method more
    # like DataPusher's behaviour. This has the advantage that the column types
    # are guessed. However it is more error prone and far slower.
    # To always skip the direct PostgreSQL COPY and use type guessing, set
    # this option to True.
    ckanext.xloader.use_type_guessing = False

    # Deprecated: use ckanext.xloader.use_type_guessing instead.
    ckanext.xloader.just_load_with_messytables = False

    # Whether ambiguous dates should be parsed day first. Defaults to False.
    # If set to True, dates like '01.02.2022' will be parsed as day = 01,
    # month = 02.
    # NB: isoformat dates like '2022-01-02' will be parsed as YYYY-MM-DD, and
    # this option will not override that.
    # See https://dateutil.readthedocs.io/en/stable/parser.html#dateutil.parser.parse
    # for more details.
    ckanext.xloader.parse_dates_dayfirst = False

    # Whether ambiguous dates should be parsed year first. Defaults to False.
    # If set to True, dates like '01.02.03' will be parsed as year = 2001,
    # month = 02, day = 03. See https://dateutil.readthedocs.io/en/stable/parser.html#dateutil.parser.parse
    # for more details.
    ckanext.xloader.parse_dates_yearfirst = False

    # The maximum time for the loading of a resource before it is aborted.
    # Give an amount in seconds. Default is 60 minutes
    ckanext.xloader.job_timeout = 3600

    # Ignore the file hash when submitting to the DataStore, if set to True
    # resources are always submitted (if their format matches), if set to
    # False (default), resources are only submitted if their hash has changed.
    ckanext.xloader.ignore_hash = False

    # When loading a file that is bigger than `max_content_length`, xloader can
    # still try and load some of the file, which is useful to display a
    # preview. Set this option to the desired number of lines/rows that it
    # loads in this case.
    # If the file-type is supported (CSV, TSV) an excerpt with the number of
    # `max_excerpt_lines` lines will be submitted while the `max_content_length`
    # is not exceeded.
    # If set to 0 (default) files that exceed the `max_content_length` will
    # not be loaded into the datastore.
    ckanext.xloader.max_excerpt_lines = 100

    # Requests verifies SSL certificates for HTTPS requests. Setting verify to
    # False should only be enabled during local development or testing. Default
    # to True.
    ckanext.xloader.ssl_verify = True

    # Uses a specific API token for the xloader_submit action instead of the
    # apikey of the site_user
    ckanext.xloader.api_token = ckan-provided-api-token


------------------------
Developer installation
------------------------

To install XLoader for development, activate your CKAN virtualenv and
in the directory up from your local ckan repo::

    git clone https://github.com/ckan/ckanext-xloader.git
    cd ckanext-xloader
    python setup.py develop
    pip install -r requirements.txt
    pip install -r dev-requirements.txt


-------------------------
Upgrading from DataPusher
-------------------------

To upgrade from DataPusher to XLoader:

1. Install XLoader as above, including running the xloader worker.

2. (Optional) For existing datasets that have been datapushed to datastore, freeze the column types (in the data dictionaries), so that XLoader doesn't change them back to string on next xload::

       ckan -c /etc/ckan/default/ckan.ini migrate_types

3. If you've not already, change the enabled plugin in your config - on the
   ``ckan.plugins`` line replace ``datapusher`` with ``xloader``.

4. (Optional) If you wish, you can disable the direct loading and continue to
   just use tabulator - for more about this see the docs on config option:
   ``ckanext.xloader.use_type_guessing``

5. Stop the datapusher worker::

       sudo a2dissite datapusher

6. Restart CKAN::

       sudo service apache2 reload
       sudo service nginx reload

----------------------
Command-line interface
----------------------

You can submit single or multiple resources to be xloaded using the
command-line interface.

e.g. ::

    [2.9] ckan -c /etc/ckan/default/ckan.ini xloader submit <dataset-name>
    [pre-2.9] paster --plugin=ckanext-xloader xloader submit <dataset-name> -c /etc/ckan/default/ckan.ini

For debugging you can try xloading it synchronously (which does the load
directly, rather than asking the worker to do it) with the ``-s`` option::

    [2.9] ckan -c /etc/ckan/default/ckan.ini xloader submit <dataset-name> -s
    [pre-2.9] paster --plugin=ckanext-xloader xloader submit <dataset-name> -s -c /etc/ckan/default/ckan.ini

See the status of jobs::

    [2.9] ckan -c /etc/ckan/default/ckan.ini xloader status
    [pre-2.9] paster --plugin=ckanext-xloader xloader status -c /etc/ckan/default/development.ini

Submit all datasets' resources to the DataStore::

    [2.9] ckan -c /etc/ckan/default/ckan.ini xloader submit all
    [pre-2.9] paster --plugin=ckanext-xloader xloader submit all -c /etc/ckan/default/ckan.ini

Re-submit all the resources already in the DataStore (Ignores any resources
that have not been stored in DataStore e.g. because they are not tabular)::

    [2.9] ckan -c /etc/ckan/default/ckan.ini xloader submit all-existing
    [pre-2.9] paster --plugin=ckanext-xloader xloader submit all-existing -c /etc/ckan/default/ckan.ini

**Full list of XLoader CLI commands**::

    [2.9] ckan -c /etc/ckan/default/ckan.ini xloader --help
    [pre-2.9] paster --plugin=ckanext-xloader xloader --help

Jobs and workers
----------------

Main docs for managing jobs: <https://docs.ckan.org/en/latest/maintaining/background-tasks.html#managing-background-jobs>

Main docs for running and managing workers are here: https://docs.ckan.org/en/latest/maintaining/background-tasks.html#running-background-jobs

Useful commands:

Clear (delete) all outstanding jobs::

    CKAN 2.9, Python 3 ckan -c /etc/ckan/default/ckan.ini jobs clear [QUEUES]
    CKAN <2.9, Python 2 paster --plugin=ckanext-xloader xloader jobs clear [QUEUES] -c /etc/ckan/default/development.ini

If having trouble with the worker process, restarting it can help::

    sudo supervisorctl restart ckan-worker:*

---------------
Troubleshooting
---------------

**KeyError: "Action 'datastore_search' not found"**

You need to enable the `datastore` plugin in your CKAN config. See
'Installation' section above to do this and restart the worker.

**ProgrammingError: (ProgrammingError) relation "_table_metadata" does not
exist**

Your DataStore permissions have not been set-up - see:
<https://docs.ckan.org/en/latest/maintaining/datastore.html#set-permissions>

**When editing a package, all its existing resources get re-loaded by xloader**

This behavior was documented in
`Issue 75 <https://github.com/ckan/ckanext-xloader/issues/75>`_ and is related
to a bug in CKAN that is fixed in versions 2.6.9, 2.7.7, 2.8.4
and 2.9.0+.

-----------------
Running the Tests
-----------------

The first time, your test datastore database needs the trigger applied::

    sudo -u postgres psql datastore_test -f full_text_function.sql

To run the tests, do::

    nosetests --nologcapture --with-pylons=test.ini

To run the tests and produce a coverage report, first make sure you have
coverage installed in your virtualenv (``pip install coverage``) then run::

    nosetests --nologcapture --with-pylons=test.ini --with-coverage --cover-package=ckanext.xloader --cover-inclusive --cover-erase --cover-tests

----------------------------------
Releasing a New Version of XLoader
----------------------------------

XLoader is available on PyPI as https://pypi.org/project/ckanext-xloader.

To publish a new version to PyPI follow these steps:

1. Update the version number in the ``setup.py`` file.
   See `PEP 440 <http://legacy.python.org/dev/peps/pep-0440/#public-version-identifiers>`_
   for how to choose version numbers.

2. Update the CHANGELOG.

3. Make sure you have the latest version of necessary packages::

       pip install --upgrade setuptools wheel twine

4. Create source and binary distributions of the new version::

       python setup.py sdist bdist_wheel && twine check dist/*

   Fix any errors you get.

5. Upload the source distribution to PyPI::

       twine upload dist/*

6. Commit any outstanding changes::

       git commit -a
       git push

7. Tag the new release of the project on GitHub with the version number from
   the ``setup.py`` file. For example if the version number in ``setup.py`` is
   0.0.1 then do::

       git tag 0.0.1
       git push --tags
