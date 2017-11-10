.. You should enable this project on travis-ci.org and coveralls.io to make
   these badges work. The necessary Travis and Coverage config files have been
   generated for you.

.. .. image:: https://travis-ci.org/davidread/ckanext-xloader.svg?branch=master
..     :target: https://travis-ci.org/davidread/ckanext-xloader

.. .. image:: https://coveralls.io/repos/davidread/ckanext-xloader/badge.svg
..   :target: https://coveralls.io/r/davidread/ckanext-xloader

.. image:: https://pypip.in/download/ckanext-xloader/badge.svg
    :target: https://pypi.python.org/pypi//ckanext-xloader/
    :alt: Downloads

.. image:: https://pypip.in/version/ckanext-xloader/badge.svg
    :target: https://pypi.python.org/pypi/ckanext-xloader/
    :alt: Latest Version

.. image:: https://pypip.in/py_versions/ckanext-xloader/badge.svg
    :target: https://pypi.python.org/pypi/ckanext-xloader/
    :alt: Supported Python versions

.. image:: https://pypip.in/status/ckanext-xloader/badge.svg
    :target: https://pypi.python.org/pypi/ckanext-xloader/
    :alt: Development Status

.. image:: https://pypip.in/license/ckanext-xloader/badge.svg
    :target: https://pypi.python.org/pypi/ckanext-xloader/
    :alt: License

================================
Express Loader - ckanext-xloader
================================

Loads CSV (and similar) data into CKAN's DataStore. Designed as a replacement for DataPusher because it offers ten times the speed and more robustness.

**OpenGov Inc.** has sponsored this development, with the aim of benefiting open data infrastructure worldwide.

-------------------------------
Key differences from DataPusher
-------------------------------

Speed of loading
----------------

DataPusher - parses CSV rows, converts to detected column types, converts the data to a JSON string, calls datastore_create for each batch of rows, which reformats the data into an INSERT statement string, which is passed to PostgreSQL.

Express Loader - pipes the CSV file directly into PostgreSQL using COPY.

In `tests <https://github.com/davidread/ckanext-xloader/issues/25>`_, Express Loader is over ten times faster than DataPusher.

Robustness
----------

DataPusher - one cause of failure was when casting cells to a guessed type. The type of a column was decided by looking at the values of only the first few rows. So if a column is mainly numeric or dates, but a string (like "N/A") comes later on, then this will cause the load to error at that point, leaving it half-loaded into DataStore.

Express Loader - loads all the cells as text, before allowing the admin to convert columns to the types they want (using the Data Dictionary feature). In future it could do automatic detection and conversion.

Simpler queueing tech
----------------------

DataPusher - job queue is done by ckan-service-provider which is bespoke, complicated and stores jobs in its own database (sqlite by default).

Express Loader - job queue is done by RQ, which is simpler and is backed by Redis and allows access to the CKAN model. You can also debug jobs easily using pdb. Job results are currently still stored in its own database, but the intention is to move this relatively small amount of data into CKAN's database, to reduce the complication of install.

(The other obvious candidate is Celery, but we don't need its heavyweight architecture and its jobs are not debuggable with pdb.)

Separate web server
-------------------

DataPusher - has the complication that the queue jobs are done by a separate (Flask) web app, apart from CKAN. This was the design because the job requires intensive processing to convert every line of the data into JSON. However it means more complicated code as info needs to be passed between the services in http requests, more for the user to set-up and manage - another app config, another apache config, separate log files.

Express Loader - the job runs in a worker process, in the same app as CKAN, so can access the CKAN config, db and logging directly and avoids many HTTP calls. This simplification makes sense because the xloader job doesn't need to do much processing - mainly it is streaming the CSV file from disk into PostgreSQL.

Caveats
-------

* All columns are loaded as 'text' type. However an admin can use the resource's Data Dictionary tab (CKAN 2.7 onwards) to change these to numeric or datestamp and re-load the file. There is scope to do this automatically in future.
* No support yet for private datasets


------------
Requirements
------------

Works with CKAN 2.7.x and later.

Works with CKAN 2.3.x - 2.6.x if you install ckanext-rq.


------------
Installation
------------

To install Express Loader:

1. Activate your CKAN virtual environment, for example::

     . /usr/lib/ckan/default/bin/activate

2. Install the ckanext-xloader Python package into your virtual environment::

     pip install ckanext-xloader

3. Install dependencies::

     pip install -r requirements.txt
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
   If in doubt, check your config's ckan.datastore.write_url. If you don't have
   database name ``datastore_default`` and username ``ckan_default`` then adjust
   the psql option and full_text_function.sql before running this.

5. Add ``xloader`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

   You should also remove ``datapusher`` if it is in the list, to avoid them
   both trying to load resources into the DataStore.

6. If it is a production server, you'll want to store jobs info in a more robust
   database than the default sqlite file::

     sudo -u postgres createdb -O ckan_default xloader_jobs -E utf-8

   And add this list to the config::

     ckanext.xloader.jobs_db.uri postgresql://ckan_default:pass@localhost/xloader_jobs

   (This step can be skipped when just developing or testing.)

7. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu::

     sudo service apache2 reload

8. Run the worker. First test it on the command-line::

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
Config Settings
---------------

Configuration:

::

    # The connection string for the jobs database used by Express Loader. The
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

    # The maximum time for the loading of a resource before it is aborted.
    # Give an amount in seconds. Default is 60 minutes
    ckanext.xloader.job_timeout = 3600

------------------------
Development Installation
------------------------

To install Express Loader for development, activate your CKAN virtualenv and
in the directory up from your local ckan repo::

    git clone https://github.com/davidread/ckanext-xloader.git
    cd ckanext-xloader
    python setup.py develop
    pip install -r requirements.txt
    pip install -r dev-requirements.txt


-------------------------
Upgrading from DataPusher
-------------------------

To upgrade from DataPusher to Express Loader:

1. Install Express Loader as above, including running the xloader worker.

2. If you've not already, change the enabled plugin in your config - on the
   ``ckan.plugins`` line replace ``datapusher`` with ``xloader``.

3. Stop the datapusher worker::

       sudo a2dissite datapusher

4. Restart CKAN::

       sudo service apache2 reload
       sudo service nginx reload

-----------------
Running the Tests
-----------------

To run the tests, do::

    nosetests --nologcapture --with-pylons=test.ini

To run the tests and produce a coverage report, first make sure you have
coverage installed in your virtualenv (``pip install coverage``) then run::

    nosetests --nologcapture --with-pylons=test.ini --with-coverage --cover-package=ckanext.xloader --cover-inclusive --cover-erase --cover-tests

-----------------------------------------
Releasing a New Version of Express Loader
-----------------------------------------

Express Loader is availabe on PyPI as https://pypi.python.org/pypi/ckanext-xloader.
To publish a new version to PyPI follow these steps:

1. Update the version number in the ``setup.py`` file.
   See `PEP 440 <http://legacy.python.org/dev/peps/pep-0440/#public-version-identifiers>`_
   for how to choose version numbers.

2. Create a source distribution of the new version::

     python setup.py sdist

3. Upload the source distribution to PyPI::

     python setup.py sdist upload

4. Tag the new release of the project on GitHub with the version number from
   the ``setup.py`` file. For example if the version number in ``setup.py`` is
   0.0.2 then do::

       git tag 0.0.2
       git push --tags
