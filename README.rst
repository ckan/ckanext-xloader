.. You should enable this project on travis-ci.org and coveralls.io to make
   these badges work. The necessary Travis and Coverage config files have been
   generated for you.

.. image:: https://travis-ci.org/davidread/ckanext-shift.svg?branch=master
    :target: https://travis-ci.org/davidread/ckanext-shift

.. image:: https://coveralls.io/repos/davidread/ckanext-shift/badge.svg
  :target: https://coveralls.io/r/davidread/ckanext-shift

.. image:: https://pypip.in/download/ckanext-shift/badge.svg
    :target: https://pypi.python.org/pypi//ckanext-shift/
    :alt: Downloads

.. image:: https://pypip.in/version/ckanext-shift/badge.svg
    :target: https://pypi.python.org/pypi/ckanext-shift/
    :alt: Latest Version

.. image:: https://pypip.in/py_versions/ckanext-shift/badge.svg
    :target: https://pypi.python.org/pypi/ckanext-shift/
    :alt: Supported Python versions

.. image:: https://pypip.in/status/ckanext-shift/badge.svg
    :target: https://pypi.python.org/pypi/ckanext-shift/
    :alt: Development Status

.. image:: https://pypip.in/license/ckanext-shift/badge.svg
    :target: https://pypi.python.org/pypi/ckanext-shift/
    :alt: License

=============
ckanext-shift
=============

Loads CSV (and similar) data into DataStore. Designed as a replacement for DataPusher.

-------------------------------
Key differences from DataPusher
-------------------------------

Simpler queueing tech
----------------------

DataPusher - job queue is done by ckan-service-provider which is bespoke, complicated and stores jobs in its own database (sqlite by default).

ckanext-shift - job queue is done by RQ, which is simpler and is backed by Redis and allows access to the CKAN model. You can also debug jobs easily using pdb. Job results are currently still stored in its own database, but the intention is to move this relatively small amount of data into CKAN's database, to reduce the complication of install.

(The other obvious candidate is Celery, but we don't need its heavyweight architecture and its jobs are not debuggable with pdb.)

Speed of loading
----------------

DataPusher - parses CSV rows, converts to detected column types, converts the data to a JSON string, calls datastore_create for each batch of rows, which reformats the data into an INSERT statement string, which is passed to PostgreSQL.

ckanext-shift - pipes the CSV file directly into PostgreSQL using COPY.

We tested the load of a very large CSV (1000000 rows, 475MB):
* DataPusher: 35 minutes
* ckanext-shift: 1 minute

Robustness
----------

DataPusher - one cause of failure was when casting cells. The type of a column was detected on the values of the first few rows, so if a column is mainly numeric or dates, but a string (like "Null") comes later on, then this will cause the load to finish at that point.

ckanext-shift - loads all the cells as text, before allowing the admin to convert columns to the types they want (Data Dictionary feature). In future it could do automatic detection and conversion.

Separate web server
-------------------

DataPusher - has the complication that the queue jobs are done by a separate (Flask) web app, aside from CKAN. This was the design because the job requires intensive processing to convert every line of the data into JSON. However it means more complicated code as info needs to be passed between the services in http requests, more for the user to set-up and manage - another app config, another apache config, separate log files.

ckanext-shift - the job runs in a worker process, in the same app as CKAN, so can access the CKAN config, db and logging directly and avoids many HTTP calls. This simplification makes sense because the shift job doesn't need to do much processing - mainly it is streaming the CSV file from disk into PostgreSQL.

Not yet complete
----------------

* Only supports CSVs, not XLS etc
* No support for private datasets
* Once loaded in Datastore, search is not yet working.


------------
Requirements
------------

Works with CKAN 2.7.0 and later.

Works with CKAN 2.3.x - 2.6.x if you install ckanext-rq.


------------
Installation
------------

To install ckanext-shift:

1. Activate your CKAN virtual environment, for example::

     . /usr/lib/ckan/default/bin/activate

2. Install the ckanext-shift Python package into your virtual environment::

..     pip install ckanext-shift
     pip install git+https://github.com/davidread/ckanext-shift.git

3. Install dependencies::

     pip install -r requirements.txt
     pip install -U requests[security]

4. If you are using CKAN version before 2.8 you need to define the
   `populate_full_text_trigger` in your database::

     sudo -u postgres psql datastore_default -f full_text_function.sql

   If successful it will print::

     CREATE FUNCTION
     ALTER FUNCTION

   NB this assumes you used the defaults for the database name and username.
   If in doubt, check your config's ckan.datastore.write_url. If you don't have
   database name `datastore_default` and username `ckan_default` then adjust
   the psql option and full_text_function.sql before running this.

5. Add ``shift`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

   You should also remove ``datapusher`` if it is in the list, to avoid them
   both trying to load resources into the DataStore.

6. If it is a production server, you'll want to store jobs info in a more robust
   database than the default sqlite file::

     sudo -u postgres createdb -O ckan_default shift_jobs -E utf-8

   And add this list to the config::

     ckanext.shift.jobs_db.uri postgresql://ckan_default:pass@localhost/shift_jobs

   (This step can be skipped when just developing or testing.)

7. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu::

     sudo service apache2 reload


---------------
Config Settings
---------------

Configuration:

    .. # The minimum number of hours to wait before re-checking a resource
    .. # (optional, default: 24).
    .. ckanext.shift.url =

::

    # The connection string for the jobs database used by ckanext-shift. The
    # default of an sqlite file is fine for development. For production use a
    # Postgresql database.
    ckanext.shift.jobs_db.uri = sqlite:////tmp/shift_jobs.db

    # The formats that are accepted. If the value of the resource.format is
    # anything else then it won't be 'shifted' to DataStore (and will therefore
    # only be available to users in the form of the original download/link).
    # Case insensitive.
    # (optional, defaults are listed in plugin.py - FORMATS).
    ckanext.shift.formats = csv application/csv
    # In future we hope to add: xls application/vnd.ms-excel

    # The maximum size of files to load into DataStore. In bytes. Default is 1MB
    # (i.e. 10485760 bytes)
    ckanext.shift.max_content_length = 100000000


------------------------
Development Installation
------------------------

To install ckanext-shift for development, activate your CKAN virtualenv and
do::

    git clone https://github.com/davidread/ckanext-shift.git
    cd ckanext-shift
    python setup.py develop
    pip install -r dev-requirements.txt

-------------------------
Upgrading from DataPusher
-------------------------

To upgrade from DataPusher to ckanext-shift:

1. In your config, on the `ckan.plugins` line replace `datapusher` with `shift`.

TBC

-----------------
Running the Tests
-----------------

To run the tests, do::

    nosetests --nologcapture --with-pylons=test.ini

If you get error `function populate_full_text_trigger() does not exist` then
you need a CKAN with https://github.com/ckan/ckan/pull/3786. (Even if you create the function on the test database, it gets cleared by: https://github.com/ckan/ckan/pull/3786/files#diff-33d20faeb53559a9b8940bcb418cb5b4R75 )

.. To run the tests and produce a coverage report, first make sure you have
.. coverage installed in your virtualenv (``pip install coverage``) then run::

..     nosetests --nologcapture --with-pylons=test.ini --with-coverage --cover-package=ckanext.shift --cover-inclusive --cover-erase --cover-tests


.. ---------------------------------
.. Registering ckanext-shift on PyPI
.. ---------------------------------

.. ckanext-shift should be availabe on PyPI as
.. https://pypi.python.org/pypi/ckanext-shift. If that link doesn't work, then
.. you can register the project on PyPI for the first time by following these
.. steps:

.. 1. Create a source distribution of the project::

..      python setup.py sdist

.. 2. Register the project::

..      python setup.py register

.. 3. Upload the source distribution to PyPI::

..      python setup.py sdist upload

.. 4. Tag the first release of the project on GitHub with the version number from
..    the ``setup.py`` file. For example if the version number in ``setup.py`` is
..    0.0.1 then do::

..        git tag 0.0.1
..        git push --tags


.. ----------------------------------------
.. Releasing a New Version of ckanext-shift
.. ----------------------------------------

.. ckanext-shift is availabe on PyPI as https://pypi.python.org/pypi/ckanext-shift.
.. To publish a new version to PyPI follow these steps:

.. 1. Update the version number in the ``setup.py`` file.
..    See `PEP 440 <http://legacy.python.org/dev/peps/pep-0440/#public-version-identifiers>`_
..    for how to choose version numbers.

.. 2. Create a source distribution of the new version::

..      python setup.py sdist

.. 3. Upload the source distribution to PyPI::

..      python setup.py sdist upload

.. 4. Tag the new release of the project on GitHub with the version number from
..    the ``setup.py`` file. For example if the version number in ``setup.py`` is
..    0.0.2 then do::

..        git tag 0.0.2
..        git push --tags
