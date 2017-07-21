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

     pip install ckanext-shift

3. Add ``shift`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu::

     sudo service apache2 reload


---------------
Config Settings
---------------

Configuration:

    .. # The minimum number of hours to wait before re-checking a resource
    .. # (optional, default: 24).
    .. ckanext.shift.url =

::

    # The formats that are accepted. If the value of the resource.format is
    # anything else then it won't be 'shifted' to DataStore (and will therefore
    # only be available to users in the form of the original download/link).
    # Case insensitive.
    # (optional, defaults are listed in plugin.py - FORMATS).
    ckanext.shift.formats = csv application/csv xls application/vnd.ms-excel

    # The maximum size of files to load into DataStore. In bytes. Default is 1MB
    # (i.e. 10485760 bytes)
    ckanext.shift.max_content_length = 20000000


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
