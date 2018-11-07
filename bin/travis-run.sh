#!/bin/sh -e

nosetests --ckan \
          --with-pylons=subdir/test.ini \
          --with-coverage \
          --cover-package=ckanext.xloader \
          --cover-inclusive \
          --cover-erase \
          --cover-tests
