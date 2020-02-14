#!/bin/bash
set -ex

flake8 --version
# stop the build if there are Python syntax errors or undefined names
flake8 . --count --select=E901,E999,F821,F822,F823 --show-source --statistics --exclude ckan,ckanext-xloader

if [ $CKANVERSION == 'master' ]
then
    export CKAN_MINOR_VERSION=100
else
    export CKAN_MINOR_VERSION=${CKANVERSION##*.}
fi


if (( $CKAN_MINOR_VERSION >= 9 ))
then
    pytest --ckan-ini=subdir/test.ini --cov=ckanext.xloader ckanext/xloader/tests
else
    nosetests --ckan --nologcapture --with-pylons=subdir/test-nose.ini --with-coverage --cover-package=ckanext.dcat --cover-inclusive --cover-erase --cover-tests ckanext/xloader/tests/nose
fi