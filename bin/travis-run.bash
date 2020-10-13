#!/bin/bash
set -ex

pip install future
pytest --ckan-ini=subdir/test.ini --cov=ckanext.xloader --disable-warnings ckanext/xloader/tests