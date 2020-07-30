#!/bin/bash
set -e

echo "This is travis-build.bash..."

echo "Installing the packages that CKAN requires..."
sudo apt-get update -qq
sudo apt-get install -y solr-jetty libcommons-fileupload-java

# !/bin/bash
ver=$(python -c"import sys; print(sys.version_info.major)")
if [ $ver -eq 2 ]; then
    echo "python version 2"
elif [ $ver -eq 3 ]; then
    echo "python version 3"
else
    echo "Unknown python version: $ver"
fi

cliCommand=paster

echo "Installing CKAN and its Python dependencies..."
if [ ! -d ckan ]; then

  echo "Creating the PostgreSQL user and database..."
  sudo -u postgres psql -c "CREATE USER ckan_default WITH PASSWORD 'pass';"
  sudo -u postgres psql -c 'CREATE DATABASE ckan_test WITH OWNER ckan_default;'
  sudo -u postgres psql -c "CREATE USER datastore_default WITH PASSWORD 'pass';"
  sudo -u postgres psql -c 'CREATE DATABASE datastore_test WITH OWNER datastore_default;'

  echo "Create full text function..."
  cp full_text_function.sql /tmp
  pushd /tmp
  sudo -u postgres psql datastore_test -f full_text_function.sql
  popd

  if [ "${CKAN_BRANCH}dd" == 'dd' ]; then
    #remote lookup tags, and get latest by version-sort
    CKAN_TAG=$(git ls-remote --tags https://github.com/$CKAN_GIT_REPO/ckan | grep refs/tags/ckan-$CKANVERSION | awk '{print $2}'| sort --version-sort | tail -n 1 | sed  's|refs/tags/||' )
    echo "CKAN tag version $CKANVERSION is: ${CKAN_TAG#ckan-}"
    git clone --depth=50 --branch=$CKAN_TAG https://github.com/$CKAN_GIT_REPO/ckan ckan
    if [ $CKANVERSION \< '2.9' ]
    then
       cliCommand=paster
    else
        cliCommand=ckan
    fi
  else
    echo "CKAN version: $CKAN_BRANCH"
    git clone --depth=50 --branch=$CKAN_BRANCH https://github.com/$CKAN_GIT_REPO/ckan ckan
    #Master is on 2.9+ so needs ckan for cliCommand
    if [ $CKAN_BRANCH == 'master' ]
    then
       cliCommand=ckan
    fi
  fi
fi

pushd ckan

if [ $ver -eq 3 ]; then
# install the recommended version of setuptools
if [ -f requirement-setuptools.txt ]
then
    echo "Updating setuptools..."
    pip install -r requirement-setuptools.txt
fi
fi

if [ $CKANVERSION == '2.7' ]
then
    echo "Installing setuptools"
    pip install setuptools==39.0.1
fi

if [ -f requirements-py2.txt ] && [ $ver -eq 2 ]; then
    pip install -r requirements-py2.txt
else
    pip install -r requirements.txt
fi
pip install -r dev-requirements.txt
python setup.py develop

echo "Initialising the database..."
if [ "$cliCommand" == "paster" ]; then
  paster db init -c test-core.ini
  paster datastore set-permissions -c test-core.ini | sudo -u postgres psql
else
  #ckan comand has config first then options.
  ckan -c test-core.ini db init
  ckan -c test-core.ini datastore set-permissions  | sudo -u postgres psql
fi
popd


echo "SOLR config..."
sudo cp ckan/ckan/config/solr/schema.xml /etc/solr/conf/schema.xml

# solr is multicore for tests on ckan master now, but it's easier to run tests
# on Travis single-core still.
# see https://github.com/ckan/ckan/issues/2972
sed -i -e 's/solr_url.*/solr_url = http:\/\/127.0.0.1:8983\/solr/' ckan/test-core.ini


echo "Installing ckanext-xloader and its requirements..."
if [ -f requirements-py2.txt ] && [ $ver -eq 2 ]; then
  pip install -r requirements-py2.txt
elif [ -f requirements.txt ]; then
  pip install -r requirements.txt
fi
pip install -r dev-requirements.txt
python setup.py develop

echo "Moving test.ini into a subdir..."
mkdir -p subdir
cp test.ini subdir

echo "start solr on $TRAVIS_DIST"

#ensure we handle jetty8 and jetty9
if [[ "$TRAVIS_DIST" == "trusty" ]]; then
  printf "NO_START=0\nJETTY_HOST=127.0.0.1\nJETTY_PORT=8983\nJAVA_HOME=$JAVA_HOME" | sudo tee /etc/default/jetty
  sudo service jetty restart

else
  # expect we are in current os, i.e. bionic
  #[ "$TRAVIS_DIST" == "bionic" ]; then
  # Fix solr-jetty starting issues https://stackoverflow.com/a/56007895
  # https://github.com/Zharktas/ckanext-report/blob/py3/bin/travis-run.bash
  sudo mkdir -p /etc/systemd/system/jetty9.service.d
  printf "[Service]\nReadWritePaths=/var/lib/solr" | sudo tee /etc/systemd/system/jetty9.service.d/solr.conf
  sed '16,21d' /etc/solr/solr-jetty.xml | sudo tee /etc/solr/solr-jetty.xml
  sudo systemctl daemon-reload || echo "all good"
  printf "NO_START=0\nJETTY_HOST=127.0.0.1\nJETTY_ARGS=\"jetty.http.port=8983\"\nJAVA_HOME=$JAVA_HOME" | sudo tee /etc/default/jetty9
  sudo service jetty9 restart
fi

# Wait for jetty to start
timeout 20 bash -c 'while [[ "$(curl -s -o /dev/null -I -w %{http_code} http://localhost:8983)" != "200" ]]; do sleep 2;done'


echo "travis-build.bash is done."
