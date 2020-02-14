import os
import json
import random
import datetime
import time
try:
    from collections import OrderedDict  # from python 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

from nose.tools import eq_, make_decorator, assert_in
import mock
import responses
from sqlalchemy import MetaData, Table
from sqlalchemy.sql import select

import ckan.plugins as p
try:
    config = p.toolkit.config
except AttributeError:
    from pylons import config

from ckanext.xloader import jobs
from ckanext.xloader import db as jobs_db
from ckanext.xloader.loader import get_write_engine
import util
try:
    from ckan.tests import helpers, factories
except ImportError:
    # older ckans
    from ckan.new_tests import helpers, factories

SOURCE_URL = 'http://www.example.com/static/file'


def mock_actions(func):
    '''
    Decorator that mocks actions used by these tests
    Based on ckan.test.helpers.mock_action
    '''
    def wrapper(*args, **kwargs):
        # Mock CKAN's resource_show API
        from ckan.logic import get_action as original_get_action

        def side_effect(called_action_name):
            if called_action_name == 'resource_show':
                def mock_resource_show(context, data_dict):
                    return {
                       'id': data_dict['id'],
                       'name': 'short name',
                       'url': SOURCE_URL,
                       'format': '',
                       'package_id': 'test-pkg',
                    }
                return mock_resource_show
            elif called_action_name == 'package_show':
                def mock_package_show(context, data_dict):
                    return {
                       'id': data_dict['id'],
                       'name': 'pkg-name',
                    }
                return mock_package_show
            else:
                return original_get_action(called_action_name)
        try:
            with mock.patch('ckanext.xloader.jobs.get_action') as mock_get_action:
                mock_get_action.side_effect = side_effect

                return_value = func(*args, **kwargs)
        finally:
            pass
            # Make sure to stop the mock, even with an exception
            # mock_action.stop()
        return return_value

    return make_decorator(func)(wrapper)


class TestxloaderDataIntoDatastore(util.PluginsMixin):
    _load_plugins = ['datastore']

    @classmethod
    def setup_class(cls):
        super(TestxloaderDataIntoDatastore, cls).setup_class()
        cls.host = 'www.ckan.org'
        cls.api_key = 'my-fake-key'
        cls.resource_id = 'foo-bar-42'
        factories.Resource(id=cls.resource_id)
        jobs_db.init(config, echo=False)
        # drop test table
        engine, conn = cls.get_datastore_engine_and_connection()
        conn.execute('DROP TABLE IF EXISTS "{}"'.format(cls.resource_id))

    @classmethod
    def teardown_class(cls):
        super(TestxloaderDataIntoDatastore, cls).teardown_class()
        if '_datastore' in dir(cls):
            connection = cls._datastore[1]
            connection.close()

    def register_urls(self, filename='simple.csv',
                      content_type='application/csv'):
        """Mock some test URLs with responses.

        Mocks some URLs related to a data file and a CKAN resource that
        contains the data file, including the URL of the data file itself and
        the resource_show, resource_update and datastore_delete URLs.

        :returns: a 2-tuple containing the URL of the data file itself and the
            resource_show URL for the resource that contains the data file

        """
        responses.add_passthru(config['solr_url'])

        # A URL that just returns a static file
        responses.add(responses.GET, SOURCE_URL,
                      body=get_sample_file(filename),
                      content_type=content_type)

        # A URL that mocks the response that CKAN's resource_update API would
        # give after successfully updating a resource.
        resource_update_url = (
            'http://www.ckan.org/api/3/action/resource_update')
        responses.add(responses.POST, resource_update_url,
                      body=json.dumps({'success': True}),
                      content_type='application/json')

        # A URL that mock's the response that CKAN's datastore plugin's
        # datastore_delete API would give after successfully deleting a
        # resource from the datastore.
        datastore_del_url = 'http://www.ckan.org/api/3/action/datastore_delete'
        responses.add(responses.POST, datastore_del_url,
                      body=json.dumps({'success': True}),
                      content_type='application/json')

        self.callback_url = 'http://www.ckan.org/api/3/action/xloader_hook'
        responses.add(responses.POST, self.callback_url,
                      body=json.dumps({'success': True}),
                      content_type='application/json')

    @classmethod
    def get_datastore_engine_and_connection(cls):
        if '_datastore' not in dir(cls):
            engine = get_write_engine()
            conn = engine.connect()
            cls._datastore = (engine, conn)
        return cls._datastore

    def get_datastore_table(self):
        engine, conn = self.get_datastore_engine_and_connection()
        meta = MetaData(bind=engine, reflect=True)
        table = Table(self.resource_id, meta,
                      autoload=True, autoload_with=engine)
        s = select([table])
        with conn.begin():
            result = conn.execute(s)
        return dict(
            num_rows=result.rowcount,
            headers=result.keys(),
            header_dict=OrderedDict([(c.key, str(c.type))
                                    for c in table.columns]),
            rows=result.fetchall(),
        )

    def get_load_logs(self, task_id):
        conn = jobs_db.ENGINE.connect()
        logs = jobs_db.LOGS_TABLE
        result = conn.execute(select([logs.c.level, logs.c.message])
                              .where(logs.c.job_id == task_id))
        return Logs(result.fetchall())

    def get_time_of_last_analyze(self):
        # When ANALYZE runs it appears to take a moment for the
        # pg_stat_user_tables to update, which we use to check analyze runs,
        # so sadly we need a sleep :(
        # DR: 0.25 is pretty reliable on my machine, but give a wide margin
        time.sleep(1)
        engine, conn = self.get_datastore_engine_and_connection()
        result = conn.execute(
            '''
            SELECT last_analyze, last_autoanalyze
            FROM pg_stat_user_tables
            WHERE relname='{}';
            '''.format(self.resource_id))
        last_analyze_datetimes = result.fetchall()[0]
        return max([x for x in last_analyze_datetimes if x] or [None])

    @mock_actions
    @responses.activate
    def test_simple_csv(self):
        # Test not only the load and xloader_hook is called at the end
        self.register_urls(filename='simple.csv')
        data = {
            'api_key': self.api_key,
            'job_type': 'xloader_to_datastore',
            'result_url': self.callback_url,
            'metadata': {
                'ckan_url': 'http://%s/' % self.host,
                'resource_id': self.resource_id
            }
        }
        job_id = 'test{}'.format(random.randint(0, 1e5))

        with mock.patch('ckanext.xloader.jobs.set_resource_metadata') \
                as mocked_set_resource_metadata:
            # in tests we call jobs directly, rather than use rq, so mock
            # get_current_job()
            with mock.patch('ckanext.xloader.jobs.get_current_job',
                            return_value=mock.Mock(id=job_id)):
                result = jobs.xloader_data_into_datastore(data)
        assert result is None, jobs_db.get_job(job_id)['error']['message']

        # Check it said it was successful
        eq_(responses.calls[-1].request.url, 'http://www.ckan.org/api/3/action/xloader_hook')
        job_dict = json.loads(responses.calls[-1].request.body)
        assert job_dict['status'] == u'complete', job_dict
        eq_(job_dict,
            {u'metadata': {u'datastore_contains_all_records_of_source_file': True,
                           u'datastore_active': True,
                           u'ckan_url': u'http://www.ckan.org/',
                           u'resource_id': u'foo-bar-42'},
             u'status': u'complete'})

        # Check the load
        data = self.get_datastore_table()
        eq_(data['headers'],
            ['_id', '_full_text', 'date', 'temperature', 'place'])
        eq_(data['header_dict']['date'], 'TEXT')
        # 'TIMESTAMP WITHOUT TIME ZONE')
        eq_(data['header_dict']['temperature'], 'TEXT')  # 'NUMERIC')
        eq_(data['header_dict']['place'], 'TEXT')  # 'TEXT')
        eq_(data['num_rows'], 6)
        eq_(data['rows'][0][2:],
            (u'2011-01-01', u'1', u'Galway'))
        # (datetime.datetime(2011, 1, 1), 1, 'Galway'))

        # Check it wanted to set the datastore_active=True
        mocked_set_resource_metadata.assert_called_once()
        eq_(mocked_set_resource_metadata.call_args[1]['update_dict'],
            {'datastore_contains_all_records_of_source_file': True,
             'datastore_active': True,
             'ckan_url': 'http://www.ckan.org/',
             'resource_id': 'foo-bar-42'})

        logs = self.get_load_logs(job_id)
        logs.assert_no_errors()

        job = jobs_db.get_job(job_id)
        eq_(job['status'], u'complete')
        eq_(job['error'], None)

        # Check ANALYZE was run
        last_analyze = self.get_time_of_last_analyze()
        assert(last_analyze)

    @mock_actions
    @responses.activate
    @mock.patch('ckanext.xloader.jobs.MAX_CONTENT_LENGTH', 10000)
    @mock.patch('ckanext.xloader.jobs.MAX_EXCERPT_LINES', 100)
    def test_too_large_csv(self):

        # Test not only the load and xloader_hook is called at the end
        self.register_urls(filename='simple-large.csv')
        data = {
            'api_key': self.api_key,
            'job_type': 'xloader_to_datastore',
            'result_url': self.callback_url,
            'metadata': {
                'ckan_url': 'http://%s/' % self.host,
                'resource_id': self.resource_id
            }
        }
        job_id = 'test{}'.format(random.randint(0, 1e5))

        with mock.patch('ckanext.xloader.jobs.set_resource_metadata') \
                as mocked_set_resource_metadata:
            # in tests we call jobs directly, rather than use rq, so mock
            # get_current_job()
            with mock.patch('ckanext.xloader.jobs.get_current_job',
                            return_value=mock.Mock(id=job_id)):
                result = jobs.xloader_data_into_datastore(data)
        assert result is None, jobs_db.get_job(job_id)['error']['message']

        # Check it said it was successful
        eq_(responses.calls[-1].request.url, 'http://www.ckan.org/api/3/action/xloader_hook')
        job_dict = json.loads(responses.calls[-1].request.body)
        assert job_dict['status'] == u'complete', job_dict
        eq_(job_dict,
            {u'metadata': {u'datastore_contains_all_records_of_source_file': False,
                           u'datastore_active': True,
                           u'ckan_url': u'http://www.ckan.org/',
                           u'resource_id': u'foo-bar-42'},
             u'status': u'complete'})

        # Check the load
        data = self.get_datastore_table()
        eq_(data['headers'],
            ['_id', '_full_text', 'id', 'text'])
        eq_(data['header_dict']['id'], 'TEXT')
        # 'TIMESTAMP WITHOUT TIME ZONE')
        eq_(data['header_dict']['text'], 'TEXT')
        assert data['num_rows'] <= 100
        assert data['num_rows'] > 0
        eq_(data['rows'][0][2:],
            (u'1', u'a'))

        # Check it wanted to set the datastore_active=True
        mocked_set_resource_metadata.assert_called_once()
        eq_(mocked_set_resource_metadata.call_args[1]['update_dict'],
            {'datastore_contains_all_records_of_source_file': False,
             'datastore_active': True,
             'ckan_url': 'http://www.ckan.org/',
             'resource_id': 'foo-bar-42'})

        logs = self.get_load_logs(job_id)
        logs.assert_no_errors()

        job = jobs_db.get_job(job_id)
        eq_(job['status'], u'complete')
        eq_(job['error'], None)

        # Check ANALYZE was run
        last_analyze = self.get_time_of_last_analyze()
        assert(last_analyze)

    @mock_actions
    @responses.activate
    @mock.patch('ckanext.xloader.jobs.MAX_CONTENT_LENGTH', 10000)
    @mock.patch('ckanext.xloader.jobs.MAX_EXCERPT_LINES', 100)
    def test_too_large_xls(self):

        # Test not only the load and xloader_hook is called at the end
        self.register_urls(filename='simple-large.xls')
        data = {
            'api_key': self.api_key,
            'job_type': 'xloader_to_datastore',
            'result_url': self.callback_url,
            'metadata': {
                'ckan_url': 'http://%s/' % self.host,
                'resource_id': self.resource_id
            }
        }
        job_id = 'test{}'.format(random.randint(0, 1e5))

        with mock.patch('ckanext.xloader.jobs.set_resource_metadata'):
            # in tests we call jobs directly, rather than use rq, so mock
            # get_current_job()
            with mock.patch('ckanext.xloader.jobs.get_current_job',
                            return_value=mock.Mock(id=job_id)):
                result = jobs.xloader_data_into_datastore(data)
        assert result is not None, jobs_db.get_job(job_id)['error']['message']

        # Check it said it was successful
        eq_(responses.calls[-1].request.url,
            'http://www.ckan.org/api/3/action/xloader_hook')
        job_dict = json.loads(responses.calls[-1].request.body)
        assert job_dict['status'] == u'error', job_dict
        eq_(job_dict,
            {u'status': u'error',
             u'metadata': {u'ckan_url': u'http://www.ckan.org/',
                           u'datastore_contains_all_records_of_source_file': False,
                           u'resource_id': u'foo-bar-42'},
             u'error': u'Loading file raised an error: array index out of range'})

        job = jobs_db.get_job(job_id)
        eq_(job['status'], u'error')
        eq_(job['error'], {u'message': u'Loading file raised an error: array index out of range'})

    @mock_actions
    @responses.activate
    def test_messytables(self):
        # xloader's COPY can't handle xls, so it will be dealt with by
        # messytables
        self.register_urls(filename='simple.xls',
                           content_type='application/vnd.ms-excel')
        data = {
            'api_key': self.api_key,
            'job_type': 'xloader_to_datastore',
            'result_url': self.callback_url,
            'metadata': {
                'ckan_url': 'http://%s/' % self.host,
                'resource_id': self.resource_id
            }
        }
        job_id = 'test{}'.format(random.randint(0, 1e5))

        with mock.patch('ckanext.xloader.jobs.set_resource_metadata') \
                as mocked_set_resource_metadata:
            # in tests we call jobs directly, rather than use rq, so mock
            # get_current_job()
            with mock.patch('ckanext.xloader.jobs.get_current_job',
                            return_value=mock.Mock(id=job_id)):
                result = jobs.xloader_data_into_datastore(data)
        eq_(result, None)

        # Check it said it was successful
        eq_(responses.calls[-1].request.url, 'http://www.ckan.org/api/3/action/xloader_hook')
        job_dict = json.loads(responses.calls[-1].request.body)
        assert job_dict['status'] == u'complete', job_dict
        eq_(job_dict,
            {u'metadata': {u'datastore_contains_all_records_of_source_file': True,
                           u'datastore_active': True,
                           u'ckan_url': u'http://www.ckan.org/',
                           u'resource_id': u'foo-bar-42'},
             u'status': u'complete'})

        # Check the load
        data = self.get_datastore_table()
        eq_(data['headers'],
            ['_id', '_full_text', 'date', 'temperature', 'place'])
        eq_(data['header_dict']['date'], 'TIMESTAMP WITHOUT TIME ZONE')
        eq_(data['header_dict']['temperature'], 'NUMERIC')
        eq_(data['header_dict']['place'], 'TEXT')
        eq_(data['num_rows'], 6)
        eq_(data['rows'][0][2:],
            (datetime.datetime(2011, 1, 1), 1, u'Galway'))

        # Check it wanted to set the datastore_active=True
        mocked_set_resource_metadata.assert_called_once()
        eq_(mocked_set_resource_metadata.call_args[1]['update_dict'],
            {'ckan_url': 'http://www.ckan.org/',
             'datastore_contains_all_records_of_source_file': True,
             'datastore_active': True,
             'resource_id': 'foo-bar-42'})

        # check logs have the error doing the COPY
        logs = self.get_load_logs(job_id)
        copy_error_index = None
        for i, log in enumerate(logs):
            if log[0] == 'WARNING' and log[1].startswith('Load using COPY failed: Error during the load into PostgreSQL'):
                copy_error_index = i
                break
        assert copy_error_index, 'Missing COPY error'

        # check messytable portion of the logs
        logs = Logs(logs[copy_error_index + 1:])
        eq_(logs[0], (u'INFO', u'Trying again with messytables'))
        logs.assert_no_errors()

        # Check ANALYZE was run
        last_analyze = self.get_time_of_last_analyze()
        assert(last_analyze)

    @mock_actions
    @responses.activate
    def test_umlaut_and_extra_comma(self):
        self.register_urls(filename='umlaut_and_extra_comma.csv')
        # This csv has an extra comma which causes the COPY to throw a
        # psycopg2.DataError and the umlaut can cause problems for logging the
        # error. We need to check that it correctly reverts to using
        # messytables to load it
        data = {
            'api_key': self.api_key,
            'job_type': 'xloader_to_datastore',
            'result_url': self.callback_url,
            'metadata': {
                'ckan_url': 'http://%s/' % self.host,
                'resource_id': self.resource_id
            }
        }
        job_id = 'test{}'.format(random.randint(0, 1e5))

        with mock.patch('ckanext.xloader.jobs.set_resource_metadata'):
            # in tests we call jobs directly, rather than use rq, so mock
            # get_current_job()
            with mock.patch('ckanext.xloader.jobs.get_current_job',
                            return_value=mock.Mock(id=job_id)):
                result = jobs.xloader_data_into_datastore(data)
        assert result is None, jobs_db.get_job(job_id)['error']['message']

        # Check it said it was successful
        eq_(responses.calls[-1].request.url, 'http://www.ckan.org/api/3/action/xloader_hook')
        job_dict = json.loads(responses.calls[-1].request.body)
        assert job_dict['status'] == u'complete', job_dict
        eq_(job_dict,
            {u'metadata': {u'datastore_contains_all_records_of_source_file': True,
                           u'datastore_active': True,
                           u'ckan_url': u'http://www.ckan.org/',
                           u'resource_id': u'foo-bar-42'},
             u'status': u'complete'})

        logs = self.get_load_logs(job_id)
        logs.assert_no_errors()

        job = jobs_db.get_job(job_id)
        eq_(job['status'], u'complete')
        eq_(job['error'], None)

    @mock_actions
    @responses.activate
    def test_first_request_is_202_pending_response(self):
        # when you first get the CSV it returns this 202 response, which is
        # what this server does: https://data-cdfw.opendata.arcgis.com/datasets
        responses.add(responses.GET, SOURCE_URL,
                      status=202,
                      body='{"processingTime":"8.716 seconds","status":"Processing","generating":{}}',
                      content_type='application/json')
        # subsequent GETs of the CSV work fine
        self.register_urls()
        data = {
            'api_key': self.api_key,
            'job_type': 'xloader_to_datastore',
            'result_url': self.callback_url,
            'metadata': {
                'ckan_url': 'http://%s/' % self.host,
                'resource_id': self.resource_id
            }
        }
        job_id = 'test{}'.format(random.randint(0, 1e5))

        with mock.patch('ckanext.xloader.jobs.set_resource_metadata') \
                as mocked_set_resource_metadata:
            # in tests we call jobs directly, rather than use rq, so mock
            # get_current_job()
            with mock.patch('ckanext.xloader.jobs.get_current_job',
                            return_value=mock.Mock(id=job_id)):
                result = jobs.xloader_data_into_datastore(data)
        assert result is None, jobs_db.get_job(job_id)['error']['message']

        # Check it said it was successful
        eq_(responses.calls[-1].request.url, 'http://www.ckan.org/api/3/action/xloader_hook')
        job_dict = json.loads(responses.calls[-1].request.body)
        assert job_dict['status'] == u'complete', job_dict
        eq_(job_dict,
            {u'metadata': {u'ckan_url': u'http://www.ckan.org/',
                           u'datastore_contains_all_records_of_source_file': True,
                           u'datastore_active': True,
                           u'resource_id': u'foo-bar-42'},
             u'status': u'complete'})

        # Check the load
        data = self.get_datastore_table()
        eq_(data['headers'],
            ['_id', '_full_text', 'date', 'temperature', 'place'])
        eq_(data['header_dict']['date'], 'TEXT')
        # 'TIMESTAMP WITHOUT TIME ZONE')
        eq_(data['header_dict']['temperature'], 'TEXT')  # 'NUMERIC')
        eq_(data['header_dict']['place'], 'TEXT')  # 'TEXT')
        eq_(data['num_rows'], 6)
        eq_(data['rows'][0][2:],
            (u'2011-01-01', u'1', u'Galway'))
        # (datetime.datetime(2011, 1, 1), 1, 'Galway'))

        # Check it wanted to set the datastore_active=True
        mocked_set_resource_metadata.assert_called_once()
        eq_(mocked_set_resource_metadata.call_args[1]['update_dict'],
            {'datastore_contains_all_records_of_source_file': True,
             'datastore_active': True,
             'ckan_url': 'http://www.ckan.org/',
             'resource_id': 'foo-bar-42'})

        logs = self.get_load_logs(job_id)
        logs.assert_no_errors()

        job = jobs_db.get_job(job_id)
        eq_(job['status'], u'complete')
        eq_(job['error'], None)


class Logs(list):
    def get_errors(self):
        return [message for level, message in self
                if level == 'ERROR']

    def grep(self, text):
        return [message for level, message in self
                if text in message]

    def assert_no_errors(self):
        errors = self.get_errors()
        assert not errors, errors


def get_sample_file(filename):
    filepath = os.path.join(os.path.dirname(__file__), 'samples', filename)
    return open(filepath).read()


class TestSetResourceMetadata(object):
    @classmethod
    def setup_class(cls):
        helpers.reset_db()

    def test_simple(self):
        resource = factories.Resource()

        jobs.set_resource_metadata(
            {'datastore_contains_all_records_of_source_file': True,
             'datastore_active': True,
             'ckan_url': 'http://www.ckan.org/',
             'resource_id': resource['id']})

        resource = helpers.call_action('resource_show', id=resource['id'])
        from pprint import pprint
        pprint(resource)
        assert_in(resource['datastore_contains_all_records_of_source_file'],
                  (True, u'True'))
        # I'm not quite sure why this is a string on travis - I get the bool
        # locally

        eq_(resource['datastore_active'], True)
        eq_(resource['ckan_url'], 'http://www.ckan.org/')
