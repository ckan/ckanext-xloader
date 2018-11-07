# encoding: utf-8

import os
import json
import random
try:
    from collections import OrderedDict  # from python 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

from nose.tools import eq_, make_decorator
import mock
import responses
from sqlalchemy import MetaData, Table
from sqlalchemy.sql import select
from pylons import config

from ckanext.xloader import jobs
from ckanext.xloader import db as jobs_db
from ckanext.xloader.loader import get_write_engine
import util
from ckan.tests import factories

SOURCE_URL = 'http://www.example.com/static/file'

def mock_actions(resource_url=None):
    def decorator(func):
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
                           'url': resource_url,
                           'format': 'CSV',
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
    return decorator


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
                      content_type='application/csv',
                      resource_url=None):
        """Mock some test URLs with responses.

        Mocks some URLs related to a data file and a CKAN resource that
        contains the data file, including the URL of the data file itself and
        the resource_show, resource_update and datastore_delete URLs.

        :returns: a 2-tuple containing the URL of the data file itself and the
            resource_show URL for the resource that contains the data file

        """
        if not resource_url:
            resource_url = SOURCE_URL

        responses.add_passthru(config['solr_url'])

        # A URL that just returns a static file
        responses.add(responses.GET, resource_url,
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
        with conn.begin() as trans:
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

    @mock_actions(resource_url=SOURCE_URL)
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

        with mock.patch('ckanext.xloader.jobs.set_datastore_active_flag') \
                as mocked_set_datastore_active_flag:
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
        mocked_set_datastore_active_flag.assert_called_once()
        eq_(mocked_set_datastore_active_flag.call_args[1]['data_dict'],
            {'ckan_url': 'http://www.ckan.org/', 'resource_id': 'foo-bar-42'})

        logs = self.get_load_logs(job_id)
        logs.assert_no_errors()

        job = jobs_db.get_job(job_id)
        eq_(job['status'], u'complete')
        eq_(job['error'], None)

    @mock_actions(resource_url=SOURCE_URL)
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

        with mock.patch('ckanext.xloader.jobs.set_datastore_active_flag') \
                as mocked_set_datastore_active_flag:
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
                           u'resource_id': u'foo-bar-42'},
             u'status': u'complete'})

        logs = self.get_load_logs(job_id)
        logs.assert_no_errors()

        job = jobs_db.get_job(job_id)
        eq_(job['status'], u'complete')
        eq_(job['error'], None)

    @mock_actions(resource_url=u'http://example.com/umlaut_name_%C3%A4%C3%B6%C3%BC.csv')
    @responses.activate
    def test_resource_url_with_umlaut(self):
        # test that xloader can handle URLs with umlauts
        # e.g. http://www.web.statistik.zh.ch/ogd/data/KANTON_ZUERICH_gpfi_Jahresrechung_Zweckverbände.csv
        self.register_urls(
            filename=u'umlaut_name_äöü.csv',
            resource_url=u'http://example.com/umlaut_name_%C3%A4%C3%B6%C3%BC.csv'
        )
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

        with mock.patch('ckanext.xloader.jobs.set_datastore_active_flag') \
                as mocked_set_datastore_active_flag:
            # in tests we call jobs directly, rather than use rq, so mock
            # get_current_job()
            with mock.patch('ckanext.xloader.jobs.get_current_job',
                            return_value=mock.Mock(id=job_id)):
                result = jobs.xloader_data_into_datastore(data)
        assert result is None, jobs_db.get_job(job_id)['error']['message'].decode('utf-8')

        # Check it said it was successful
        eq_(responses.calls[-1].request.url, 'http://www.ckan.org/api/3/action/xloader_hook')
        job_dict = json.loads(responses.calls[-1].request.body)
        assert job_dict['status'] == u'complete', job_dict
        eq_(job_dict,
            {u'metadata': {u'ckan_url': u'http://www.ckan.org/',
                           u'resource_id': u'foo-bar-42'},
             u'status': u'complete'})

        logs = self.get_load_logs(job_id)
        logs.assert_no_errors()

        job = jobs_db.get_job(job_id)
        eq_(job['status'], u'complete')
        eq_(job['error'], None)

    @mock_actions(resource_url=SOURCE_URL)
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

        with mock.patch('ckanext.xloader.jobs.set_datastore_active_flag') \
                as mocked_set_datastore_active_flag:
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
        mocked_set_datastore_active_flag.assert_called_once()
        eq_(mocked_set_datastore_active_flag.call_args[1]['data_dict'],
            {'ckan_url': 'http://www.ckan.org/', 'resource_id': 'foo-bar-42'})

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
