import os
import json
import random
try:
    from collections import OrderedDict  # from python 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

from nose.tools import eq_
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

    def register_urls(self, filename='simple.csv', format='CSV',
                      content_type='application/csv'):
        """Mock some test URLs with responses.

        Mocks some URLs related to a data file and a CKAN resource that
        contains the data file, including the URL of the data file itself and
        the resource_show, resource_update and datastore_delete URLs.

        :returns: a 2-tuple containing the URL of the data file itself and the
            resource_show URL for the resource that contains the data file

        """
        responses.add_passthru(config['solr_url'])

        # A URL that just returns a static file (simple.csv by default).
        source_url = 'http://www.example.com/static/file'
        responses.add(responses.GET, source_url,
                      body=get_sample_file(filename),
                      content_type=content_type)
        # A URL that mocks CKAN's resource_show API.
        res_url = 'http://www.ckan.org/api/3/action/resource_show'
        responses.add(responses.POST, res_url,
                      body=json.dumps({
                           'success': True,
                           'result': {
                               'id': self.resource_id,
                               'name': 'short name',
                               'url': source_url,
                               'format': format
                           }
                      }),
                      content_type='application/json')

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

    @responses.activate
    def test_simple_csv(self):
        # Test not only the load and xloader_hook is called at the end
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
        eq_(result, None)

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
