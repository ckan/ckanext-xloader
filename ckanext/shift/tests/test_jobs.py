import os
import json
try:
    from collections import OrderedDict  # from python 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

from nose.tools import eq_
import mock
import httpretty
from sqlalchemy import MetaData, Table
from sqlalchemy.sql import select
from pylons import config

import ckan.plugins as p
from ckan.tests import helpers, factories
import ckanext.datastore.backend.postgres as datastore_db
from ckanext.shift import jobs
from ckanext.shift import db as jobs_db

class TestShiftDataIntoDatastore(object):

    @classmethod
    def setup_class(cls):
        if not p.plugin_loaded('shift'):
            p.load('shift')
        helpers.reset_db()

        cls.host = 'www.ckan.org'
        cls.api_key = 'my-fake-key'
        cls.resource_id = 'foo-bar-42'
        jobs_db.init(config, echo=False)
        # drop test table
        engine, conn = cls.get_datastore_engine_and_connection()
        conn.execute('DROP TABLE IF EXISTS "{}"'.format(cls.resource_id))

    @classmethod
    def teardown_class(cls):
        p.unload('shift')
        helpers.reset_db()

    def register_urls(self, filename='simple.csv', format='CSV',
                      content_type='application/csv'):
        """Mock some test URLs with httpretty.

        Mocks some URLs related to a data file and a CKAN resource that
        contains the data file, including the URL of the data file itself and
        the resource_show, resource_update and datastore_delete URLs.

        :returns: a 2-tuple containing the URL of the data file itself and the
            resource_show URL for the resource that contains the data file

        """
        # A URL that just returns a static file (simple.csv by default).
        source_url = 'http://www.source.org/static/file'
        httpretty.register_uri(httpretty.GET, source_url,
                               body=get_sample_file(filename),
                               content_type=content_type)
        # A URL that mocks CKAN's resource_show API.
        res_url = 'http://www.ckan.org/api/3/action/resource_show'
        httpretty.register_uri(httpretty.POST, res_url,
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
        # give after successfully upddating a resource.
        resource_update_url = (
            'http://www.ckan.org/api/3/action/resource_update')
        httpretty.register_uri(httpretty.POST, resource_update_url,
                               body=json.dumps({'success': True}),
                               content_type='application/json')

        # A URL that mock's the response that CKAN's datastore plugin's
        # datastore_delete API would give after successfully deleting a
        # resource from the datastore.
        datastore_del_url = 'http://www.ckan.org/api/3/action/datastore_delete'
        httpretty.register_uri(httpretty.POST, datastore_del_url,
                               body=json.dumps({'success': True}),
                               content_type='application/json')

    @classmethod
    def get_datastore_engine_and_connection(cls):
        if '_datastore' not in dir(cls):
            engine = datastore_db.get_write_engine()
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

    @httpretty.activate
    def test_simple_csv(self):
        # Test successfully fetching and parsing a simple CSV file.
        #
        # When given dry_run=True and a resource with a simple CSV file the
        # push_to_datastore job should fetch and parse the file and return the
        # right headers and data rows from the file.
        self.register_urls()
        data = {
            'api_key': self.api_key,
            'job_type': 'push_to_datastore',
            'metadata': {
                'ckan_url': 'http://%s/' % self.host,
                'resource_id': self.resource_id
            }
        }

        jobs.shift_data_into_datastore('fake_job_id', data)
        data = self.get_datastore_table()
        eq_(data['headers'],
            ['_id', '_full_text', 'date', 'temperature', 'place'])
        eq_(data['header_dict']['date'], 'VARCHAR')
        # 'TIMESTAMP WITHOUT TIME ZONE')
        eq_(data['header_dict']['temperature'], 'VARCHAR')  # 'NUMERIC')
        eq_(data['header_dict']['place'], 'VARCHAR')  # 'TEXT')
        eq_(data['num_rows'], 6)
        eq_(data['rows'][0][2:],
            (u'2011-01-01', u'1', u'Galway'))
        # (datetime.datetime(2011, 1, 1), 1, 'Galway'))

    def test_submit(self):
        # checks that shift_submit enqueues the resource (to be shifted)
        user = factories.User()
        # normally creating a resource causes shift_submit to be called,
        # but we avoid that by setting an invalid format
        res = factories.Resource(user=user, format='aaa')
        # mock the enqueue
        with mock.patch('ckanext.shift.queue._queue') as queue_mock:
            # r_mock().json = mock.Mock(
            #     side_effect=lambda: dict.fromkeys(
            #         ['job_id', 'job_key']))
            helpers.call_action(
                'shift_submit', context=dict(user=user['name']),
                resource_id=res['id'])
            eq_(1, queue_mock.enqueue.call_count)

    def test_duplicated_submits(self):
        def submit(res, user):
            return helpers.call_action(
                'shift_submit', context=dict(user=user['name']),
                resource_id=res['id'])

        user = factories.User()
        res = factories.Resource(user=user, format='csv')
        with mock.patch('ckanext.shift.queue._queue') as queue_mock:
            queue_mock.reset_mock()
            submit(res, user)
            # a second submit will not enqueue it again, because of the
            # existing task for this resource - shown by task_status_show
            submit(res, user)
            eq_(1, queue_mock.enqueue.call_count)


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
