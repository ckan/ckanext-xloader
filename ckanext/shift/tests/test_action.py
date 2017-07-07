from nose.tools import eq_
import mock

import ckan.plugins as p
from ckan.tests import helpers, factories


class TestAction(object):

    @classmethod
    def setup_class(cls):
        if not p.plugin_loaded('datastore'):
            p.load('datastore')
        if not p.plugin_loaded('shift'):
            p.load('shift')

        helpers.reset_db()

    @classmethod
    def teardown_class(cls):

        p.unload('shift')
        p.unload('datastore')

        helpers.reset_db()

    def test_submit(self):
        # checks that shift_submit enqueues the resource (to be shifted)
        user = factories.User()
        # normally creating a resource causes shift_submit to be called,
        # but we avoid that by setting an invalid format
        res = factories.Resource(user=user, format='aaa')
        # mock the enqueue
        with mock.patch('ckanext.shift.job_queue._queue') as queue_mock:
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
        with mock.patch('ckanext.shift.job_queue._queue') as queue_mock:
            queue_mock.reset_mock()
            submit(res, user)
            # a second submit will not enqueue it again, because of the
            # existing task for this resource - shown by task_status_show
            submit(res, user)
            eq_(1, queue_mock.enqueue.call_count)
