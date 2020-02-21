import mock
import pytest

import ckan.plugins as p
from ckantoolkit.tests import helpers, factories


@pytest.mark.usefixtures(u"clean_db")
class TestAction():

    @classmethod
    def setup_class(cls):
        if not p.plugin_loaded('datastore'):
            p.load('datastore')
        if not p.plugin_loaded('xloader'):
            p.load('xloader')

    @classmethod
    def teardown_class(cls):

        p.unload('xloader')
        p.unload('datastore')

    def test_submit(self):
        # checks that xloader_submit enqueues the resource (to be xloadered)
        user = factories.User()
        # normally creating a resource causes xloader_submit to be called,
        # but we avoid that by setting an invalid format
        res = factories.Resource(user=user, format='aaa')
        # mock the enqueue
        with mock.patch('ckanext.xloader.action.enqueue_job',
                        return_value=mock.MagicMock(id=123)) as enqueue_mock:
            helpers.call_action(
                'xloader_submit', context=dict(user=user['name']),
                resource_id=res['id'])
            assert enqueue_mock.call_count == 1

    def test_duplicated_submits(self):
        def submit(res, user):
            return helpers.call_action(
                'xloader_submit', context=dict(user=user['name']),
                resource_id=res['id'])

        user = factories.User()

        with mock.patch('ckanext.xloader.action.enqueue_job',
                        return_value=mock.MagicMock(id=123)) as enqueue_mock:
            enqueue_mock.reset_mock()
            # creating the resource causes it to be queued
            res = factories.Resource(user=user, format='csv')
            assert enqueue_mock.call_count == 1

            # a second request to queue it will be stopped, because of the
            # existing task for this resource - shown by task_status_show
            submit(res, user)
            assert enqueue_mock.call_count == 1

    def test_xloader_hook(self):
        # Check the task_status is stored correctly after a xloader job.
        user = factories.User()
        res = factories.Resource(user=user, format='csv')
        task_status = helpers.call_action(
            'task_status_update', context={},
            entity_id=res['id'],
            entity_type='resource',
            task_type='xloader',
            key='xloader',
            value='{}',
            error='{}',
            state='pending',
        )

        helpers.call_action(
            'xloader_hook', context=dict(user=user['name']),
            metadata={'resource_id': res['id']},
            status='complete',
            )

        task_status = helpers.call_action(
            'task_status_show', context={},
            entity_id=res['id'],
            task_type='xloader',
            key='xloader',
        )
        assert task_status['state'] == 'complete'
