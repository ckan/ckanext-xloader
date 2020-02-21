import datetime

# from nose.tools import eq_
# import mock

import ckan.plugins as p
from ckan.tests import helpers, factories


class TestNotify(object):

    @classmethod
    def setup_class(cls):
        if not p.plugin_loaded('datastore'):
            p.load('datastore')
        if not p.plugin_loaded('xloader'):
            p.load('xloader')

        helpers.reset_db()

    @classmethod
    def teardown_class(cls):

        p.unload('xloader')
        p.unload('datastore')

        helpers.reset_db()

    @helpers.mock_action('xloader_submit')
    def test_submit_on_resource_create(self, mock_xloader_submit):
        dataset = factories.Dataset()

        assert not mock_xloader_submit.called

        helpers.call_action('resource_create', {},
                            package_id=dataset['id'],
                            url='http://example.com/file.csv',
                            format='CSV')

        assert mock_xloader_submit.called

    @helpers.mock_action('xloader_submit')
    def test_submit_when_url_changes(self, mock_xloader_submit):
        dataset = factories.Dataset()

        resource = helpers.call_action('resource_create', {},
                                       package_id=dataset['id'],
                                       url='http://example.com/file.pdf',
                                       )

        assert not mock_xloader_submit.called  # because of the format being PDF

        helpers.call_action('resource_update', {},
                            id=resource['id'],
                            package_id=dataset['id'],
                            url='http://example.com/file.csv',
                            format='CSV'
                            )

        assert mock_xloader_submit.called

    def _pending_task(self, resource_id):
        return {
            'entity_id': resource_id,
            'entity_type': 'resource',
            'task_type': 'xloader',
            'last_updated': str(datetime.datetime.utcnow()),
            'state': 'pending',
            'key': 'xloader',
            'value': '{}',
            'error': '{}',
        }

    # @helpers.mock_action('xloader_submit')
    # def test_does_not_submit_while_ongoing_job(self, mock_xloader_submit):
    #     dataset = factories.Dataset()

    #     resource = helpers.call_action('resource_create', {},
    #                                    package_id=dataset['id'],
    #                                    url='http://example.com/file.CSV',
    #                                    format='CSV'
    #                                    )

    #     assert mock_xloader_submit.called
    #     eq_(len(mock_xloader_submit.mock_calls), 1)

    #     # Create a task with a state pending to mimic an ongoing job
    #     # on the xloader
    #     helpers.call_action('task_status_update', {},
    #                         **self._pending_task(resource['id']))

    #     # Update the resource
    #     helpers.call_action('resource_update', {},
    #                         id=resource['id'],
    #                         package_id=dataset['id'],
    #                         url='http://example.com/file.csv',
    #                         format='CSV',
    #                         description='Test',
    #                         )
    #     # Not called
    #     eq_(len(mock_xloader_submit.mock_calls), 1)

    # @helpers.mock_action('xloader_submit')
    # def test_resubmits_if_url_changes_in_the_meantime(
    #         self, mock_xloader_submit):
    #     dataset = factories.Dataset()

    #     resource = helpers.call_action('resource_create', {},
    #                                    package_id=dataset['id'],
    #                                    url='http://example.com/file.csv',
    #                                    format='CSV'
    #                                    )

    #     assert mock_xloader_submit.called
    #     eq_(len(mock_xloader_submit.mock_calls), 1)

    #     # Create a task with a state pending to mimic an ongoing job
    #     # on the xloader
    #     task = helpers.call_action('task_status_update', {},
    #                                **self._pending_task(resource['id']))

    #     # Update the resource, set a new URL
    #     helpers.call_action('resource_update', {},
    #                         id=resource['id'],
    #                         package_id=dataset['id'],
    #                         url='http://example.com/another.file.csv',
    #                         format='CSV',
    #                         )
    #     # Not called
    #     eq_(len(mock_xloader_submit.mock_calls), 1)

    #     # Call xloader_hook with state complete, to mock the xloader
    #     # finishing the job and telling CKAN
    #     data_dict = {
    #         'metadata': {
    #             'resource_id': resource['id'],
    #             'original_url': 'http://example.com/file.csv',
    #             'task_created': task['last_updated'],
    #         },
    #         'status': 'complete',
    #     }
    #     helpers.call_action('xloader_hook', {}, **data_dict)

    #     # xloader_submit was called again
    #     eq_(len(mock_xloader_submit.mock_calls), 2)

    # @helpers.mock_action('xloader_submit')
    # def test_resubmits_if_upload_changes_in_the_meantime(
    #         self, mock_xloader_submit):
    #     dataset = factories.Dataset()

    #     resource = helpers.call_action('resource_create', {},
    #                                    package_id=dataset['id'],
    #                                    url='http://example.com/file.csv',
    #                                    format='CSV'
    #                                    )

    #     assert mock_xloader_submit.called
    #     eq_(len(mock_xloader_submit.mock_calls), 1)

    #     # Create a task with a state pending to mimic an ongoing job
    #     # on the xloader
    #     task = helpers.call_action('task_status_update', {},
    #                                **self._pending_task(resource['id']))

    #     # Update the resource, set a new last_modified (changes on file upload)
    #     helpers.call_action(
    #         'resource_update', {},
    #         id=resource['id'],
    #         package_id=dataset['id'],
    #         url='http://example.com/file.csv',
    #         format='CSV',
    #         last_modified=datetime.datetime.utcnow().isoformat()
    #     )
    #     # Not called
    #     eq_(len(mock_xloader_submit.mock_calls), 1)

    #     # Call xloader_hook with state complete, to mock the xloader
    #     # finishing the job and telling CKAN
    #     data_dict = {
    #         'metadata': {
    #             'resource_id': resource['id'],
    #             'original_url': 'http://example.com/file.csv',
    #             'task_created': task['last_updated'],
    #         },
    #         'status': 'complete',
    #     }
    #     helpers.call_action('xloader_hook', {}, **data_dict)

    #     # xloader_submit was called again
    #     eq_(len(mock_xloader_submit.mock_calls), 2)

    # @helpers.mock_action('xloader_submit')
    # def test_does_not_resubmit_if_a_resource_field_changes_in_the_meantime(
    #         self, mock_xloader_submit):
    #     dataset = factories.Dataset()

    #     resource = helpers.call_action('resource_create', {},
    #                                    package_id=dataset['id'],
    #                                    url='http://example.com/file.csv',
    #                                    format='CSV'
    #                                    )

    #     assert mock_xloader_submit.called
    #     eq_(len(mock_xloader_submit.mock_calls), 1)

    #     # Create a task with a state pending to mimic an ongoing job
    #     # on the xloader
    #     task = helpers.call_action('task_status_update', {},
    #                                **self._pending_task(resource['id']))

    #     # Update the resource, set a new description
    #     helpers.call_action('resource_update', {},
    #                         id=resource['id'],
    #                         package_id=dataset['id'],
    #                         url='http://example.com/file.csv',
    #                         format='CSV',
    #                         description='Test',
    #                         )
    #     # Not called
    #     eq_(len(mock_xloader_submit.mock_calls), 1)

    #     # Call xloader_hook with state complete, to mock the xloader
    #     # finishing the job and telling CKAN
    #     data_dict = {
    #         'metadata': {
    #             'resource_id': resource['id'],
    #             'original_url': 'http://example.com/file.csv',
    #             'task_created': task['last_updated'],
    #         },
    #         'status': 'complete',
    #     }
    #     helpers.call_action('xloader_hook', {}, **data_dict)

    #     # Not called
    #     eq_(len(mock_xloader_submit.mock_calls), 1)

    # @helpers.mock_action('xloader_submit')
    # def test_does_not_resubmit_if_a_dataset_field_changes_in_the_meantime(
    #         self, mock_xloader_submit):
    #     dataset = factories.Dataset()

    #     resource = helpers.call_action('resource_create', {},
    #                                    package_id=dataset['id'],
    #                                    url='http://example.com/file.csv',
    #                                    format='CSV'
    #                                    )

    #     assert mock_xloader_submit.called
    #     eq_(len(mock_xloader_submit.mock_calls), 1)

    #     # Create a task with a state pending to mimic an ongoing job
    #     # on the xloader
    #     task = helpers.call_action('task_status_update', {},
    #                                **self._pending_task(resource['id']))

    #     # Update the parent dataset
    #     helpers.call_action('package_update', {},
    #                         id=dataset['id'],
    #                         notes='Test notes',
    #                         resources=[resource]
    #                         )
    #     # Not called
    #     eq_(len(mock_xloader_submit.mock_calls), 1)

    #     # Call xloader_hook with state complete, to mock the xloader
    #     # finishing the job and telling CKAN
    #     data_dict = {
    #         'metadata': {
    #             'resource_id': resource['id'],
    #             'original_url': 'http://example.com/file.csv',
    #             'task_created': task['last_updated'],
    #         },
    #         'status': 'complete',
    #     }
    #     helpers.call_action('xloader_hook', {}, **data_dict)

    #     # Not called
    #     eq_(len(mock_xloader_submit.mock_calls), 1)
